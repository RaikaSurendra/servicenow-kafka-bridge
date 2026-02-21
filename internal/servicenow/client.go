// Package servicenow provides the HTTP client for the ServiceNow Table API.
//
// # Client Architecture
//
// The Client wraps Go's standard net/http.Client and provides:
//
//   - Authentication: Delegates to the [Authenticator] interface for token management.
//   - Retry with backoff: Uses exponential backoff with jitter for transient errors.
//   - 401 recovery: Calls [Authenticator.ForceRefresh] and retries immediately.
//   - 429 rate limiting: Respects the Retry-After header from ServiceNow.
//   - Optional rate limiter: Proactive client-side rate limiting via golang.org/x/time/rate.
//
// # Retry Strategy
//
// The retry loop differentiates errors by HTTP status code:
//
//	┌─────────────────────┬─────────────────────────────────────────────────┐
//	│ Status / Error      │ Action                                          │
//	├─────────────────────┼─────────────────────────────────────────────────┤
//	│ 2xx                 │ Success — return parsed response                │
//	│ 401 Unauthorized    │ ForceRefresh auth, retry immediately (no backoff)│
//	│ 429 Too Many Reqs   │ Sleep for Retry-After header value, then retry  │
//	│ 5xx / network error │ Exponential backoff with jitter (100ms → 5min)  │
//	│ 4xx (other)         │ Fatal — return error immediately, do not retry  │
//	└─────────────────────┴─────────────────────────────────────────────────┘
//
// This is a significant improvement over the Java reference implementation
// (ServiceNowTableApiClient.java) which uses a fixed backoff for all error
// types and does not handle 429 responses.
//
// # URL Construction
//
// APIs are called at: {BaseURL}{TableAPIPath}/{tableName}?sysparm_query=...
//
// Query parameters follow the ServiceNow Table API convention:
//   - sysparm_query: Encoded query string from [QueryBuilder]
//   - sysparm_limit: Max records to return
//   - sysparm_offset: Pagination offset
//   - sysparm_exclude_reference_link: true (strip reference URLs)
//   - sysparm_fields: Comma-separated field list
//
// # Thread Safety
//
// The Client is safe for concurrent use. Multiple goroutines (pollers) can
// call GetRecords simultaneously. The underlying http.Client handles connection
// pooling, and the Authenticator ensures thread-safe token access.
package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/observability"
	"golang.org/x/time/rate"
)

// Client provides methods to interact with the ServiceNow Table API.
// All methods are safe for concurrent use.
type Client interface {
	// GetRecords queries a ServiceNow table and returns matching records.
	// The query parameter must not be nil (use NewQueryBuilder() for an empty query).
	// Fields can be nil to return all fields.
	GetRecords(ctx context.Context, table string, query *QueryBuilder, offset int, limit int, fields []string) ([]Record, error)

	// InsertRecord creates a new record in the specified ServiceNow table.
	// Returns the created record (which includes the assigned sys_id).
	InsertRecord(ctx context.Context, table string, record Record) (*Record, error)

	// UpdateRecord updates an existing record identified by sysID in the
	// specified table. Uses HTTP PATCH for partial updates.
	// Returns the updated record.
	UpdateRecord(ctx context.Context, table string, sysID string, record Record) (*Record, error)

	// Close releases any resources held by the client.
	Close()
}

// httpClient is the concrete implementation of the Client interface.
type httpClient struct {
	baseURL      string
	tableAPIPath string
	auth         Authenticator
	http         *http.Client
	limiter      *rate.Limiter
	logger       *slog.Logger

	// Retry configuration
	maxRetries         int
	initialBackoff     time.Duration
	maxBackoff         time.Duration
	retryBackoffFactor float64
}

// ClientOption is a functional option for configuring the HTTP client.
type ClientOption func(*httpClient)

// WithRateLimiter sets a client-side rate limiter.
func WithRateLimiter(rps float64) ClientOption {
	return func(c *httpClient) {
		if rps > 0 {
			c.limiter = rate.NewLimiter(rate.Limit(rps), int(math.Max(1, rps)))
		}
	}
}

// NewClient creates a new ServiceNow HTTP client.
//
// The client uses the provided Authenticator for all requests. The caller is
// responsible for calling Close() on both the client and the authenticator
// when they are no longer needed.
func NewClient(cfg config.ServiceNowConfig, auth Authenticator, logger *slog.Logger, opts ...ClientOption) Client {
	c := &httpClient{
		baseURL:      strings.TrimRight(cfg.BaseURL, "/"),
		tableAPIPath: cfg.TableAPIPath,
		auth:         auth,
		logger:       logger.With("component", "sn-client"),
		http: &http.Client{
			Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},

		// Retry defaults
		maxRetries:         cfg.MaxRetries,
		initialBackoff:     100 * time.Millisecond,
		maxBackoff:         5 * time.Minute,
		retryBackoffFactor: 2.0,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Close releases resources. Currently a no-op but included for interface
// compliance and future extensibility.
func (c *httpClient) Close() {}

// GetRecords queries a ServiceNow table using the Table API.
//
// This is the Go equivalent of ServiceNowTableApiClient.getRecords() from
// the Java reference. URL construction follows the same pattern:
//
//	GET {baseURL}/api/now/table/{table}?sysparm_query=...&sysparm_limit=...
//
// The response JSON has the structure: {"result": [{...}, {...}, ...]}
func (c *httpClient) GetRecords(ctx context.Context, table string, query *QueryBuilder, offset int, limit int, fields []string) ([]Record, error) {
	if query == nil {
		return nil, fmt.Errorf("query must not be nil; use NewQueryBuilder() for an empty query")
	}

	// Build the request URL with query parameters.
	reqURL, err := c.buildTableURL(table, query.Build(), offset, limit, fields)
	if err != nil {
		return nil, err
	}

	c.logger.Debug("fetching records",
		"table", table,
		"url", reqURL,
		"query", query.Build(),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	body, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}

	var resp TableResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing response JSON: %w (body: %.200s)", err, string(body))
	}

	return resp.Result, nil
}

// InsertRecord creates a new record via POST to the Table API.
//
//	POST {baseURL}/api/now/table/{table}
//	Body: JSON record
//
// Returns the created record from the response, which includes the sys_id
// assigned by ServiceNow.
func (c *httpClient) InsertRecord(ctx context.Context, table string, record Record) (*Record, error) {
	reqURL := fmt.Sprintf("%s%s/%s", c.baseURL, c.tableAPIPath, strings.TrimLeft(table, "/"))

	jsonBody, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshaling record: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, fmt.Errorf("creating POST request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	body, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}

	// Parse the single-record response: {"result": {...}}
	var resp struct {
		Result Record `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing insert response: %w", err)
	}
	return &resp.Result, nil
}

// UpdateRecord updates an existing record via PATCH to the Table API.
//
//	PATCH {baseURL}/api/now/table/{table}/{sys_id}
//	Body: JSON record (partial update)
func (c *httpClient) UpdateRecord(ctx context.Context, table string, sysID string, record Record) (*Record, error) {
	reqURL := fmt.Sprintf("%s%s/%s/%s", c.baseURL, c.tableAPIPath, strings.TrimLeft(table, "/"), sysID)

	jsonBody, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshaling record: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, reqURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, fmt.Errorf("creating PATCH request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	body, err := c.doWithRetry(ctx, req)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result Record `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing update response: %w", err)
	}
	return &resp.Result, nil
}

// buildTableURL constructs the full request URL for a Table API GET query.
// All values are properly URL-encoded using net/url.
func (c *httpClient) buildTableURL(table, query string, offset, limit int, fields []string) (string, error) {
	u, err := url.Parse(c.baseURL + c.tableAPIPath + "/" + strings.TrimLeft(table, "/"))
	if err != nil {
		return "", fmt.Errorf("building URL: %w", err)
	}

	params := url.Values{}
	params.Set("sysparm_exclude_reference_link", "true")
	if offset >= 0 {
		params.Set("sysparm_offset", strconv.Itoa(offset))
	}
	if limit > 0 {
		params.Set("sysparm_limit", strconv.Itoa(limit))
	}
	if query != "" {
		params.Set("sysparm_query", query)
	}
	if len(fields) > 0 {
		params.Set("sysparm_fields", strings.Join(fields, ","))
	}

	u.RawQuery = params.Encode()
	return u.String(), nil
}

// doWithRetry executes an HTTP request with the configured retry strategy.
//
// This is the Go equivalent of ServiceNowTableApiClient.sendWithRetry() from
// the Java reference (lines 158-225), with significant improvements:
//
//   - Exponential backoff with jitter instead of fixed delay
//   - Distinct handling for 401, 429, 4xx, and 5xx responses
//   - Context-aware cancellation
//   - Optional rate limiting before each request
//
// The retry loop works as follows:
//
//  1. Wait for rate limiter (if configured)
//  2. Attach current auth token to the request
//  3. Send the request
//  4. On success: return the response body
//  5. On 401: call ForceRefresh, retry immediately (no backoff)
//  6. On 429: sleep for Retry-After seconds, retry
//  7. On 5xx or network error: exponential backoff with jitter
//  8. On 4xx (other): return error immediately (non-retryable)
func (c *httpClient) doWithRetry(ctx context.Context, req *http.Request) ([]byte, error) {
	method := req.Method
	endpoint := req.URL.Path
	backoff := c.initialBackoff
	maxAttempts := c.maxRetries
	if maxAttempts < 0 {
		maxAttempts = math.MaxInt32 // unlimited
	}

	var lastErr error
	for attempt := 0; attempt <= maxAttempts; attempt++ {
		// Check context before each attempt.
		if err := ctx.Err(); err != nil {
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "context_canceled").Inc()
			return nil, fmt.Errorf("request cancelled: %w", err)
		}

		// Apply rate limiting if configured.
		if c.limiter != nil {
			if err := c.limiter.Wait(ctx); err != nil {
				observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "rate_limited").Inc()
				return nil, fmt.Errorf("rate limiter wait: %w", err)
			}
		}

		// Get the current auth token and set it on the request.
		token, err := c.auth.Token(ctx)
		if err != nil {
			lastErr = fmt.Errorf("getting auth token: %w", err)
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "auth").Inc()
			continue
		}
		// Clone request to avoid mutating the original on retry.
		reqClone := req.Clone(ctx)
		reqClone.Header.Set("Authorization", token)

		// If the request has a body, we need to re-create it for retries.
		// For GET requests this is nil, for POST/PATCH we need the original body.
		requestStart := time.Now()
		resp, err := c.http.Do(reqClone)
		observability.Metrics.SNAPIRequestsTotal.WithLabelValues(method, endpoint).Inc()
		observability.Metrics.SNAPILatency.WithLabelValues(method, endpoint).Observe(time.Since(requestStart).Seconds())
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %w", err)
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "network").Inc()
			c.logger.Warn("request failed, will retry",
				"attempt", attempt+1,
				"error", err,
				"backoff", backoff,
			)
			c.sleepWithJitter(ctx, backoff)
			backoff = c.nextBackoff(backoff)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("reading response body: %w", readErr)
			c.sleepWithJitter(ctx, backoff)
			backoff = c.nextBackoff(backoff)
			continue
		}

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			// Success!
			return body, nil

		case resp.StatusCode == 401:
			// Unauthorized — force a token refresh and retry immediately.
			c.logger.Info("received 401, forcing token refresh",
				"attempt", attempt+1,
			)
			if refreshErr := c.auth.ForceRefresh(ctx); refreshErr != nil {
				c.logger.Error("token refresh failed", "error", refreshErr)
			}
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "401").Inc()
			lastErr = fmt.Errorf("received 401 Unauthorized: %s", truncateBody(body))
			// Retry immediately — do NOT apply backoff.
			continue

		case resp.StatusCode == 429:
			// Rate limited — respect Retry-After header.
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			c.logger.Warn("received 429, respecting Retry-After",
				"retry_after", retryAfter,
				"attempt", attempt+1,
			)
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, "429").Inc()
			lastErr = fmt.Errorf("received 429 Too Many Requests")
			c.sleepWithJitter(ctx, retryAfter)
			continue

		case resp.StatusCode >= 500:
			// Server error — retry with backoff.
			lastErr = fmt.Errorf("received %d: %s", resp.StatusCode, truncateBody(body))
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, fmt.Sprintf("%d", resp.StatusCode)).Inc()
			c.logger.Warn("server error, will retry",
				"status", resp.StatusCode,
				"attempt", attempt+1,
				"backoff", backoff,
			)
			c.sleepWithJitter(ctx, backoff)
			backoff = c.nextBackoff(backoff)
			continue

		default:
			// 4xx (non-401, non-429) — fatal, do not retry.
			observability.Metrics.SNAPIErrorsTotal.WithLabelValues(method, fmt.Sprintf("%d", resp.StatusCode)).Inc()
			return nil, fmt.Errorf("non-retryable error %d: %s", resp.StatusCode, truncateBody(body))
		}
	}

	return nil, fmt.Errorf("exhausted %d retries: %w", maxAttempts, lastErr)
}

// nextBackoff calculates the next backoff duration using exponential growth
// capped at maxBackoff.
func (c *httpClient) nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * c.retryBackoffFactor)
	if next > c.maxBackoff {
		return c.maxBackoff
	}
	return next
}

// sleepWithJitter sleeps for a random duration between 50% and 100% of the
// given base duration. This prevents thundering herd when multiple pollers
// retry simultaneously. Returns early if the context is cancelled.
func (c *httpClient) sleepWithJitter(ctx context.Context, base time.Duration) {
	// Jitter: sleep for [0.5*base, base]
	jitter := time.Duration(float64(base) * (0.5 + rand.Float64()*0.5))
	select {
	case <-ctx.Done():
	case <-time.After(jitter):
	}
}

// parseRetryAfter parses the Retry-After header value as seconds.
// Returns a default of 30 seconds if the header is empty or unparseable.
func parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 30 * time.Second
	}
	seconds, err := strconv.Atoi(header)
	if err != nil {
		return 30 * time.Second
	}
	return time.Duration(seconds) * time.Second
}

// truncateBody returns the first 500 bytes of a response body for logging.
func truncateBody(body []byte) string {
	if len(body) > 500 {
		return string(body[:500]) + "..."
	}
	return string(body)
}
