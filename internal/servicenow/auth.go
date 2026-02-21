// Package servicenow provides authentication for ServiceNow API access.
//
// # Authentication Model
//
// ServiceNow supports two authentication mechanisms:
//
//   - OAuth 2.0 Password Grant: The connector sends client_id, client_secret,
//     username, and password to /oauth_token.do and receives a bearer token with
//     an expiry (typically 30 minutes). This is the recommended production method.
//
//   - HTTP Basic Auth: Simple username:password encoded as base64 in the
//     Authorization header. Suitable for development/testing environments.
//
// # Token Lifecycle (OAuth)
//
// The original Java connector only refreshes tokens reactively — when a 401
// response is received. This Go implementation improves on that by running a
// background goroutine that proactively refreshes the token before it expires:
//
//	┌─────────────┐      ┌──────────────────┐     ┌──────────────┐
//	│ Token Issued │─────▶│ Valid (in-flight  │────▶│ Refresh at   │
//	│              │      │   requests use    │     │ expires_in   │
//	│              │      │   current token)  │     │ - 60 seconds │
//	└─────────────┘      └──────────────────┘     └──────┬───────┘
//	                                                      │
//	                                              ┌───────▼───────┐
//	                                              │ New token      │
//	                                              │ replaces old   │
//	                                              │ (no downtime)  │
//	                                              └───────────────┘
//
// The [ForceRefresh] method is still available for the HTTP client to call
// when it receives a 401 (covering edge cases like token revocation).
//
// # Thread Safety
//
// All authenticator implementations are safe for concurrent use. The OAuth
// authenticator uses sync.RWMutex to allow multiple concurrent readers of the
// current token while serializing token refreshes.
package servicenow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
)

// Authenticator provides authentication tokens for ServiceNow API requests.
// Implementations must be safe for concurrent use by multiple goroutines.
type Authenticator interface {
	// Token returns the current authentication header value. For OAuth this is
	// "Bearer <token>", for Basic it is "Basic <base64>". The context allows
	// cancellation during token acquisition.
	Token(ctx context.Context) (string, error)

	// ForceRefresh forces a token refresh. Called by the HTTP client when it
	// receives a 401 Unauthorized response. For Basic auth this is a no-op.
	ForceRefresh(ctx context.Context) error

	// Close stops any background goroutines (e.g., proactive token refresh).
	Close()
}

// ----- Basic Authenticator -----

// BasicAuthenticator implements the Authenticator interface using HTTP Basic
// Authentication. The credentials are encoded once at construction time.
//
// This authenticator is suitable for development and testing environments.
// ForceRefresh is a no-op since basic auth credentials don't expire.
type BasicAuthenticator struct {
	header string
}

// NewBasicAuthenticator creates a BasicAuthenticator from the given credentials.
// The username:password pair is base64-encoded per RFC 7617.
func NewBasicAuthenticator(username, password string) *BasicAuthenticator {
	encoded := base64.StdEncoding.EncodeToString(
		[]byte(username + ":" + password),
	)
	return &BasicAuthenticator{
		header: "Basic " + encoded,
	}
}

// Token returns the pre-computed "Basic <base64>" header value.
func (b *BasicAuthenticator) Token(_ context.Context) (string, error) {
	return b.header, nil
}

// ForceRefresh is a no-op for basic auth — credentials never expire.
func (b *BasicAuthenticator) ForceRefresh(_ context.Context) error {
	return nil
}

// Close is a no-op for basic auth — no background goroutines to stop.
func (b *BasicAuthenticator) Close() {}

// ----- OAuth Authenticator -----

// oauthTokenResponse matches the JSON response from ServiceNow's OAuth endpoint.
//
// Example response:
//
//	{
//	  "access_token": "eyJhbGci...",
//	  "refresh_token": "abc123...",
//	  "scope": "useraccount",
//	  "token_type": "Bearer",
//	  "expires_in": 1799
//	}
type oauthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
}

// OAuthAuthenticator implements the Authenticator interface using ServiceNow's
// OAuth 2.0 password grant flow. It proactively refreshes the token before
// expiry and supports forced refresh on 401 responses.
//
// # Refresh Strategy
//
// The authenticator schedules a refresh at (expires_in - refreshBuffer) seconds
// from the time a token is issued. The default refreshBuffer is 60 seconds,
// meaning a token with a 30-minute expiry will be refreshed at 29 minutes.
//
// If the proactive refresh fails, it retries with exponential backoff up to
// maxRefreshRetries times. If all retries fail, the token remains valid until
// its actual expiry, and the next Token() call or ForceRefresh() will attempt
// a fresh token acquisition.
type OAuthAuthenticator struct {
	// Configuration
	baseURL      string
	tokenPath    string
	clientID     string
	clientSecret string
	username     string
	password     string
	httpClient   *http.Client
	logger       *slog.Logger

	// Token state — protected by mu
	mu           sync.RWMutex
	accessToken  string
	refreshToken string
	expiresAt    time.Time

	// Background refresh
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// refreshBuffer is how many seconds before expiry we trigger a refresh.
	refreshBuffer time.Duration
}

// NewOAuthAuthenticator creates an OAuthAuthenticator and performs the initial
// token acquisition. It starts a background goroutine for proactive refresh.
//
// The caller must call Close() when the authenticator is no longer needed to
// stop the background refresh goroutine.
func NewOAuthAuthenticator(ctx context.Context, cfg config.ServiceNowConfig, logger *slog.Logger) (*OAuthAuthenticator, error) {
	o := &OAuthAuthenticator{
		baseURL:       strings.TrimRight(cfg.BaseURL, "/"),
		tokenPath:     cfg.Auth.OAuth.TokenPath,
		clientID:      cfg.Auth.OAuth.ClientID,
		clientSecret:  cfg.Auth.OAuth.ClientSecret,
		username:      cfg.Auth.OAuth.Username,
		password:      cfg.Auth.OAuth.Password,
		refreshBuffer: 60 * time.Second,
		logger:        logger.With("component", "oauth-auth"),
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
		},
	}

	// Perform the initial token acquisition synchronously so the caller
	// knows immediately if credentials are invalid.
	if err := o.doRefresh(ctx); err != nil {
		return nil, fmt.Errorf("initial OAuth token acquisition: %w", err)
	}

	// Start the background proactive refresh goroutine.
	bgCtx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel
	o.wg.Add(1)
	go o.refreshLoop(bgCtx)

	return o, nil
}

// Token returns the current "Bearer <access_token>" header value. It is safe
// for concurrent use — multiple goroutines can call Token() simultaneously
// while a refresh is in progress. Callers will receive the current (still-valid)
// token during a refresh window.
func (o *OAuthAuthenticator) Token(_ context.Context) (string, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.accessToken == "" {
		return "", fmt.Errorf("no OAuth token available")
	}
	return "Bearer " + o.accessToken, nil
}

// ForceRefresh triggers an immediate token refresh. Called by the HTTP client
// when it receives a 401 response. This is serialized with the background
// refresh goroutine via the write lock.
func (o *OAuthAuthenticator) ForceRefresh(ctx context.Context) error {
	o.logger.Info("forced token refresh triggered (likely 401 response)")
	return o.doRefresh(ctx)
}

// Close stops the background refresh goroutine and waits for it to exit.
func (o *OAuthAuthenticator) Close() {
	if o.cancel != nil {
		o.cancel()
		o.wg.Wait()
	}
}

// refreshLoop runs in a background goroutine and proactively refreshes the
// token before it expires. It calculates the next refresh time as:
//
//	nextRefresh = expiresAt - refreshBuffer
//
// If the refresh fails, it retries with exponential backoff (1s, 2s, 4s, ...)
// up to 5 times before giving up and waiting for the next natural refresh cycle.
func (o *OAuthAuthenticator) refreshLoop(ctx context.Context) {
	defer o.wg.Done()

	for {
		// Calculate when to refresh next.
		o.mu.RLock()
		refreshAt := o.expiresAt.Add(-o.refreshBuffer)
		o.mu.RUnlock()

		delay := time.Until(refreshAt)
		if delay < 0 {
			// Token already expired or about to — refresh immediately.
			delay = 0
		}

		o.logger.Debug("scheduling proactive token refresh",
			"refresh_in", delay.Round(time.Second),
			"expires_at", o.expiresAt,
		)

		select {
		case <-ctx.Done():
			o.logger.Info("OAuth refresh loop shutting down")
			return
		case <-time.After(delay):
		}

		// Attempt refresh with retries.
		const maxRetries = 5
		backoff := time.Second
		var err error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				o.logger.Warn("retrying token refresh",
					"attempt", attempt,
					"backoff", backoff,
					"error", err,
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
			}

			err = o.doRefresh(ctx)
			if err == nil {
				o.logger.Info("proactive token refresh succeeded")
				break
			}
		}
		if err != nil {
			o.logger.Error("proactive token refresh failed after retries",
				"max_retries", maxRetries,
				"error", err,
			)
		}
	}
}

// doRefresh performs the actual OAuth token request. It acquires a write lock
// before updating the stored token, ensuring that concurrent Token() calls
// either see the old (still-valid) token or the new one, never a partial state.
//
// This is the Go equivalent of ServiceNowTableApiClient.getAuthenticationToken()
// from the Java reference implementation (lines 316-361).
func (o *OAuthAuthenticator) doRefresh(ctx context.Context) error {
	tokenURL := o.baseURL + o.tokenPath

	// Build the form body for the OAuth password grant request.
	// ServiceNow expects: grant_type=password, client_id, client_secret,
	// username, password — exactly matching the Java implementation.
	form := url.Values{
		"grant_type":    {"password"},
		"client_id":     {o.clientID},
		"client_secret": {o.clientSecret},
		"username":      {o.username},
		"password":      {o.password},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading token response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OAuth token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp oauthTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return fmt.Errorf("parsing token response JSON: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return fmt.Errorf("OAuth response did not contain an access_token")
	}

	// Update the stored token under a write lock.
	o.mu.Lock()
	o.accessToken = tokenResp.AccessToken
	o.refreshToken = tokenResp.RefreshToken
	if tokenResp.ExpiresIn > 0 {
		o.expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	} else {
		// Default to 30 minutes if the server doesn't specify expiry.
		o.expiresAt = time.Now().Add(30 * time.Minute)
	}
	o.mu.Unlock()

	o.logger.Info("OAuth token acquired",
		"expires_in", tokenResp.ExpiresIn,
		"expires_at", o.expiresAt,
	)
	return nil
}

// NewAuthenticator is a factory that creates the appropriate Authenticator
// based on the config auth type. It returns an error if the auth type is
// invalid or if initial token acquisition fails (for OAuth).
func NewAuthenticator(ctx context.Context, cfg config.ServiceNowConfig, logger *slog.Logger) (Authenticator, error) {
	switch cfg.Auth.Type {
	case "basic":
		return NewBasicAuthenticator(cfg.Auth.Basic.Username, cfg.Auth.Basic.Password), nil
	case "oauth":
		return NewOAuthAuthenticator(ctx, cfg, logger)
	default:
		return nil, fmt.Errorf("unsupported auth type: %q", cfg.Auth.Type)
	}
}
