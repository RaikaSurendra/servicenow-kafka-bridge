package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/your-org/servicenow-kafka-bridge/internal/config"
)

// mockAuth is a test authenticator that returns a fixed token.
type mockAuth struct {
	token        string
	refreshCount int32
}

func (m *mockAuth) Token(_ context.Context) (string, error) {
	return "Bearer " + m.token, nil
}
func (m *mockAuth) ForceRefresh(_ context.Context) error {
	atomic.AddInt32(&m.refreshCount, 1)
	return nil
}
func (m *mockAuth) Close() {}

func testCfg(baseURL string) config.ServiceNowConfig {
	return config.ServiceNowConfig{
		BaseURL:        baseURL,
		TableAPIPath:   "/api/now/table",
		TimeoutSeconds: 10,
		MaxRetries:     3,
	}
}

func TestGetRecords_Success(t *testing.T) {
	records := []Record{
		{"sys_id": "001", "short_description": "Test incident 1"},
		{"sys_id": "002", "short_description": "Test incident 2"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/api/now/table/incident") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		// Verify query parameters
		q := r.URL.Query()
		if q.Get("sysparm_limit") != "20" {
			t.Errorf("expected limit 20, got %s", q.Get("sysparm_limit"))
		}
		if q.Get("sysparm_exclude_reference_link") != "true" {
			t.Errorf("expected exclude_reference_link=true, got %s", q.Get("sysparm_exclude_reference_link"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: records})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	query := NewQueryBuilder().WhereIsNotEmpty("sys_id")
	result, err := client.GetRecords(context.Background(), "incident", query, 0, 20, nil)
	if err != nil {
		t.Fatalf("GetRecords failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 records, got %d", len(result))
	}
	if result[0]["sys_id"] != "001" {
		t.Errorf("first record sys_id = %v", result[0]["sys_id"])
	}
}

func TestGetRecords_WithFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields := r.URL.Query().Get("sysparm_fields")
		if fields != "sys_id,short_description,sys_updated_on" {
			t.Errorf("unexpected fields: %s", fields)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: []Record{}})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	query := NewQueryBuilder()
	_, err := client.GetRecords(context.Background(), "incident", query, 0, 10,
		[]string{"sys_id", "short_description", "sys_updated_on"})
	if err != nil {
		t.Fatalf("GetRecords failed: %v", err)
	}
}

func TestGetRecords_NilQuery(t *testing.T) {
	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg("http://example.com"), auth, logger)

	_, err := client.GetRecords(context.Background(), "incident", nil, 0, 10, nil)
	if err == nil {
		t.Fatal("expected error for nil query")
	}
}

func TestGetRecords_Retry401(t *testing.T) {
	var attempt int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: []Record{{"sys_id": "001"}}})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	query := NewQueryBuilder()
	result, err := client.GetRecords(context.Background(), "incident", query, 0, 10, nil)
	if err != nil {
		t.Fatalf("GetRecords should succeed after 401 retry: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 record, got %d", len(result))
	}
	if atomic.LoadInt32(&auth.refreshCount) == 0 {
		t.Error("expected ForceRefresh to be called")
	}
}

func TestGetRecords_Retry5xx(t *testing.T) {
	var attempt int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("service unavailable"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: []Record{{"sys_id": "001"}}})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	query := NewQueryBuilder()
	result, err := client.GetRecords(context.Background(), "incident", query, 0, 10, nil)
	if err != nil {
		t.Fatalf("GetRecords should succeed after 5xx retries: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 record, got %d", len(result))
	}
}

func TestGetRecords_Fatal4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	query := NewQueryBuilder()
	_, err := client.GetRecords(context.Background(), "incident", query, 0, 10, nil)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "non-retryable") {
		t.Errorf("error should mention non-retryable: %v", err)
	}
}

func TestGetRecords_429RetryAfter(t *testing.T) {
	var attempt int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempt, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("rate limited"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: []Record{{"sys_id": "001"}}})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	start := time.Now()
	query := NewQueryBuilder()
	result, err := client.GetRecords(context.Background(), "incident", query, 0, 10, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("should succeed after 429: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 record, got %d", len(result))
	}
	// Should have waited at least ~500ms (jitter is [0.5, 1.0] of 1s)
	if elapsed < 400*time.Millisecond {
		t.Errorf("expected retry delay, elapsed only %v", elapsed)
	}
}

func TestGetRecords_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TableResponse{Result: []Record{}})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	query := NewQueryBuilder()
	_, err := client.GetRecords(ctx, "incident", query, 0, 10, nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestInsertRecord_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected JSON content type")
		}
		result := Record{"sys_id": "new-001", "short_description": "Created"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"result": result})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	record := Record{"short_description": "New incident"}
	result, err := client.InsertRecord(context.Background(), "incident", record)
	if err != nil {
		t.Fatalf("InsertRecord failed: %v", err)
	}
	if (*result)["sys_id"] != "new-001" {
		t.Errorf("expected sys_id new-001, got %v", (*result)["sys_id"])
	}
}

func TestUpdateRecord_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/incident/abc123") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		result := Record{"sys_id": "abc123", "state": "2"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"result": result})
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := NewClient(testCfg(srv.URL), auth, logger)

	record := Record{"state": "2"}
	result, err := client.UpdateRecord(context.Background(), "incident", "abc123", record)
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	if (*result)["sys_id"] != "abc123" {
		t.Errorf("expected sys_id abc123, got %v", (*result)["sys_id"])
	}
}

func TestGetRecords_ExhaustsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer srv.Close()

	auth := &mockAuth{token: "test-token"}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Only 1 retry to keep test fast
	cfg := testCfg(srv.URL)
	cfg.MaxRetries = 1
	client := NewClient(cfg, auth, logger)

	query := NewQueryBuilder()
	_, err := client.GetRecords(context.Background(), "incident", query, 0, 10, nil)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "retries") {
		t.Errorf("error should mention retries: %v", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		header string
		want   time.Duration
	}{
		{"", 30 * time.Second},
		{"5", 5 * time.Second},
		{"120", 120 * time.Second},
		{"invalid", 30 * time.Second},
	}
	for _, tt := range tests {
		got := parseRetryAfter(tt.header)
		if got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tt.header, got, tt.want)
		}
	}
}

func TestTruncateBody(t *testing.T) {
	short := "hello"
	if truncateBody([]byte(short)) != short {
		t.Error("short body should not be truncated")
	}

	long := strings.Repeat("x", 600)
	result := truncateBody([]byte(long))
	if len(result) > 510 { // 500 + "..."
		t.Errorf("truncated body too long: %d", len(result))
	}
	if !strings.HasSuffix(result, "...") {
		t.Error("truncated body should end with ...")
	}
}

func TestBuildTableURL(t *testing.T) {
	c := &httpClient{
		baseURL:      "https://instance.service-now.com",
		tableAPIPath: "/api/now/table",
	}
	url, err := c.buildTableURL("incident", "sys_idISNOTEMPTY", 0, 20, []string{"sys_id", "state"})
	if err != nil {
		t.Fatalf("buildTableURL failed: %v", err)
	}
	if !strings.Contains(url, "/api/now/table/incident") {
		t.Errorf("URL missing table path: %s", url)
	}
	if !strings.Contains(url, "sysparm_limit=20") {
		t.Errorf("URL missing limit: %s", url)
	}
	if !strings.Contains(url, "sysparm_fields=sys_id%2Cstate") {
		t.Errorf("URL missing fields: %s", url)
	}
	if !strings.Contains(url, fmt.Sprintf("sysparm_query=%s", "sys_idISNOTEMPTY")) {
		t.Errorf("URL missing query: %s", url)
	}
}
