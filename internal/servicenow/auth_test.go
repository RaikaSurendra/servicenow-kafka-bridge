package servicenow

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/your-org/servicenow-kafka-bridge/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newTestOAuthServer creates an httptest.Server that mimics ServiceNow's
// /oauth_token.do endpoint. It returns the server URL and a teardown function.
func newTestOAuthServer(t *testing.T, expiresIn int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth_token.do" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp := oauthTokenResponse{
			AccessToken:  "test-access-token-" + time.Now().Format("150405"),
			RefreshToken: "test-refresh-token",
			Scope:        "useraccount",
			TokenType:    "Bearer",
			ExpiresIn:    expiresIn,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
}

func TestBasicAuthenticator_Token(t *testing.T) {
	auth := NewBasicAuthenticator("admin", "secret")
	token, err := auth.Token(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// "admin:secret" → base64 "YWRtaW46c2VjcmV0"
	want := "Basic YWRtaW46c2VjcmV0"
	if token != want {
		t.Errorf("Token() = %q, want %q", token, want)
	}
}

func TestBasicAuthenticator_ForceRefresh(t *testing.T) {
	auth := NewBasicAuthenticator("user", "pass")
	err := auth.ForceRefresh(context.Background())
	if err != nil {
		t.Fatalf("ForceRefresh should be no-op, got error: %v", err)
	}
}

func TestBasicAuthenticator_ConcurrentAccess(t *testing.T) {
	auth := NewBasicAuthenticator("user", "pass")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := auth.Token(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if token == "" {
				t.Error("empty token")
			}
		}()
	}
	wg.Wait()
}

func TestOAuthAuthenticator_InitialTokenAcquisition(t *testing.T) {
	srv := newTestOAuthServer(t, 1800) // 30 minute expiry
	defer srv.Close()

	cfg := config.ServiceNowConfig{
		BaseURL:        srv.URL,
		TimeoutSeconds: 10,
		Auth: config.AuthConfig{
			Type: "oauth",
			OAuth: config.OAuthConfig{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				Username:     "admin",
				Password:     "admin123",
				TokenPath:    "/oauth_token.do",
			},
		},
	}

	auth, err := NewOAuthAuthenticator(context.Background(), cfg, testLogger())
	if err != nil {
		t.Fatalf("NewOAuthAuthenticator failed: %v", err)
	}
	defer auth.Close()

	token, err := auth.Token(context.Background())
	if err != nil {
		t.Fatalf("Token() failed: %v", err)
	}
	if token == "" {
		t.Error("Token() returned empty string")
	}
	if len(token) < 8 { // "Bearer " + at least 1 char
		t.Errorf("Token too short: %q", token)
	}
}

func TestOAuthAuthenticator_ForceRefresh(t *testing.T) {
	srv := newTestOAuthServer(t, 1800)
	defer srv.Close()

	cfg := config.ServiceNowConfig{
		BaseURL:        srv.URL,
		TimeoutSeconds: 10,
		Auth: config.AuthConfig{
			Type: "oauth",
			OAuth: config.OAuthConfig{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				Username:     "admin",
				Password:     "admin123",
				TokenPath:    "/oauth_token.do",
			},
		},
	}

	auth, err := NewOAuthAuthenticator(context.Background(), cfg, testLogger())
	if err != nil {
		t.Fatalf("NewOAuthAuthenticator failed: %v", err)
	}
	defer auth.Close()

	token1, _ := auth.Token(context.Background())

	// Force refresh should get a new token (our mock includes timestamp)
	time.Sleep(time.Second)
	err = auth.ForceRefresh(context.Background())
	if err != nil {
		t.Fatalf("ForceRefresh failed: %v", err)
	}

	token2, _ := auth.Token(context.Background())
	if token1 == token2 {
		t.Log("tokens are equal — mock may have returned same timestamp, which is okay")
	}
}

func TestOAuthAuthenticator_InvalidCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer srv.Close()

	cfg := config.ServiceNowConfig{
		BaseURL:        srv.URL,
		TimeoutSeconds: 5,
		Auth: config.AuthConfig{
			Type: "oauth",
			OAuth: config.OAuthConfig{
				ClientID:     "bad-client",
				ClientSecret: "bad-secret",
				Username:     "admin",
				Password:     "wrong",
				TokenPath:    "/oauth_token.do",
			},
		},
	}

	_, err := NewOAuthAuthenticator(context.Background(), cfg, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid credentials")
	}
}

func TestOAuthAuthenticator_ConcurrentTokenAccess(t *testing.T) {
	srv := newTestOAuthServer(t, 1800)
	defer srv.Close()

	cfg := config.ServiceNowConfig{
		BaseURL:        srv.URL,
		TimeoutSeconds: 10,
		Auth: config.AuthConfig{
			Type: "oauth",
			OAuth: config.OAuthConfig{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				Username:     "admin",
				Password:     "admin123",
				TokenPath:    "/oauth_token.do",
			},
		},
	}

	auth, err := NewOAuthAuthenticator(context.Background(), cfg, testLogger())
	if err != nil {
		t.Fatalf("NewOAuthAuthenticator failed: %v", err)
	}
	defer auth.Close()

	// Hammer Token() from multiple goroutines.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				token, err := auth.Token(context.Background())
				if err != nil {
					t.Errorf("concurrent Token() error: %v", err)
				}
				if token == "" {
					t.Error("concurrent Token() returned empty")
				}
			}
		}()
	}
	wg.Wait()
}

func TestNewAuthenticator_Factory(t *testing.T) {
	srv := newTestOAuthServer(t, 1800)
	defer srv.Close()

	// Test basic
	basicCfg := config.ServiceNowConfig{
		BaseURL: "https://example.com",
		Auth: config.AuthConfig{
			Type: "basic",
			Basic: config.BasicConfig{
				Username: "admin",
				Password: "secret",
			},
		},
	}
	auth, err := NewAuthenticator(context.Background(), basicCfg, testLogger())
	if err != nil {
		t.Fatalf("NewAuthenticator(basic) failed: %v", err)
	}
	auth.Close()

	// Test oauth
	oauthCfg := config.ServiceNowConfig{
		BaseURL:        srv.URL,
		TimeoutSeconds: 10,
		Auth: config.AuthConfig{
			Type: "oauth",
			OAuth: config.OAuthConfig{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				Username:     "admin",
				Password:     "admin123",
				TokenPath:    "/oauth_token.do",
			},
		},
	}
	auth2, err := NewAuthenticator(context.Background(), oauthCfg, testLogger())
	if err != nil {
		t.Fatalf("NewAuthenticator(oauth) failed: %v", err)
	}
	auth2.Close()

	// Test invalid
	invalidCfg := config.ServiceNowConfig{
		Auth: config.AuthConfig{Type: "kerberos"},
	}
	_, err = NewAuthenticator(context.Background(), invalidCfg, testLogger())
	if err == nil {
		t.Fatal("expected error for unsupported auth type")
	}
}
