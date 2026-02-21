package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validYAML = `
servicenow:
  base_url: https://instance.service-now.com
  auth:
    type: oauth
    oauth:
      client_id: test-id
      client_secret: test-secret
      username: admin
      password: admin123
kafka:
  brokers:
    - localhost:9092
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
sink:
  enabled: false
offset:
  storage: file
`

const sinkYAML = `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: false
sink:
  enabled: true
  topics:
    - topic: sn.incident
      table: incident
`

const saslYAML = `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
  sasl:
    mechanism: PLAIN
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSASLValidation(t *testing.T) {
	path := writeTemp(t, saslYAML)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing SASL credentials")
	}
	if !strings.Contains(err.Error(), "kafka.sasl.username") {
		t.Errorf("error should mention kafka.sasl.username: %v", err)
	}
}

func TestSinkDefaults(t *testing.T) {
	path := writeTemp(t, sinkYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Sink.GroupID != "servicenow-kafka-bridge-sink" {
		t.Errorf("GroupID default = %q", cfg.Sink.GroupID)
	}
	if cfg.Sink.CommitOnPartialFailure == nil || !cfg.Sink.CommitOnPartialFailureValue() {
		t.Errorf("CommitOnPartialFailure default = %v", cfg.Sink.CommitOnPartialFailure)
	}
	if cfg.Sink.Concurrency != 5 {
		t.Errorf("Concurrency default = %d, want 5", cfg.Sink.Concurrency)
	}
}

func TestLoadValidConfig(t *testing.T) {
	path := writeTemp(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.ServiceNow.BaseURL != "https://instance.service-now.com" {
		t.Errorf("BaseURL = %q", cfg.ServiceNow.BaseURL)
	}
	if cfg.ServiceNow.Auth.Type != "oauth" {
		t.Errorf("Auth.Type = %q", cfg.ServiceNow.Auth.Type)
	}
	if len(cfg.Source.Tables) != 1 || cfg.Source.Tables[0].Name != "incident" {
		t.Errorf("Tables = %v", cfg.Source.Tables)
	}
}

func TestDefaults(t *testing.T) {
	path := writeTemp(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.ServiceNow.TableAPIPath != "/api/now/table" {
		t.Errorf("TableAPIPath default = %q, want /api/now/table", cfg.ServiceNow.TableAPIPath)
	}
	if cfg.ServiceNow.TimeoutSeconds != 30 {
		t.Errorf("TimeoutSeconds default = %d, want 30", cfg.ServiceNow.TimeoutSeconds)
	}
	if cfg.ServiceNow.ReadTimeoutSecs != 25 {
		t.Errorf("ReadTimeoutSecs default = %d, want 25", cfg.ServiceNow.ReadTimeoutSecs)
	}
	if cfg.ServiceNow.ConnTimeoutSecs != 10 {
		t.Errorf("ConnTimeoutSecs default = %d, want 10", cfg.ServiceNow.ConnTimeoutSecs)
	}
	if cfg.ServiceNow.MaxRetries != -1 {
		t.Errorf("MaxRetries default = %d, want -1", cfg.ServiceNow.MaxRetries)
	}
	if cfg.ServiceNow.RetryBackoffSecs != 30 {
		t.Errorf("RetryBackoffSecs default = %d, want 30", cfg.ServiceNow.RetryBackoffSecs)
	}
	if cfg.Source.FastPollInterval.Duration != 500*time.Millisecond {
		t.Errorf("FastPollInterval default = %v", cfg.Source.FastPollInterval)
	}
	if cfg.Source.SlowPollInterval.Duration != 30*time.Second {
		t.Errorf("SlowPollInterval default = %v", cfg.Source.SlowPollInterval)
	}
	if cfg.Source.BatchSize != 20 {
		t.Errorf("BatchSize default = %d, want 20", cfg.Source.BatchSize)
	}
	if cfg.Source.InitialQueryHoursAgo != -1 {
		t.Errorf("InitialQueryHoursAgo default = %d, want -1", cfg.Source.InitialQueryHoursAgo)
	}
	if cfg.Offset.Storage != "file" {
		t.Errorf("Offset.Storage default = %q, want file", cfg.Offset.Storage)
	}
	if cfg.Offset.FlushInterval.Duration != 5*time.Second {
		t.Errorf("FlushInterval default = %v", cfg.Offset.FlushInterval)
	}
	if cfg.Observability.Addr != ":8080" {
		t.Errorf("Observability.Addr default = %q, want :8080", cfg.Observability.Addr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default = %q, want info", cfg.LogLevel)
	}

	// Table-level defaults
	tbl := cfg.Source.Tables[0]
	if tbl.Topic != "sn.incident" {
		t.Errorf("Table topic default = %q, want sn.incident", tbl.Topic)
	}
	if tbl.Partitioner != "default" {
		t.Errorf("Table partitioner default = %q, want default", tbl.Partitioner)
	}
}

func TestMissingBaseURL(t *testing.T) {
	yaml := `
servicenow:
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing base_url")
	}
	if !strings.Contains(err.Error(), "base_url") {
		t.Errorf("error should mention base_url: %v", err)
	}
}

func TestInvalidAuthType(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: invalid
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid auth type")
	}
	if !strings.Contains(err.Error(), "oauth") {
		t.Errorf("error should mention valid types: %v", err)
	}
}

func TestMissingOAuthFields(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: oauth
    oauth:
      client_id: ""
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing oauth fields")
	}
	if !strings.Contains(err.Error(), "client_id") {
		t.Errorf("error should mention client_id: %v", err)
	}
}

func TestMissingTableFields(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: ""
      timestamp_field: ""
      identifier_field: ""
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing table fields")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "name") {
		t.Errorf("error should mention name: %v", err)
	}
	if !strings.Contains(errStr, "timestamp_field") {
		t.Errorf("error should mention timestamp_field: %v", err)
	}
	if !strings.Contains(errStr, "identifier_field") {
		t.Errorf("error should mention identifier_field: %v", err)
	}
}

func TestEnvVarExpansion(t *testing.T) {
	t.Setenv("SN_BASE_URL", "https://from-env.service-now.com")
	t.Setenv("SN_CLIENT_ID", "env-client-id")
	t.Setenv("SN_CLIENT_SECRET", "env-secret")
	t.Setenv("SN_USER", "env-user")
	t.Setenv("SN_PASS", "env-pass")

	yaml := `
servicenow:
  base_url: ${SN_BASE_URL}
  auth:
    type: oauth
    oauth:
      client_id: ${SN_CLIENT_ID}
      client_secret: ${SN_CLIENT_SECRET}
      username: ${SN_USER}
      password: ${SN_PASS}
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ServiceNow.BaseURL != "https://from-env.service-now.com" {
		t.Errorf("BaseURL = %q, want env value", cfg.ServiceNow.BaseURL)
	}
	if cfg.ServiceNow.Auth.OAuth.ClientID != "env-client-id" {
		t.Errorf("ClientID = %q, want env value", cfg.ServiceNow.Auth.OAuth.ClientID)
	}
}

func TestNoPipelineEnabled(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: false
sink:
  enabled: false
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when no pipeline is enabled")
	}
	if !strings.Contains(err.Error(), "source.enabled") {
		t.Errorf("error should mention enabling a pipeline: %v", err)
	}
}

func TestBasicAuthConfig(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: admin
      password: secret
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ServiceNow.Auth.Type != "basic" {
		t.Errorf("Auth.Type = %q, want basic", cfg.ServiceNow.Auth.Type)
	}
	if cfg.ServiceNow.Auth.Basic.Username != "admin" {
		t.Errorf("Basic.Username = %q", cfg.ServiceNow.Auth.Basic.Username)
	}
}

func TestFieldBasedPartitionerRequiresFields(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
      partitioner: field_based
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for field_based partitioner without key fields")
	}
	if !strings.Contains(err.Error(), "partition_key_fields") {
		t.Errorf("error should mention partition_key_fields: %v", err)
	}
}

func TestInvalidURL(t *testing.T) {
	yaml := `
servicenow:
  base_url: "not-a-url"
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
	if !strings.Contains(err.Error(), "valid URL") {
		t.Errorf("error should mention valid URL: %v", err)
	}
}

func TestFileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestCustomDuration(t *testing.T) {
	yaml := `
servicenow:
  base_url: https://example.com
  auth:
    type: basic
    basic:
      username: user
      password: pass
kafka:
  brokers: [localhost:9092]
source:
  enabled: true
  topic_prefix: sn
  fast_poll_interval: "200ms"
  slow_poll_interval: "1m"
  tables:
    - name: incident
      timestamp_field: sys_updated_on
      identifier_field: sys_id
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Source.FastPollInterval.Duration != 200*time.Millisecond {
		t.Errorf("FastPollInterval = %v, want 200ms", cfg.Source.FastPollInterval)
	}
	if cfg.Source.SlowPollInterval.Duration != time.Minute {
		t.Errorf("SlowPollInterval = %v, want 1m", cfg.Source.SlowPollInterval)
	}
}
