// Package config provides YAML-based configuration loading, validation, and
// defaults for the ServiceNow-Kafka Bridge.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the bridge.
type Config struct {
	ServiceNow    ServiceNowConfig    `yaml:"servicenow"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Source        SourceConfig        `yaml:"source"`
	Sink          SinkConfig          `yaml:"sink"`
	Offset        OffsetConfig        `yaml:"offset"`
	Observability ObservabilityConfig `yaml:"observability"`
	LogLevel      string              `yaml:"log_level"`
}

// ServiceNowConfig holds ServiceNow instance connection settings.
type ServiceNowConfig struct {
	BaseURL          string     `yaml:"base_url"`
	TableAPIPath     string     `yaml:"table_api_path"`
	Auth             AuthConfig `yaml:"auth"`
	TimeoutSeconds   int        `yaml:"timeout_seconds"`
	ReadTimeoutSecs  int        `yaml:"read_timeout_seconds"`
	ConnTimeoutSecs  int        `yaml:"conn_timeout_seconds"`
	MaxRetries       int        `yaml:"max_retries"`
	RetryBackoffSecs int        `yaml:"retry_backoff_seconds"`
	RateLimitRPS     float64    `yaml:"rate_limit_rps"`
}

// AuthConfig determines which authentication method is used.
type AuthConfig struct {
	Type  string      `yaml:"type"` // "oauth" or "basic"
	OAuth OAuthConfig `yaml:"oauth"`
	Basic BasicConfig `yaml:"basic"`
}

// OAuthConfig holds OAuth password-grant credentials.
type OAuthConfig struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	TokenPath    string `yaml:"token_path"`
}

// BasicConfig holds HTTP Basic Auth credentials.
type BasicConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// KafkaConfig holds kafka broker and security settings.
type KafkaConfig struct {
	Brokers           []string   `yaml:"brokers"`
	TLS               TLSConfig  `yaml:"tls"`
	SASL              SASLConfig `yaml:"sasl"`
	SchemaRegistryURL string     `yaml:"schema_registry_url"`
}

// TLSConfig enables TLS for Kafka connections.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CACert   string `yaml:"ca_cert"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// SASLConfig enables SASL for Kafka connections.
type SASLConfig struct {
	Mechanism string `yaml:"mechanism"` // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// SourceConfig controls the source (ServiceNow → Kafka) pipeline.
type SourceConfig struct {
	Enabled              bool          `yaml:"enabled"`
	TopicPrefix          string        `yaml:"topic_prefix"`
	Tables               []TableConfig `yaml:"tables"`
	FastPollInterval     Duration      `yaml:"fast_poll_interval"`
	SlowPollInterval     Duration      `yaml:"slow_poll_interval"`
	BatchSize            int           `yaml:"batch_size"`
	InitialQueryHoursAgo int           `yaml:"initial_query_hours_ago"`
	TimestampDelaySecs   int           `yaml:"timestamp_delay_seconds"`
}

// TableConfig defines a single ServiceNow table to poll.
type TableConfig struct {
	Name               string   `yaml:"name"`
	TimestampField     string   `yaml:"timestamp_field"`
	IdentifierField    string   `yaml:"identifier_field"`
	Fields             []string `yaml:"fields"`
	Topic              string   `yaml:"topic"`
	Partitioner        string   `yaml:"partitioner"`
	PartitionKeyFields []string `yaml:"partition_key_fields"`
}

// SinkConfig controls the sink (Kafka → ServiceNow) pipeline.
type SinkConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Topics      []SinkTopicConfig `yaml:"topics"`
	Concurrency int               `yaml:"concurrency"`
	DLQTopic    string            `yaml:"dlq_topic"`
	GroupID     string            `yaml:"group_id"`
	// CommitOnPartialFailure controls whether offsets are committed when
	// some records in a batch fail. Defaults to true when unset.
	CommitOnPartialFailure *bool `yaml:"commit_on_partial_failure"`
}

// CommitOnPartialFailureValue returns the effective commit policy.
func (s SinkConfig) CommitOnPartialFailureValue() bool {
	if s.CommitOnPartialFailure == nil {
		return true
	}
	return *s.CommitOnPartialFailure
}

// SinkTopicConfig maps a Kafka topic to a ServiceNow table.
type SinkTopicConfig struct {
	Topic string `yaml:"topic"`
	Table string `yaml:"table"`
}

// OffsetConfig controls how offsets are stored.
type OffsetConfig struct {
	Storage       string   `yaml:"storage"` // "file" or "kafka"
	FilePath      string   `yaml:"file_path"`
	KafkaTopic    string   `yaml:"kafka_topic"`
	FlushInterval Duration `yaml:"flush_interval"`
}

// ObservabilityConfig controls the metrics/health HTTP server.
type ObservabilityConfig struct {
	Addr string `yaml:"addr"`
}

// Duration is a time.Duration that unmarshals from YAML strings like "500ms" or "30s".
type Duration struct {
	time.Duration
}

// UnmarshalYAML implements yaml.Unmarshaler for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

// MarshalYAML implements yaml.Marshaler for Duration.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.String(), nil
}

// Load reads a YAML config file, expands environment variables, and validates.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Expand ${VAR} and $VAR references in the YAML.
	expanded := os.ExpandEnv(string(data))

	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return cfg, nil
}

// applyDefaults sets default values for unset fields.
func applyDefaults(cfg *Config) {
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	// ServiceNow defaults
	sn := &cfg.ServiceNow
	if sn.TableAPIPath == "" {
		sn.TableAPIPath = "/api/now/table"
	}
	if sn.Auth.Type == "" {
		sn.Auth.Type = "oauth"
	}
	if sn.Auth.OAuth.TokenPath == "" {
		sn.Auth.OAuth.TokenPath = "/oauth_token.do"
	}
	if sn.TimeoutSeconds == 0 {
		sn.TimeoutSeconds = 30
	}
	if sn.ReadTimeoutSecs == 0 {
		sn.ReadTimeoutSecs = 25
	}
	if sn.ConnTimeoutSecs == 0 {
		sn.ConnTimeoutSecs = 10
	}
	if sn.MaxRetries == 0 {
		sn.MaxRetries = -1 // unlimited
	}
	if sn.RetryBackoffSecs == 0 {
		sn.RetryBackoffSecs = 30
	}

	// Source defaults
	src := &cfg.Source
	if src.FastPollInterval.Duration == 0 {
		src.FastPollInterval.Duration = 500 * time.Millisecond
	}
	if src.SlowPollInterval.Duration == 0 {
		src.SlowPollInterval.Duration = 30 * time.Second
	}
	if src.BatchSize == 0 {
		src.BatchSize = 20
	}
	if src.InitialQueryHoursAgo == 0 {
		src.InitialQueryHoursAgo = -1
	}

	// Set default topic for each table if not specified
	for i := range src.Tables {
		if src.Tables[i].Topic == "" && src.TopicPrefix != "" {
			src.Tables[i].Topic = src.TopicPrefix + "." + src.Tables[i].Name
		}
		if src.Tables[i].Partitioner == "" {
			src.Tables[i].Partitioner = "default"
		}
	}

	// Offset defaults
	off := &cfg.Offset
	if off.Storage == "" {
		off.Storage = "file"
	}
	if off.FilePath == "" {
		off.FilePath = "offsets.json"
	}
	if off.KafkaTopic == "" {
		off.KafkaTopic = "_bridge_offsets"
	}
	if off.FlushInterval.Duration == 0 {
		off.FlushInterval.Duration = 5 * time.Second
	}

	// Sink defaults
	if cfg.Sink.Enabled {
		if cfg.Sink.Concurrency <= 0 {
			cfg.Sink.Concurrency = 5
		}
		if cfg.Sink.GroupID == "" {
			cfg.Sink.GroupID = "servicenow-kafka-bridge-sink"
		}
		if cfg.Sink.CommitOnPartialFailure == nil {
			defaultCommit := true
			cfg.Sink.CommitOnPartialFailure = &defaultCommit
		}
	}

	// Observability defaults
	if cfg.Observability.Addr == "" {
		cfg.Observability.Addr = ":8080"
	}
}

// validate checks that all required fields are present and valid.
func validate(cfg *Config) error {
	var errs []error

	// ServiceNow
	if cfg.ServiceNow.BaseURL == "" {
		errs = append(errs, errors.New("servicenow.base_url is required"))
	} else if u, err := url.Parse(cfg.ServiceNow.BaseURL); err != nil || u.Scheme == "" {
		errs = append(errs, fmt.Errorf("servicenow.base_url is not a valid URL: %s", cfg.ServiceNow.BaseURL))
	}

	switch cfg.ServiceNow.Auth.Type {
	case "oauth":
		o := cfg.ServiceNow.Auth.OAuth
		if o.ClientID == "" {
			errs = append(errs, errors.New("servicenow.auth.oauth.client_id is required for oauth auth"))
		}
		if o.ClientSecret == "" {
			errs = append(errs, errors.New("servicenow.auth.oauth.client_secret is required for oauth auth"))
		}
		if o.Username == "" {
			errs = append(errs, errors.New("servicenow.auth.oauth.username is required for oauth auth"))
		}
		if o.Password == "" {
			errs = append(errs, errors.New("servicenow.auth.oauth.password is required for oauth auth"))
		}
	case "basic":
		b := cfg.ServiceNow.Auth.Basic
		if b.Username == "" {
			errs = append(errs, errors.New("servicenow.auth.basic.username is required for basic auth"))
		}
		if b.Password == "" {
			errs = append(errs, errors.New("servicenow.auth.basic.password is required for basic auth"))
		}
	default:
		errs = append(errs, fmt.Errorf("servicenow.auth.type must be 'oauth' or 'basic', got %q", cfg.ServiceNow.Auth.Type))
	}

	// Kafka
	if len(cfg.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("kafka.brokers must contain at least one broker"))
	}
	if cfg.Kafka.TLS.Enabled {
		if cfg.Kafka.TLS.CertFile == "" && cfg.Kafka.TLS.KeyFile != "" {
			errs = append(errs, errors.New("kafka.tls.cert_file is required when key_file is set"))
		}
		if cfg.Kafka.TLS.KeyFile == "" && cfg.Kafka.TLS.CertFile != "" {
			errs = append(errs, errors.New("kafka.tls.key_file is required when cert_file is set"))
		}
		for _, entry := range []struct {
			name  string
			value string
		}{
			{name: "kafka.tls.ca_cert", value: cfg.Kafka.TLS.CACert},
			{name: "kafka.tls.cert_file", value: cfg.Kafka.TLS.CertFile},
			{name: "kafka.tls.key_file", value: cfg.Kafka.TLS.KeyFile},
		} {
			if entry.value == "" {
				continue
			}
			if _, err := os.Stat(entry.value); err != nil {
				errs = append(errs, fmt.Errorf("%s not found: %s", entry.name, entry.value))
			}
		}
	}
	if cfg.Kafka.SASL.Mechanism != "" {
		mech := strings.ToUpper(cfg.Kafka.SASL.Mechanism)
		switch mech {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			errs = append(errs, fmt.Errorf("kafka.sasl.mechanism must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512, got %q", cfg.Kafka.SASL.Mechanism))
		}
		if cfg.Kafka.SASL.Username == "" || cfg.Kafka.SASL.Password == "" {
			errs = append(errs, errors.New("kafka.sasl.username and kafka.sasl.password are required when sasl.mechanism is set"))
		}
	}

	// Source
	if cfg.Source.Enabled {
		if cfg.Source.TopicPrefix == "" {
			errs = append(errs, errors.New("source.topic_prefix is required when source is enabled"))
		}
		if len(cfg.Source.Tables) == 0 {
			errs = append(errs, errors.New("source.tables must contain at least one table when source is enabled"))
		}
		for i, tbl := range cfg.Source.Tables {
			if tbl.Name == "" {
				errs = append(errs, fmt.Errorf("source.tables[%d].name is required", i))
			}
			if tbl.TimestampField == "" {
				errs = append(errs, fmt.Errorf("source.tables[%d].timestamp_field is required", i))
			}
			if tbl.IdentifierField == "" {
				errs = append(errs, fmt.Errorf("source.tables[%d].identifier_field is required", i))
			}
			switch tbl.Partitioner {
			case "default", "round_robin", "field_based":
			default:
				errs = append(errs, fmt.Errorf("source.tables[%d].partitioner must be 'default', 'round_robin', or 'field_based', got %q", i, tbl.Partitioner))
			}
			if tbl.Partitioner == "field_based" && len(tbl.PartitionKeyFields) == 0 {
				errs = append(errs, fmt.Errorf("source.tables[%d].partition_key_fields required when partitioner is 'field_based'", i))
			}
		}
	}

	// Sink
	if cfg.Sink.Enabled {
		if len(cfg.Sink.Topics) == 0 {
			errs = append(errs, errors.New("sink.topics must contain at least one topic when sink is enabled"))
		}
		if cfg.Sink.GroupID == "" {
			errs = append(errs, errors.New("sink.group_id is required when sink is enabled"))
		}
	}

	// At least one pipeline must be enabled
	if !cfg.Source.Enabled && !cfg.Sink.Enabled {
		errs = append(errs, errors.New("at least one of source.enabled or sink.enabled must be true"))
	}

	// Offset
	switch cfg.Offset.Storage {
	case "file", "kafka":
	default:
		errs = append(errs, fmt.Errorf("offset.storage must be 'file' or 'kafka', got %q", cfg.Offset.Storage))
	}

	return errors.Join(errs...)
}
