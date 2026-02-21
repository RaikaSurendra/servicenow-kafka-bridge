package partition

import (
	"fmt"
	"testing"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/servicenow"
)

func TestDefaultPartitioner_KeyFromSysID(t *testing.T) {
	p := &DefaultPartitioner{IdentifierField: "sys_id"}
	record := servicenow.Record{"sys_id": "abc123", "state": "1"}
	key := p.Key(record)
	if string(key) != "abc123" {
		t.Errorf("Key = %q, want %q", key, "abc123")
	}
}

func TestDefaultPartitioner_MissingField(t *testing.T) {
	p := &DefaultPartitioner{IdentifierField: "sys_id"}
	record := servicenow.Record{"state": "1"}
	key := p.Key(record)
	if key != nil {
		t.Errorf("Key should be nil for missing field, got %q", key)
	}
}

func TestRoundRobinPartitioner_ReturnsNil(t *testing.T) {
	p := &RoundRobinPartitioner{}
	record := servicenow.Record{"sys_id": "abc123"}
	key := p.Key(record)
	if key != nil {
		t.Errorf("Key should be nil for round-robin, got %q", key)
	}
}

func TestFieldBasedPartitioner_SingleField(t *testing.T) {
	p := &FieldBasedPartitioner{Fields: []string{"company"}}
	r1 := servicenow.Record{"company": "ACME Corp", "sys_id": "001"}
	r2 := servicenow.Record{"company": "ACME Corp", "sys_id": "002"}
	r3 := servicenow.Record{"company": "Other Inc", "sys_id": "003"}

	key1 := p.Key(r1)
	key2 := p.Key(r2)
	key3 := p.Key(r3)

	if string(key1) != string(key2) {
		t.Error("same company should produce same key")
	}
	if string(key1) == string(key3) {
		t.Error("different companies should produce different keys")
	}
}

func TestFieldBasedPartitioner_MultipleFields(t *testing.T) {
	p := &FieldBasedPartitioner{Fields: []string{"company", "assignment_group"}}
	r1 := servicenow.Record{
		"company":          "ACME",
		"assignment_group": "IT",
	}
	r2 := servicenow.Record{
		"company":          "ACME",
		"assignment_group": "HR",
	}

	key1 := p.Key(r1)
	key2 := p.Key(r2)

	if string(key1) == string(key2) {
		t.Error("different assignment groups should produce different keys")
	}
}

func TestFieldBasedPartitioner_DeterministicOrder(t *testing.T) {
	// Field order in config shouldn't matter — keys should be deterministic.
	p1 := &FieldBasedPartitioner{Fields: []string{"company", "assignment_group"}}
	p2 := &FieldBasedPartitioner{Fields: []string{"assignment_group", "company"}}

	record := servicenow.Record{
		"company":          "ACME",
		"assignment_group": "IT",
	}

	key1 := p1.Key(record)
	key2 := p2.Key(record)

	if string(key1) != string(key2) {
		t.Errorf("field order should not affect key: %q vs %q", key1, key2)
	}
}

func TestFieldBasedPartitioner_MissingField(t *testing.T) {
	p := &FieldBasedPartitioner{Fields: []string{"company", "missing_field"}}
	record := servicenow.Record{"company": "ACME"}

	key := p.Key(record)
	if key == nil {
		t.Error("key should not be nil even with missing fields")
	}
	if len(key) != 64 { // SHA-256 hex encoded
		t.Errorf("key length should be 64, got %d", len(key))
	}
}

func TestFieldBasedPartitioner_EmptyFields(t *testing.T) {
	p := &FieldBasedPartitioner{Fields: []string{}}
	record := servicenow.Record{"sys_id": "abc"}
	key := p.Key(record)
	if key != nil {
		t.Errorf("empty fields should return nil key, got %q", key)
	}
}

func TestNew_Factory(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.TableConfig
		wantType string
	}{
		{
			name:     "default",
			cfg:      config.TableConfig{Partitioner: "default", IdentifierField: "sys_id"},
			wantType: "*partition.DefaultPartitioner",
		},
		{
			name:     "round_robin",
			cfg:      config.TableConfig{Partitioner: "round_robin"},
			wantType: "*partition.RoundRobinPartitioner",
		},
		{
			name:     "field_based",
			cfg:      config.TableConfig{Partitioner: "field_based", PartitionKeyFields: []string{"company"}},
			wantType: "*partition.FieldBasedPartitioner",
		},
		{
			name:     "empty defaults to default",
			cfg:      config.TableConfig{Partitioner: "", IdentifierField: "sys_id"},
			wantType: "*partition.DefaultPartitioner",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.cfg)
			got := typeName(p)
			if got != tt.wantType {
				t.Errorf("New() type = %q, want %q", got, tt.wantType)
			}
		})
	}
}

func typeName(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

func TestFieldBasedPartitioner_FixedKeyLength(t *testing.T) {
	p := &FieldBasedPartitioner{Fields: []string{"company"}}

	// Short value
	key1 := p.Key(servicenow.Record{"company": "A"})
	// Long value
	key2 := p.Key(servicenow.Record{"company": "A very long company name that goes on and on"})

	if len(key1) != len(key2) {
		t.Errorf("keys should have equal length: %d vs %d", len(key1), len(key2))
	}
	if len(key1) != 64 {
		t.Errorf("SHA-256 hex key should be 64 chars, got %d", len(key1))
	}
}
