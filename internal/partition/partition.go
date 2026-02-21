// Package partition provides pluggable message partitioning strategies for
// distributing ServiceNow records across Kafka topic partitions.
//
// # Why Does Partitioning Matter?
//
// When a ServiceNow table has millions of records and the Kafka topic has
// multiple partitions, the choice of partition key determines:
//
//   - Ordering: Records with the same key are guaranteed to go to the same
//     partition and therefore arrive in order at the consumer.
//   - Parallelism: Records spread across partitions can be consumed in parallel.
//   - Data locality: Grouping related records (e.g., by company, assignment_group)
//     allows consumers to process them together.
//
// # Available Strategies
//
//   - [DefaultPartitioner]: Uses the sys_id as the partition key. This spreads
//     records evenly across partitions while preserving ordering for the same
//     record (important for change-capture).
//
//   - [RoundRobinPartitioner]: Returns a nil key, causing franz-go to distribute
//     messages round-robin across partitions. Best throughput but no ordering
//     guarantee.
//
//   - [FieldBasedPartitioner]: Hashes one or more field values to determine the
//     partition. Use this to co-locate related records. For example, partitioning
//     by "company" ensures all incidents for the same company go to the same
//     partition.
//
// # Usage
//
//	p := partition.New(tableConfig)
//	key := p.Key(record) // returns the partition key bytes or nil
package partition

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/your-org/servicenow-kafka-bridge/internal/config"
	"github.com/your-org/servicenow-kafka-bridge/internal/servicenow"
)

// Partitioner determines the Kafka message key for a ServiceNow record.
// The key controls which partition the message is sent to.
type Partitioner interface {
	// Key returns the Kafka message key for the given record. Returns nil
	// for round-robin partitioning (no key).
	Key(record servicenow.Record) []byte
}

// New creates a Partitioner based on the table configuration.
//
// The partitioner type is determined by the TableConfig.Partitioner field:
//   - "default": Uses the identifier field (typically sys_id) as the key.
//   - "round_robin": Returns nil key for even distribution.
//   - "field_based": Hashes the specified partition_key_fields.
func New(cfg config.TableConfig) Partitioner {
	switch cfg.Partitioner {
	case "round_robin":
		return &RoundRobinPartitioner{}
	case "field_based":
		return &FieldBasedPartitioner{
			Fields: cfg.PartitionKeyFields,
		}
	default: // "default"
		return &DefaultPartitioner{
			IdentifierField: cfg.IdentifierField,
		}
	}
}

// ----- Default Partitioner -----

// DefaultPartitioner uses the identifier field (typically sys_id) as the
// partition key. This ensures that all updates to the same record go to
// the same partition, maintaining per-record ordering.
//
// For example, if sys_id = "abc123", the key will be []byte("abc123").
type DefaultPartitioner struct {
	IdentifierField string
}

// Key returns the identifier field value as the partition key.
func (d *DefaultPartitioner) Key(record servicenow.Record) []byte {
	val, ok := record[d.IdentifierField]
	if !ok {
		return nil
	}
	return []byte(fmt.Sprintf("%v", val))
}

// ----- Round Robin Partitioner -----

// RoundRobinPartitioner returns a nil key, causing the Kafka client to
// distribute messages evenly across all partitions. This provides the
// best throughput but does not guarantee ordering for individual records.
type RoundRobinPartitioner struct{}

// Key always returns nil for round-robin distribution.
func (r *RoundRobinPartitioner) Key(_ servicenow.Record) []byte {
	return nil
}

// ----- Field-Based Partitioner -----

// FieldBasedPartitioner hashes one or more record field values to produce
// a deterministic partition key. Records with the same field values will
// always produce the same key and therefore land on the same partition.
//
// # Hash Algorithm
//
// The field values are concatenated in sorted field name order, separated by
// a null byte. The concatenated value is then SHA-256 hashed to produce a
// fixed-length key. We use SHA-256 (rather than simpler hashing) because:
//
//   - Uniform distribution across partitions regardless of input patterns.
//   - Deterministic: same fields always produce the same key.
//   - Fixed length: 64 hex characters regardless of input size.
//
// # Example
//
// Given fields=["company", "assignment_group"] and a record with:
//
//	company = "ACME Corp"
//	assignment_group = "IT Support"
//
// The key is SHA-256("ACME Corp\x00IT Support") → "a1b2c3..."
type FieldBasedPartitioner struct {
	Fields []string
}

// Key returns the SHA-256 hash of the specified field values concatenated
// with null byte separators. Fields are sorted alphabetically to ensure
// deterministic key generation regardless of map iteration order.
func (f *FieldBasedPartitioner) Key(record servicenow.Record) []byte {
	if len(f.Fields) == 0 {
		return nil
	}

	// Sort fields for deterministic key generation.
	sorted := make([]string, len(f.Fields))
	copy(sorted, f.Fields)
	sort.Strings(sorted)

	var parts []string
	for _, field := range sorted {
		val, ok := record[field]
		if ok {
			parts = append(parts, fmt.Sprintf("%v", val))
		} else {
			parts = append(parts, "")
		}
	}

	composite := strings.Join(parts, "\x00")
	hash := sha256.Sum256([]byte(composite))
	return []byte(hex.EncodeToString(hash[:]))
}
