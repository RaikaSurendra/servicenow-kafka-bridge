package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
)

// SchemaRegistryClient is a simple client for interacting with a Confluent-compatible Schema Registry.
type SchemaRegistryClient interface {
	// GetSchemaID returns the ID for the given subject's schema.
	GetSchemaID(ctx context.Context, subject string, schema avro.Schema) (int, error)
}

// AvroSerializer handles converting ServiceNow records to Avro bytes with a Confluent magic byte prefix.
type AvroSerializer struct {
	registry SchemaRegistryClient
	schemas  map[string]avro.Schema
}

func NewAvroSerializer(registry SchemaRegistryClient) *AvroSerializer {
	return &AvroSerializer{
		registry: registry,
		schemas:  make(map[string]avro.Schema),
	}
}

// Serialize converts a map (ServiceNow record) to Avro bytes.
// It follows the Confluent Wire Format:
// [Magic Byte (0)] [Schema ID (4 bytes)] [Avro Data]
func (s *AvroSerializer) Serialize(ctx context.Context, subject string, schema avro.Schema, record interface{}) ([]byte, error) {
	schemaID, err := s.registry.GetSchemaID(ctx, subject, schema)
	if err != nil {
		return nil, fmt.Errorf("getting schema ID for subject %s: %w", subject, err)
	}

	data, err := avro.Marshal(schema, record)
	if err != nil {
		return nil, fmt.Errorf("marshaling avro: %w", err)
	}

	// Confluent wire format: 1 byte magic + 4 bytes schema ID + data
	result := make([]byte, 5+len(data))
	result[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(result[1:5], uint32(schemaID))
	copy(result[5:], data)

	return result, nil
}
