package kafka

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

// GenerateAvroSchema creates a simple Avro record schema for a ServiceNow table and its fields.
// For now, it treats all fields as optional strings, which is safe for the ServiceNow REST API.
func GenerateAvroSchema(tableName string, fields []string) (avro.Schema, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("cannot generate schema with no fields")
	}

	avroFields := make([]*avro.Field, 0, len(fields))
	for _, f := range fields {
		// Create a union schema: ["null", "string"] to make the field optional.
		schema, _ := avro.NewUnionSchema([]avro.Schema{
			&avro.NullSchema{},
			avro.NewPrimitiveSchema(avro.String, nil),
		})

		field, err := avro.NewField(f, schema, avro.WithDefault(nil))
		if err != nil {
			return nil, fmt.Errorf("creating field %s: %w", f, err)
		}
		avroFields = append(avroFields, field)
	}

	recordSchema, err := avro.NewRecordSchema(tableName, "com.servicenow.bridge", avroFields)
	if err != nil {
		return nil, fmt.Errorf("creating record schema: %w", err)
	}

	return recordSchema, nil
}
