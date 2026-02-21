package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/hamba/avro/v2"
)

// HTTPRegistryClient implements SchemaRegistryClient using the Confluent HTTP API.
type HTTPRegistryClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPRegistryClient(baseURL string) *HTTPRegistryClient {
	return &HTTPRegistryClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *HTTPRegistryClient) GetSchemaID(ctx context.Context, subject string, schema avro.Schema) (int, error) {
	// 1. Prepare request to POST /subjects/{subject}/versions
	// This will register the schema if it doesn't exist and return the ID.
	type registerReq struct {
		Schema string `json:"schema"`
	}
	reqBody, err := json.Marshal(registerReq{Schema: schema.String()})
	if err != nil {
		return 0, fmt.Errorf("encoding schema: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("registry error (status %d): %s", resp.StatusCode, string(body))
	}

	var registerResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
		return 0, fmt.Errorf("decoding response: %w", err)
	}

	return registerResp.ID, nil
}
