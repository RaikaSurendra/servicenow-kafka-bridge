// Package servicenow provides types and utilities for interacting with the ServiceNow Table API.
package servicenow

// Record represents a single ServiceNow table record as a map of field names to values.
type Record map[string]interface{}

// TableResponse represents the JSON response from the ServiceNow Table API.
type TableResponse struct {
	Result []Record `json:"result"`
}

// ErrorResponse represents a ServiceNow API error response body.
type ErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Detail  string `json:"detail"`
	} `json:"error"`
}
