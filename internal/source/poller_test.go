package source

import (
	"testing"
	"time"

	"github.com/your-org/servicenow-kafka-bridge/internal/config"
	"github.com/your-org/servicenow-kafka-bridge/internal/offset"
	"github.com/your-org/servicenow-kafka-bridge/internal/servicenow"
)

// TestBuildUnboundedQuery verifies the query generated for the first poll
// when no offset exists and initial_query_hours_ago is -1 (unbounded).
func TestBuildUnboundedQuery(t *testing.T) {
	p := &Poller{
		table: config.TableConfig{
			Name:            "incident",
			TimestampField:  "sys_updated_on",
			IdentifierField: "sys_id",
		},
		timestampField:       "sys_updated_on",
		identifierField:      "sys_id",
		initialQueryHoursAgo: -1,
		currentOffset:        offset.Offset{},
	}

	query := p.buildQuery()
	q := query.Build()

	// Should contain ISNOTEMPTY and ORDERBY.
	assertContains(t, q, "sys_idISNOTEMPTY")
	assertContains(t, q, "ORDERBYsys_updated_on")
	assertContains(t, q, "ORDERBYsys_id")
}

// TestBuildBoundedQuery verifies the two-clause bounded query generated
// when an offset exists. This must exactly match the Java reference.
func TestBuildBoundedQuery(t *testing.T) {
	from := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)

	p := &Poller{
		table: config.TableConfig{
			Name:            "incident",
			TimestampField:  "sys_updated_on",
			IdentifierField: "sys_id",
		},
		timestampField:  "sys_updated_on",
		identifierField: "sys_id",
		currentOffset: offset.Offset{
			Timestamp:      from.Unix(),
			LastIdentifier: "abc123",
		},
		timestampDelaySecs: 0,
	}

	query := p.buildBoundedQuery(from, from.Add(30*time.Minute))
	q := query.Build()

	// Clause 1: exact timestamp + identifier filter
	assertContains(t, q, "sys_updated_on=javascript:gs.dateGenerate('2024-03-15','10:00:00')")
	assertContains(t, q, "sys_id>abc123")

	// Union operator
	assertContains(t, q, "^NQ")

	// Clause 2: between timestamps
	assertContains(t, q, "sys_updated_on>javascript:gs.dateGenerate('2024-03-15','10:00:00')")
	assertContains(t, q, "sys_updated_on<javascript:gs.dateGenerate('2024-03-15','10:30:00')")

	// Both clauses should filter empty identifiers
	// Count occurrences of ISNOTEMPTY
	count := countOccurrences(q, "sys_idISNOTEMPTY")
	if count != 2 {
		t.Errorf("expected 2 ISNOTEMPTY clauses, got %d in query: %s", count, q)
	}
}

// TestBuildBoundedQueryNoLastIdentifier verifies that the bounded query
// does not include a >lastID clause when no identifier has been seen yet.
func TestBuildBoundedQueryNoLastIdentifier(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	p := &Poller{
		timestampField:  "sys_updated_on",
		identifierField: "sys_id",
		currentOffset: offset.Offset{
			Timestamp:      from.Unix(),
			LastIdentifier: "", // no identifier yet
		},
	}

	query := p.buildBoundedQuery(from, from.Add(time.Hour))
	q := query.Build()

	// Should NOT have sys_id>...  but SHOULD have timestamp=...
	assertContains(t, q, "sys_updated_on=javascript:gs.dateGenerate")
	assertNotContains(t, q, "sys_id>")
}

// TestGetFromDateTimeUTC_NoOffset verifies that the initial poll uses
// initial_query_hours_ago to calculate the from time.
func TestGetFromDateTimeUTC_NoOffset(t *testing.T) {
	p := &Poller{
		currentOffset:        offset.Offset{},
		initialQueryHoursAgo: 24,
	}

	from := p.getFromDateTimeUTC()
	if from == nil {
		t.Fatal("expected non-nil from time with initialQueryHoursAgo=24")
	}

	expectedRange := time.Now().UTC().Add(-24 * time.Hour)
	diff := from.Sub(expectedRange).Abs()
	if diff > time.Second {
		t.Errorf("from time off by %v", diff)
	}
}

// TestGetFromDateTimeUTC_Unbounded verifies that -1 means unbounded.
func TestGetFromDateTimeUTC_Unbounded(t *testing.T) {
	p := &Poller{
		currentOffset:        offset.Offset{},
		initialQueryHoursAgo: -1,
	}

	from := p.getFromDateTimeUTC()
	if from != nil {
		t.Errorf("expected nil for unbounded, got %v", from)
	}
}

// TestGetFromDateTimeUTC_WithOffset verifies that the offset timestamp is used.
func TestGetFromDateTimeUTC_WithOffset(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	p := &Poller{
		currentOffset: offset.Offset{
			Timestamp:      ts.Unix(),
			LastIdentifier: "abc",
		},
		initialQueryHoursAgo: 24, // should be ignored when offset exists
	}

	from := p.getFromDateTimeUTC()
	if from == nil {
		t.Fatal("expected non-nil from time with offset")
	}
	if !from.Equal(ts) {
		t.Errorf("from = %v, want %v", from, ts)
	}
}

// TestUpdateOffset verifies that the offset is updated from record fields.
func TestUpdateOffset(t *testing.T) {
	p := &Poller{
		timestampField:  "sys_updated_on",
		identifierField: "sys_id",
		currentOffset:   offset.Offset{},
	}

	record := servicenow.Record{
		"sys_id":         "xyz789",
		"sys_updated_on": "2024-06-15 14:30:00",
	}

	p.updateOffset(record)

	if p.currentOffset.LastIdentifier != "xyz789" {
		t.Errorf("LastIdentifier = %q, want xyz789", p.currentOffset.LastIdentifier)
	}

	expectedTs := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC).Unix()
	if p.currentOffset.Timestamp != expectedTs {
		t.Errorf("Timestamp = %d, want %d", p.currentOffset.Timestamp, expectedTs)
	}
}

// TestUpdateOffset_BadTimestamp verifies graceful handling of unparseable timestamps.
func TestUpdateOffset_BadTimestamp(t *testing.T) {
	p := &Poller{
		timestampField:  "sys_updated_on",
		identifierField: "sys_id",
		currentOffset:   offset.Offset{Timestamp: 1000, LastIdentifier: "old"},
		logger:          testSourceLogger(),
	}

	record := servicenow.Record{
		"sys_id":         "new123",
		"sys_updated_on": "not-a-timestamp",
	}

	// Should update identifier but not timestamp.
	p.updateOffset(record)

	if p.currentOffset.LastIdentifier != "new123" {
		t.Errorf("LastIdentifier should still be updated")
	}
	if p.currentOffset.Timestamp != 1000 {
		t.Error("Timestamp should not change on parse failure")
	}
}

func TestGetThroughDateTimeUTC(t *testing.T) {
	p := &Poller{timestampDelaySecs: 5}
	through := p.getThroughDateTimeUTC()
	expected := time.Now().UTC().Add(-5 * time.Second)
	diff := through.Sub(expected).Abs()
	if diff > time.Second {
		t.Errorf("through time off by %v", diff)
	}
}

// Helper functions

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !contains(s, substr) {
		t.Errorf("query %q does not contain %q", s, substr)
	}
}

func assertNotContains(t *testing.T, s, substr string) {
	t.Helper()
	if contains(s, substr) {
		t.Errorf("query %q should not contain %q", s, substr)
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func countOccurrences(s, substr string) int {
	count := 0
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			count++
		}
	}
	return count
}
