package sink

import (
	"testing"

	"github.com/your-org/servicenow-kafka-bridge/internal/servicenow"
)

func TestExtractSysID_Present(t *testing.T) {
	record := servicenow.Record{"sys_id": "abc123", "state": "1"}
	id, ok := extractSysID(record)
	if !ok || id != "abc123" {
		t.Errorf("extractSysID = (%q, %v), want (abc123, true)", id, ok)
	}
}

func TestExtractSysID_Missing(t *testing.T) {
	record := servicenow.Record{"state": "1"}
	_, ok := extractSysID(record)
	if ok {
		t.Error("expected ok=false for missing sys_id")
	}
}

func TestExtractSysID_Empty(t *testing.T) {
	record := servicenow.Record{"sys_id": "", "state": "1"}
	_, ok := extractSysID(record)
	if ok {
		t.Error("expected ok=false for empty sys_id")
	}
}

func TestExtractSysID_NonString(t *testing.T) {
	record := servicenow.Record{"sys_id": 12345, "state": "1"}
	_, ok := extractSysID(record)
	if ok {
		t.Error("expected ok=false for non-string sys_id")
	}
}
