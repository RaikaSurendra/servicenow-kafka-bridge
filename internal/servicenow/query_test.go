package servicenow

import (
	"testing"
	"time"
)

func TestWhereEquals(t *testing.T) {
	q := NewQueryBuilder().WhereEquals("company", "1245")
	got := q.Build()
	want := "company=1245"
	if got != want {
		t.Errorf("WhereEquals: got %q, want %q", got, want)
	}
}

func TestMultipleWhereClauses(t *testing.T) {
	q := NewQueryBuilder().
		WhereEquals("company", "1245").
		WhereEquals("someotherkey", "1111")
	got := q.Build()
	want := "company=1245^someotherkey=1111"
	if got != want {
		t.Errorf("Multiple where: got %q, want %q", got, want)
	}
}

func TestWhereNotEquals(t *testing.T) {
	q := NewQueryBuilder().WhereNotEquals("status", "closed")
	got := q.Build()
	want := "status!=closed"
	if got != want {
		t.Errorf("WhereNotEquals: got %q, want %q", got, want)
	}
}

func TestWhereGreaterThan(t *testing.T) {
	q := NewQueryBuilder().WhereGreaterThan("priority", "3")
	got := q.Build()
	want := "priority>3"
	if got != want {
		t.Errorf("WhereGreaterThan: got %q, want %q", got, want)
	}
}

func TestWhereLessThan(t *testing.T) {
	q := NewQueryBuilder().WhereLessThan("priority", "3")
	got := q.Build()
	want := "priority<3"
	if got != want {
		t.Errorf("WhereLessThan: got %q, want %q", got, want)
	}
}

func TestWhereIsNotEmpty(t *testing.T) {
	q := NewQueryBuilder().WhereIsNotEmpty("sys_id")
	got := q.Build()
	want := "sys_idISNOTEMPTY"
	if got != want {
		t.Errorf("WhereIsNotEmpty: got %q, want %q", got, want)
	}
}

func TestWhereIsEmpty(t *testing.T) {
	q := NewQueryBuilder().WhereIsEmpty("sys_id")
	got := q.Build()
	want := "sys_idISEMPTY"
	if got != want {
		t.Errorf("WhereIsEmpty: got %q, want %q", got, want)
	}
}

func TestOrderByAsc(t *testing.T) {
	q := NewQueryBuilder().
		WhereIsNotEmpty("sys_id").
		OrderByAsc("sys_updated_on").
		OrderByAsc("sys_id")
	got := q.Build()
	want := "sys_idISNOTEMPTY^ORDERBYsys_updated_on^ORDERBYsys_id"
	if got != want {
		t.Errorf("OrderByAsc: got %q, want %q", got, want)
	}
}

func TestOrderByDesc(t *testing.T) {
	q := NewQueryBuilder().OrderByDesc("sys_updated_on")
	got := q.Build()
	want := "ORDERBYDESCsys_updated_on"
	if got != want {
		t.Errorf("OrderByDesc: got %q, want %q", got, want)
	}
}

func TestOrWhereEquals(t *testing.T) {
	q := NewQueryBuilder().
		WhereEquals("status", "open").
		OrWhereEquals("status", "pending")
	got := q.Build()
	want := "status=open^ORstatus=pending"
	if got != want {
		t.Errorf("OrWhereEquals: got %q, want %q", got, want)
	}
}

func TestToServiceNowDateTime(t *testing.T) {
	ts := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
	got := ToServiceNowDateTime(ts)
	want := "javascript:gs.dateGenerate('2024-01-15','14:30:00')"
	if got != want {
		t.Errorf("ToServiceNowDateTime: got %q, want %q", got, want)
	}
}

func TestWhereTimestampEquals(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	q := NewQueryBuilder().WhereTimestampEquals("sys_updated_on", ts)
	got := q.Build()
	want := "sys_updated_on=javascript:gs.dateGenerate('2024-06-01','12:00:00')"
	if got != want {
		t.Errorf("WhereTimestampEquals: got %q, want %q", got, want)
	}
}

func TestWhereBetweenExclusive(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	through := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	q := NewQueryBuilder().WhereBetweenExclusive("sys_updated_on", from, through)
	got := q.Build()
	want := "sys_updated_on>javascript:gs.dateGenerate('2024-01-01','00:00:00')^sys_updated_on<javascript:gs.dateGenerate('2024-01-31','23:59:59')"
	if got != want {
		t.Errorf("WhereBetweenExclusive: got %q, want %q", got, want)
	}
}

func TestWhereBetweenInclusive(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	through := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	q := NewQueryBuilder().WhereBetweenInclusive("sys_updated_on", from, through)
	got := q.Build()
	want := "sys_updated_on>=javascript:gs.dateGenerate('2024-01-01','00:00:00')^sys_updated_on<=javascript:gs.dateGenerate('2024-01-31','23:59:59')"
	if got != want {
		t.Errorf("WhereBetweenInclusive: got %q, want %q", got, want)
	}
}

func TestSanitizeValue(t *testing.T) {
	// Caret in value should be escaped to ^^
	q := NewQueryBuilder().WhereEquals("description", "a^b")
	got := q.Build()
	want := "description=a^^b"
	if got != want {
		t.Errorf("SanitizeValue: got %q, want %q", got, want)
	}
}

func TestSanitizeMultipleCarets(t *testing.T) {
	q := NewQueryBuilder().WhereEquals("desc", "a^b^c")
	got := q.Build()
	want := "desc=a^^b^^c"
	if got != want {
		t.Errorf("SanitizeMultipleCarets: got %q, want %q", got, want)
	}
}

func TestUnion(t *testing.T) {
	// Bounded query pattern with two clauses unioned by ^NQ
	from := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	through := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	lastID := "abc123"

	clause1 := NewQueryBuilder().
		WhereTimestampEquals("sys_updated_on", from).
		WhereGreaterThan("sys_id", lastID).
		WhereIsNotEmpty("sys_id")

	clause2 := NewQueryBuilder().
		WhereBetweenExclusive("sys_updated_on", from, through).
		WhereIsNotEmpty("sys_id").
		OrderByAsc("sys_updated_on").
		OrderByAsc("sys_id")

	clause1.Union(clause2)
	got := clause1.Build()

	wantParts := []string{
		"sys_updated_on=javascript:gs.dateGenerate('2024-03-15','10:00:00')",
		"^sys_id>abc123",
		"^sys_idISNOTEMPTY",
		"^NQ",
		"sys_updated_on>javascript:gs.dateGenerate('2024-03-15','10:00:00')",
		"^sys_updated_on<javascript:gs.dateGenerate('2024-03-15','10:30:00')",
		"^sys_idISNOTEMPTY",
		"^ORDERBYsys_updated_on",
		"^ORDERBYsys_id",
	}
	want := ""
	for _, p := range wantParts {
		want += p
	}
	if got != want {
		t.Errorf("Union bounded query:\ngot  %q\nwant %q", got, want)
	}
}

func TestUnboundedQuery(t *testing.T) {
	// Unbounded query: fetch all records ordered by timestamp and identifier
	q := NewQueryBuilder().
		WhereIsNotEmpty("sys_id").
		OrderByAsc("sys_updated_on").
		OrderByAsc("sys_id")
	got := q.Build()
	want := "sys_idISNOTEMPTY^ORDERBYsys_updated_on^ORDERBYsys_id"
	if got != want {
		t.Errorf("Unbounded query: got %q, want %q", got, want)
	}
}

func TestEmptyBuilder(t *testing.T) {
	q := NewQueryBuilder()
	got := q.Build()
	if got != "" {
		t.Errorf("Empty builder: got %q, want empty string", got)
	}
}

func TestWhereIsAnything(t *testing.T) {
	q := NewQueryBuilder().WhereIsAnything("field")
	got := q.Build()
	want := "fieldANYTHING"
	if got != want {
		t.Errorf("WhereIsAnything: got %q, want %q", got, want)
	}
}

func TestWhereIsEmptyString(t *testing.T) {
	q := NewQueryBuilder().WhereIsEmptyString("field")
	got := q.Build()
	want := "fieldEMPTYSTRING"
	if got != want {
		t.Errorf("WhereIsEmptyString: got %q, want %q", got, want)
	}
}

func TestGreaterThanOrEqual(t *testing.T) {
	q := NewQueryBuilder().WhereGreaterThanOrEqual("priority", "2")
	got := q.Build()
	want := "priority>=2"
	if got != want {
		t.Errorf("WhereGreaterThanOrEqual: got %q, want %q", got, want)
	}
}

func TestLessThanOrEqual(t *testing.T) {
	q := NewQueryBuilder().WhereLessThanOrEqual("priority", "5")
	got := q.Build()
	want := "priority<=5"
	if got != want {
		t.Errorf("WhereLessThanOrEqual: got %q, want %q", got, want)
	}
}

func TestOrWhereNotEquals(t *testing.T) {
	q := NewQueryBuilder().
		WhereNotEquals("status", "closed").
		OrWhereNotEquals("status", "cancelled")
	got := q.Build()
	want := "status!=closed^ORstatus!=cancelled"
	if got != want {
		t.Errorf("OrWhereNotEquals: got %q, want %q", got, want)
	}
}
