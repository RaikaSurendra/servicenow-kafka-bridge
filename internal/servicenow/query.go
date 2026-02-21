// Package servicenow provides a ServiceNow Table API query builder.
package servicenow

import (
	"fmt"
	"strings"
	"time"
)

// ServiceNow query syntax constants.
const (
	snAND         = "^"
	snOR          = "^OR"
	snNQ          = "^NQ"
	snIS          = "="
	snISNOT       = "!="
	snSTARTSWITH  = "STARTSWITH"
	snLIKE        = "LIKE"
	snNOTLIKE     = "NOTLIKE"
	snISANYTHING  = "ANYTHING"
	snISEMPTYSTR  = "EMPTYSTRING"
	snISEMPTY     = "ISEMPTY"
	snISNOTEMPTY  = "ISNOTEMPTY"
	snGTE         = ">="
	snLTE         = "<="
	snLT          = "<"
	snGT          = ">"
	snORDERBY     = "ORDERBY"
	snORDERBYDESC = "ORDERBYDESC"
)

// QueryBuilder constructs ServiceNow encoded query strings using a fluent API.
//
// Example output: "company=1245^someotherkey=1111"
type QueryBuilder struct {
	query strings.Builder
}

// NewQueryBuilder creates a new empty QueryBuilder.
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

// sanitizeValue escapes the '^' character in values to prevent query injection.
// In ServiceNow query syntax, '^' is the AND separator, so literal carets in
// values must be escaped as '^^'.
func sanitizeValue(v string) string {
	if strings.TrimSpace(v) == "" {
		return v
	}
	return strings.ReplaceAll(v, snAND, snAND+snAND)
}

// Build returns the final query string, stripping the leading '^' separator.
func (q *QueryBuilder) Build() string {
	s := q.query.String()
	return strings.TrimLeft(s, "^")
}

// WhereEquals adds: ^field=value
func (q *QueryBuilder) WhereEquals(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snIS, value)
	return q
}

// OrWhereEquals adds: ^ORfield=value
func (q *QueryBuilder) OrWhereEquals(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snOR, field, snIS, value)
	return q
}

// WhereNotEquals adds: ^field!=value
func (q *QueryBuilder) WhereNotEquals(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snISNOT, value)
	return q
}

// OrWhereNotEquals adds: ^ORfield!=value
func (q *QueryBuilder) OrWhereNotEquals(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snOR, field, snISNOT, value)
	return q
}

// WhereGreaterThan adds: ^field>value
func (q *QueryBuilder) WhereGreaterThan(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snGT, value)
	return q
}

// WhereGreaterThanOrEqual adds: ^field>=value
func (q *QueryBuilder) WhereGreaterThanOrEqual(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snGTE, value)
	return q
}

// WhereLessThan adds: ^field<value
func (q *QueryBuilder) WhereLessThan(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snLT, value)
	return q
}

// WhereLessThanOrEqual adds: ^field<=value
func (q *QueryBuilder) WhereLessThanOrEqual(field, value string) *QueryBuilder {
	value = sanitizeValue(value)
	fmt.Fprintf(&q.query, "%s%s%s%s", snAND, field, snLTE, value)
	return q
}

// WhereIsAnything adds: ^fieldANYTHING
func (q *QueryBuilder) WhereIsAnything(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, field, snISANYTHING)
	return q
}

// WhereIsEmptyString adds: ^fieldEMPTYSTRING
func (q *QueryBuilder) WhereIsEmptyString(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, field, snISEMPTYSTR)
	return q
}

// WhereIsEmpty adds: ^fieldISEMPTY
func (q *QueryBuilder) WhereIsEmpty(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, field, snISEMPTY)
	return q
}

// WhereIsNotEmpty adds: ^fieldISNOTEMPTY
func (q *QueryBuilder) WhereIsNotEmpty(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, field, snISNOTEMPTY)
	return q
}

// OrderByAsc adds: ^ORDERBYfield
func (q *QueryBuilder) OrderByAsc(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, snORDERBY, field)
	return q
}

// OrderByDesc adds: ^ORDERBYDESCfield
func (q *QueryBuilder) OrderByDesc(field string) *QueryBuilder {
	field = sanitizeValue(field)
	fmt.Fprintf(&q.query, "%s%s%s", snAND, snORDERBYDESC, field)
	return q
}

// WhereTimestampEquals adds: ^field=javascript:gs.dateGenerate('date','time')
func (q *QueryBuilder) WhereTimestampEquals(field string, t time.Time) *QueryBuilder {
	q.WhereEquals(field, ToServiceNowDateTime(t))
	return q
}

// WhereBetweenInclusive adds: ^field>=from ^field<=through (both inclusive).
func (q *QueryBuilder) WhereBetweenInclusive(field string, from, through time.Time) *QueryBuilder {
	q.WhereGreaterThanOrEqual(field, ToServiceNowDateTime(from))
	q.WhereLessThanOrEqual(field, ToServiceNowDateTime(through))
	return q
}

// WhereBetweenExclusive adds: ^field>from ^field<through (both exclusive).
func (q *QueryBuilder) WhereBetweenExclusive(field string, from, through time.Time) *QueryBuilder {
	q.WhereGreaterThan(field, ToServiceNowDateTime(from))
	q.WhereLessThan(field, ToServiceNowDateTime(through))
	return q
}

// Union appends another query via the ^NQ (new query / union) operator.
func (q *QueryBuilder) Union(other *QueryBuilder) *QueryBuilder {
	q.query.WriteString(snNQ)
	q.query.WriteString(other.Build())
	return q
}

// ToServiceNowDateTime formats a time.Time into the ServiceNow encoded query
// datetime format: javascript:gs.dateGenerate('2024-01-15','14:30:00')
func ToServiceNowDateTime(t time.Time) string {
	t = t.UTC()
	date := t.Format("2006-01-02")
	tod := t.Format("15:04:05")
	return fmt.Sprintf("javascript:gs.dateGenerate('%s','%s')", date, tod)
}
