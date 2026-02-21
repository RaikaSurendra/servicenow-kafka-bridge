// Package source implements the ServiceNow → Kafka source pipeline.
//
// # Overview
//
// The source pipeline periodically polls ServiceNow tables for new or updated
// records and publishes them to Kafka topics. Each configured table gets its
// own [Poller] goroutine that runs independently.
//
// # Data Flow
//
//	┌──────────────┐     ┌───────────────────┐     ┌──────────────┐
//	│  ServiceNow  │────▶│  Source Poller     │────▶│    Kafka     │
//	│  Table API   │     │  (per table)       │     │  Producer    │
//	│              │     │                    │     │              │
//	│  GET /table  │     │  1. Build query    │     │  Topic per   │
//	│  ?sysparm_   │     │  2. Fetch records  │     │  table       │
//	│   query=...  │     │  3. Produce to     │     │              │
//	│              │     │     Kafka (sync)   │     │  acks=all    │
//	│              │     │  4. Update offset  │     │              │
//	└──────────────┘     └───────────────────┘     └──────────────┘
//
// # Query Logic (Port from TableAPISubTask.java)
//
// The poller constructs two types of queries depending on whether an offset exists:
//
// ## Unbounded Query (No Offset — First Poll)
//
// When no offset exists for a table (first-ever poll or intentional reset):
//
//	sys_idISNOTEMPTY^ORDERBYsys_updated_on^ORDERBYsys_id
//
// If initial_query_hours_ago is configured, a timestamp floor is added.
//
// ## Bounded Query (Offset Exists — Subsequent Polls)
//
// Two clauses unioned with ^NQ:
//
//	  Clause 1 — Records at the exact last-seen timestamp but with a
//	  larger identifier (catches records we haven't seen yet at this timestamp):
//
//		sys_updated_on=javascript:gs.dateGenerate('...')^sys_id>lastID^sys_idISNOTEMPTY
//
//	  Clause 2 — Records in the open interval (lastTimestamp, throughTimestamp):
//
//		sys_updated_on>javascript:gs.dateGenerate('from')^sys_updated_on<javascript:gs.dateGenerate('through')
//		^sys_idISNOTEMPTY^ORDERBYsys_updated_on^ORDERBYsys_id
//
//	  Final: Clause1^NQClause2
//
// This two-clause design ensures no records are missed when multiple records
// share the same sys_updated_on timestamp (which is common in bulk updates).
//
// # Polling Intervals
//
// The poller uses two configurable intervals:
//
//   - FastPollInterval (default 500ms): Used after a poll that returned records.
//     If there are more records to fetch, poll again quickly.
//
//   - SlowPollInterval (default 30s): Used after a poll that returned no
//     records. Back off to avoid hammering the ServiceNow API.
//
// # Offset Safety
//
// The offset is advanced **only after** the Kafka producer confirms receipt
// (synchronous produce with acks=all). This guarantees at-least-once delivery:
//
//  1. Fetch records from ServiceNow
//  2. Produce each record to Kafka (sync, acks=all)
//  3. Only then: update the in-memory offset
//  4. Periodically flush offsets to the Store
//
// If the process crashes between steps 2 and 3, the records will be re-fetched
// and re-produced on restart (at-most duplicated, never lost).
package source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/your-org/servicenow-kafka-bridge/internal/config"
	"github.com/your-org/servicenow-kafka-bridge/internal/kafka"
	"github.com/your-org/servicenow-kafka-bridge/internal/offset"
	"github.com/your-org/servicenow-kafka-bridge/internal/partition"
	"github.com/your-org/servicenow-kafka-bridge/internal/servicenow"
)

// Poller continuously polls a single ServiceNow table and publishes
// new/updated records to a Kafka topic. Each table gets its own Poller.
//
// The Poller implements the exact same query and batch processing logic
// as TableAPISubTask.java from the Java reference, with improvements:
//
//   - Configurable timestamp delay to handle clock skew.
//   - Context-based cancellation for graceful shutdown.
//   - Metrics integration (planned).
type Poller struct {
	// Table configuration
	table           config.TableConfig
	timestampField  string
	identifierField string
	batchSize       int

	// Base config
	fastPollInterval     time.Duration
	slowPollInterval     time.Duration
	initialQueryHoursAgo int
	timestampDelaySecs   int

	// Dependencies
	snClient    servicenow.Client
	producer    *kafka.Producer
	store       offset.Store
	partitioner partition.Partitioner
	logger      *slog.Logger

	// Runtime state
	currentOffset offset.Offset
}

// NewPoller creates a Poller for the given table configuration.
func NewPoller(
	table config.TableConfig,
	srcCfg config.SourceConfig,
	snClient servicenow.Client,
	producer *kafka.Producer,
	store offset.Store,
	logger *slog.Logger,
) (*Poller, error) {
	// Load existing offset for this table.
	off, err := store.Get(table.Name)
	if err != nil {
		return nil, fmt.Errorf("loading offset for table %s: %w", table.Name, err)
	}

	p := &Poller{
		table:                table,
		timestampField:       table.TimestampField,
		identifierField:      table.IdentifierField,
		batchSize:            srcCfg.BatchSize,
		fastPollInterval:     srcCfg.FastPollInterval.Duration,
		slowPollInterval:     srcCfg.SlowPollInterval.Duration,
		initialQueryHoursAgo: srcCfg.InitialQueryHoursAgo,
		timestampDelaySecs:   srcCfg.TimestampDelaySecs,
		snClient:             snClient,
		producer:             producer,
		store:                store,
		partitioner:          partition.New(table),
		logger: logger.With(
			"component", "source-poller",
			"table", table.Name,
			"topic", table.Topic,
		),
		currentOffset: off,
	}

	return p, nil
}

// Run starts the polling loop. It blocks until the context is cancelled.
// This method should be run in its own goroutine (typically via errgroup).
//
// Each iteration of the loop:
//  1. Builds a query based on the current offset.
//  2. Fetches records from ServiceNow.
//  3. For each record: serializes to JSON, produces to Kafka (sync).
//  4. Updates the in-memory offset after successful produce.
//  5. Waits for the next poll interval (fast if records found, slow if not).
func (p *Poller) Run(ctx context.Context) error {
	p.logger.Info("starting source poller",
		"offset", p.currentOffset,
		"batch_size", p.batchSize,
	)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("source poller shutting down")
			return ctx.Err()
		default:
		}

		recordsFound, err := p.pollOnce(ctx)
		if err != nil {
			p.logger.Error("poll cycle failed", "error", err)
			// Don't exit — wait and retry. The error may be transient.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.slowPollInterval):
			}
			continue
		}

		// Choose the next poll interval based on whether we found records.
		// This is the same logic as TableAPISubTask.java:
		//   - Records found → poll quickly (there may be more).
		//   - No records → back off (avoid unnecessary API calls).
		interval := p.slowPollInterval
		if recordsFound {
			interval = p.fastPollInterval
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// pollOnce executes a single poll cycle. Returns true if records were found.
//
// This method encapsulates the core logic from TableAPISubTask.poll():
//  1. Build the query (bounded or unbounded based on offset state).
//  2. Call GetRecords on the ServiceNow client.
//  3. Process each record: serialize, produce, update offset.
func (p *Poller) pollOnce(ctx context.Context) (bool, error) {
	query := p.buildQuery()

	records, err := p.snClient.GetRecords(
		ctx,
		p.table.Name,
		query,
		0,
		p.batchSize,
		p.table.Fields,
	)
	if err != nil {
		return false, fmt.Errorf("fetching records: %w", err)
	}

	if len(records) == 0 {
		return false, nil
	}

	p.logger.Info("fetched records", "count", len(records))

	for _, record := range records {
		if err := p.processRecord(ctx, record); err != nil {
			return false, fmt.Errorf("processing record: %w", err)
		}
	}

	// Persist the offset after all records in this batch have been produced.
	if err := p.store.Set(p.table.Name, p.currentOffset); err != nil {
		return false, fmt.Errorf("storing offset: %w", err)
	}

	return true, nil
}

// processRecord serializes a record to JSON, produces it to Kafka, and
// updates the in-memory offset. The offset is updated **only after** the
// synchronous Kafka produce confirms receipt.
func (p *Poller) processRecord(ctx context.Context, record servicenow.Record) error {
	// Serialize the record to JSON for the Kafka message value.
	value, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling record: %w", err)
	}

	// Compute the partition key using the configured partitioner.
	key := p.partitioner.Key(record)

	// Add metadata headers for consumers.
	headers := map[string]string{
		"sn_table": p.table.Name,
	}

	// Synchronous produce — blocks until broker acknowledges.
	// This is the critical at-least-once guarantee: we do NOT advance the
	// offset until Kafka confirms the message was written.
	if err := p.producer.ProduceSync(ctx, p.table.Topic, key, value, headers); err != nil {
		return fmt.Errorf("producing record: %w", err)
	}

	// Extract the timestamp and identifier from the record for offset tracking.
	p.updateOffset(record)
	return nil
}

// updateOffset extracts the timestamp and identifier from the processed record
// and updates the in-memory offset.
//
// The timestamp is parsed from the ServiceNow datetime format (YYYY-MM-DD HH:MM:SS)
// and stored as Unix epoch seconds, matching the Java TimestampSourceOffset.
func (p *Poller) updateOffset(record servicenow.Record) {
	// Extract the identifier (typically sys_id).
	if idVal, ok := record[p.identifierField]; ok {
		p.currentOffset.LastIdentifier = fmt.Sprintf("%v", idVal)
	}

	// Extract the timestamp field and parse it.
	if tsVal, ok := record[p.timestampField]; ok {
		tsStr := fmt.Sprintf("%v", tsVal)
		// ServiceNow returns timestamps in "YYYY-MM-DD HH:MM:SS" format.
		if t, err := time.Parse("2006-01-02 15:04:05", tsStr); err == nil {
			p.currentOffset.Timestamp = t.Unix()
		} else {
			p.logger.Warn("failed to parse timestamp",
				"field", p.timestampField,
				"value", tsStr,
				"error", err,
			)
		}
	}
}

// buildQuery constructs the ServiceNow query based on the current offset state.
//
// This is a faithful port of TableAPISubTask.buildQuery() from the Java reference.
// See the package documentation for detailed query structure.
func (p *Poller) buildQuery() *servicenow.QueryBuilder {
	fromTime := p.getFromDateTimeUTC()
	throughTime := p.getThroughDateTimeUTC()

	if fromTime == nil {
		return p.buildUnboundedQuery(throughTime)
	}
	return p.buildBoundedQuery(*fromTime, throughTime)
}

// buildUnboundedQuery creates a query with no lower time bound.
// Used for the first poll when no offset exists.
//
// Port of TableAPISubTask.buildQueryUnboundedQuery().
func (p *Poller) buildUnboundedQuery(through time.Time) *servicenow.QueryBuilder {
	q := servicenow.NewQueryBuilder().
		WhereIsNotEmpty(p.identifierField)

	// If through time is specified, add an upper bound.
	if !through.IsZero() {
		q.WhereLessThan(p.timestampField, servicenow.ToServiceNowDateTime(through))
	}

	q.OrderByAsc(p.timestampField).
		OrderByAsc(p.identifierField)

	return q
}

// buildBoundedQuery creates the two-clause bounded query.
//
// This is a faithful port of TableAPISubTask.buildQueryBoundedQuery().
// It uses two clauses joined by ^NQ (union):
//
// Clause 1: Records at exact fromTime with sys_id > lastIdentifier
// Clause 2: Records between (fromTime, throughTime) exclusive
func (p *Poller) buildBoundedQuery(from, through time.Time) *servicenow.QueryBuilder {
	// Clause 1: Exact timestamp match with identifier filter.
	// This catches records we haven't processed yet at the last-seen timestamp.
	clause1 := servicenow.NewQueryBuilder().
		WhereTimestampEquals(p.timestampField, from)

	// Only add the identifier filter if we have a last-seen identifier.
	if p.currentOffset.LastIdentifier != "" {
		clause1.WhereGreaterThan(p.identifierField, p.currentOffset.LastIdentifier)
	}

	// Filter out records with no identifier (safety check from Java reference).
	clause1.WhereIsNotEmpty(p.identifierField)

	// Clause 2: Open interval for newer records.
	clause2 := servicenow.NewQueryBuilder().
		WhereBetweenExclusive(p.timestampField, from, through).
		WhereIsNotEmpty(p.identifierField).
		OrderByAsc(p.timestampField).
		OrderByAsc(p.identifierField)

	// Union the two clauses.
	clause1.Union(clause2)

	return clause1
}

// getFromDateTimeUTC determines the lower time bound for the query.
//
// Port of TableAPISubTask.getFromDateTimeUtc():
//   - If we have an offset: use its timestamp.
//   - If no offset and initial_query_hours_ago is set: use now - hours.
//   - If no offset and initial_query_hours_ago is -1: return nil (unbounded).
func (p *Poller) getFromDateTimeUTC() *time.Time {
	if !p.currentOffset.IsZero() {
		t := p.currentOffset.Time()
		return &t
	}

	if p.initialQueryHoursAgo < 0 {
		return nil // unbounded
	}

	t := time.Now().UTC().Add(-time.Duration(p.initialQueryHoursAgo) * time.Hour)
	return &t
}

// getThroughDateTimeUTC determines the upper time bound for the query.
// It uses the current time minus a configurable delay to handle clock skew
// between the bridge and ServiceNow.
//
// The delay ensures we don't miss records that are being created "right now"
// but haven't been fully indexed by ServiceNow yet.
func (p *Poller) getThroughDateTimeUTC() time.Time {
	return time.Now().UTC().Add(-time.Duration(p.timestampDelaySecs) * time.Second)
}
