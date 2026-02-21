// Package sink implements the Kafka → ServiceNow sink pipeline.
//
// # Overview
//
// The sink pipeline consumes messages from Kafka topics and writes them to
// ServiceNow tables via the Table API. Each Kafka message is expected to
// contain a JSON-serialized ServiceNow record.
//
// # Data Flow
//
//	┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
//	│    Kafka     │────▶│   Sink Worker     │────▶│  ServiceNow  │
//	│   Consumer   │     │   (per topic)     │     │  Table API   │
//	│              │     │                   │     │              │
//	│  Poll batch  │     │  1. Deserialize   │     │  POST (new)  │
//	│  of records  │     │  2. Determine     │     │  PATCH (upd) │
//	│              │     │     insert/update  │     │              │
//	│              │     │  3. Call API       │     │              │
//	│              │     │  4. Commit offset  │     │              │
//	└──────────────┘     └──────────────────┘     └──────────────┘
//
// # Insert vs. Update Decision
//
// The sink uses a simple heuristic to decide whether to insert or update:
//
//   - If the record contains a "sys_id" field with a non-empty value,
//     it is treated as an update (PATCH to /api/now/table/{table}/{sys_id}).
//
//   - If the record does not contain "sys_id" or it is empty,
//     it is treated as a new insert (POST to /api/now/table/{table}).
//
// # Offset Management
//
// Kafka consumer offsets are committed only after records have been successfully
// written to ServiceNow. This provides at-least-once delivery semantics:
//
//   - If the sink crashes after writing to ServiceNow but before committing
//     the offset, the record will be re-consumed and re-written on restart.
//   - ServiceNow's sys_id-based deduplication handles this gracefully for
//     updates (PATCH is idempotent).
//
// # Error Handling
//
// If a write to ServiceNow fails:
//
//   - 4xx errors (except 401/429): The record is logged and skipped to avoid
//     blocking the pipeline. The error is reported via metrics.
//   - 401/429/5xx: Handled by the HTTP client's retry logic (see servicenow.Client).
//   - If the HTTP client exhausts retries, the record is logged and skipped.
//
// # Thread Safety
//
// The Worker is designed to run in a single goroutine per topic. Multiple
// Workers can run concurrently for different topics. The shared ServiceNow
// client and Kafka consumer handle internal synchronization.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
	kafkapkg "github.com/RaikaSurendra/servicenow-kafka-bridge/internal/kafka"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/observability"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/servicenow"
)

// Worker consumes messages from a Kafka topic and writes them to a
// ServiceNow table. One Worker is created per configured sink topic.
type Worker struct {
	sinkConfig  config.SinkConfig
	topicConfig config.SinkTopicConfig
	consumer    *kafkapkg.Consumer
	producer    *kafkapkg.Producer // added for DLQ support
	snClient    servicenow.Client
	logger      *slog.Logger
}

// NewWorker creates a sink Worker for the given topic-table mapping.
func NewWorker(
	sinkCfg config.SinkConfig,
	topicCfg config.SinkTopicConfig,
	consumer *kafkapkg.Consumer,
	producer *kafkapkg.Producer, // added for DLQ support
	snClient servicenow.Client,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		sinkConfig:  sinkCfg,
		topicConfig: topicCfg,
		consumer:    consumer,
		producer:    producer,
		snClient:    snClient,
		logger: logger.With(
			"component", "sink-worker",
			"topic", topicCfg.Topic,
			"table", topicCfg.Table,
		),
	}
}

// Run starts the sink consumption loop. It blocks until the context is
// cancelled. This method should be run in its own goroutine.
//
// Each iteration:
//  1. Poll Kafka for a batch of records.
//  2. For each record: deserialize, write to ServiceNow (concurrently).
//  3. Commit Kafka offsets after all records in the batch are processed.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting sink worker",
		"topic", w.topicConfig.Topic,
		"table", w.topicConfig.Table,
		"concurrency", w.sinkConfig.Concurrency,
	)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("sink worker shutting down")
			return ctx.Err()
		default:
		}

		records, err := w.consumer.Poll(ctx)
		if err != nil {
			w.logger.Error("poll failed", "error", err)
			// Back off on poll failure.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
			continue
		}

		if len(records) == 0 {
			continue
		}

		w.logger.Debug("received records from Kafka", "count", len(records))

		// Process records concurrently using an errgroup with a limit.
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(w.sinkConfig.Concurrency)
		var hadFailure atomic.Bool

		for _, rec := range records {
			rec := rec // capture loop variable
			g.Go(func() error {
				if err := w.processRecord(gCtx, rec.Value); err != nil {
					hadFailure.Store(true)
					observability.Metrics.SinkErrorsTotal.WithLabelValues(w.topicConfig.Table, "write").Inc()
					w.logger.Error("failed to write record to ServiceNow",
						"error", err,
						"partition", rec.Partition,
						"offset", rec.Offset,
					)

					// If DLQ is enabled, move the record to the DLQ topic.
					if w.sinkConfig.DLQTopic != "" && w.producer != nil {
						if dlqErr := w.moveToDLQ(gCtx, rec.Value, err); dlqErr != nil {
							observability.Metrics.SinkErrorsTotal.WithLabelValues(w.topicConfig.Table, "dlq").Inc()
							w.logger.Error("failed to move record to DLQ", "error", dlqErr)
						} else {
							w.logger.Info("record moved to DLQ", "topic", w.sinkConfig.DLQTopic)
						}
					}

					// We return nil here because we want to continue processing other
					// records in the batch even if one fails (logging the error instead).
					return nil
				}
				return nil
			})
		}

		// Wait for all workers in the batch to complete.
		_ = g.Wait()

		w.logger.Debug("batch processed", "total", len(records))

		// Commit offsets after processing the batch.
		// Optionally skip commits when partial failures occurred.
		if hadFailure.Load() && !w.sinkConfig.CommitOnPartialFailureValue() {
			w.logger.Warn("skipping offset commit due to partial failures",
				"topic", w.topicConfig.Topic,
				"table", w.topicConfig.Table,
			)
			continue
		}
		if err := w.consumer.CommitOffsets(ctx); err != nil {
			observability.Metrics.SinkErrorsTotal.WithLabelValues(w.topicConfig.Table, "commit").Inc()
			w.logger.Error("failed to commit offsets", "error", err)
		}
	}
}

// processRecord deserializes a Kafka message and writes it to ServiceNow.
//
// The insert/update decision is based on the presence of a "sys_id" field:
//   - sys_id present and non-empty: PATCH (update)
//   - sys_id absent or empty: POST (insert)
func (w *Worker) processRecord(ctx context.Context, value []byte) error {
	var record servicenow.Record
	if err := json.Unmarshal(value, &record); err != nil {
		return fmt.Errorf("deserializing record: %w (raw: %.200s)", err, string(value))
	}

	// Determine insert vs update.
	sysID, hasSysID := extractSysID(record)

	if hasSysID {
		// Update existing record.
		w.logger.Debug("updating record", "sys_id", sysID, "table", w.topicConfig.Table)
		_, err := w.snClient.UpdateRecord(ctx, w.topicConfig.Table, sysID, record)
		if err != nil {
			return fmt.Errorf("updating record %s: %w", sysID, err)
		}
		observability.Metrics.SinkRecordsTotal.WithLabelValues(w.topicConfig.Table, "update").Inc()
	} else {
		// Insert new record.
		w.logger.Debug("inserting new record", "table", w.topicConfig.Table)
		_, err := w.snClient.InsertRecord(ctx, w.topicConfig.Table, record)
		if err != nil {
			return fmt.Errorf("inserting record: %w", err)
		}
		observability.Metrics.SinkRecordsTotal.WithLabelValues(w.topicConfig.Table, "insert").Inc()
	}

	return nil
}

// extractSysID extracts the sys_id from a record and returns it along with
// a boolean indicating whether a valid sys_id was found.
func extractSysID(record servicenow.Record) (string, bool) {
	val, ok := record["sys_id"]
	if !ok {
		return "", false
	}
	sysID, ok := val.(string)
	if !ok || sysID == "" {
		return "", false
	}
	return sysID, true
}

// moveToDLQ wraps a failed record with error metadata and sends it to the DLQ topic.
func (w *Worker) moveToDLQ(ctx context.Context, value []byte, err error) error {
	// Wrap the original value with error info.
	dlqMsg := map[string]interface{}{
		"original_payload": string(value),
		"error":            err.Error(),
		"timestamp":        time.Now().Format(time.RFC3339),
		"topic":            w.topicConfig.Topic,
		"table":            w.topicConfig.Table,
	}

	dlqJSON, jerr := json.Marshal(dlqMsg)
	if jerr != nil {
		return fmt.Errorf("marshaling DLQ message: %w", jerr)
	}

	// Produce to DLQ topic synchronously.
	return w.producer.ProduceSync(ctx, w.sinkConfig.DLQTopic, nil, dlqJSON, nil)
}
