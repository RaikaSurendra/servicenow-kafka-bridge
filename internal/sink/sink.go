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
	"time"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
	kafkapkg "github.com/RaikaSurendra/servicenow-kafka-bridge/internal/kafka"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/servicenow"
)

// Worker consumes messages from a Kafka topic and writes them to a
// ServiceNow table. One Worker is created per configured sink topic.
type Worker struct {
	topicConfig config.SinkTopicConfig
	consumer    *kafkapkg.Consumer
	snClient    servicenow.Client
	logger      *slog.Logger
}

// NewWorker creates a sink Worker for the given topic-table mapping.
func NewWorker(
	topicCfg config.SinkTopicConfig,
	consumer *kafkapkg.Consumer,
	snClient servicenow.Client,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		topicConfig: topicCfg,
		consumer:    consumer,
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
//  2. For each record: deserialize, write to ServiceNow.
//  3. Commit Kafka offsets after all records in the batch are processed.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting sink worker",
		"topic", w.topicConfig.Topic,
		"table", w.topicConfig.Table,
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

		// Process each record — write to ServiceNow.
		successCount := 0
		failCount := 0
		for _, rec := range records {
			if err := w.processRecord(ctx, rec.Value); err != nil {
				w.logger.Error("failed to write record to ServiceNow",
					"error", err,
					"partition", rec.Partition,
					"offset", rec.Offset,
				)
				failCount++
				// Continue processing remaining records — don't block the batch.
				continue
			}
			successCount++
		}

		w.logger.Info("batch processed",
			"success", successCount,
			"failed", failCount,
			"total", len(records),
		)

		// Commit offsets after processing the batch.
		// Even if some records failed, we commit to avoid re-processing
		// records that already succeeded. Failed records are logged.
		if err := w.consumer.CommitOffsets(ctx); err != nil {
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
	} else {
		// Insert new record.
		w.logger.Debug("inserting new record", "table", w.topicConfig.Table)
		_, err := w.snClient.InsertRecord(ctx, w.topicConfig.Table, record)
		if err != nil {
			return fmt.Errorf("inserting record: %w", err)
		}
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
