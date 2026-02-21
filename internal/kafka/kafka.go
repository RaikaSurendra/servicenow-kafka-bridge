// Package kafka provides a thin wrapper around the franz-go Kafka client library,
// adapting it to the ServiceNow-Kafka Bridge's producer and consumer needs.
//
// # Architecture
//
// The bridge uses Kafka in two directions:
//
//   - Source pipeline (ServiceNow → Kafka): Uses the [Producer] to publish records
//     fetched from ServiceNow tables to Kafka topics. The producer is synchronous
//     by default — calling ProduceSync blocks until the broker acknowledges the
//     message. This guarantees that offsets are only advanced after Kafka confirms
//     receipt (at-least-once delivery).
//
//   - Sink pipeline (Kafka → ServiceNow): Uses the [Consumer] to read messages
//     from Kafka topics and write them to ServiceNow tables via the Table API.
//
// # franz-go Client
//
// We use github.com/twmb/franz-go as the Kafka client for several reasons:
//
//   - Pure Go. No CGo dependency on librdkafka.
//   - Modern API with context-aware methods.
//   - Natively supports idempotent producing, exactly-once transactions,
//     and consumer group management.
//   - Active maintenance and excellent documentation.
//
// # Thread Safety
//
// Both Producer and Consumer are safe for concurrent use. The underlying
// franz-go client handles connection pooling and request serialization
// internally.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/your-org/servicenow-kafka-bridge/internal/config"
)

// Producer wraps a franz-go client for producing messages to Kafka.
//
// The producer is configured with acks=all (RequiredAcks: -1) by default
// to ensure messages are replicated before being acknowledged. This trades
// throughput for durability, which is appropriate for a data bridge where
// data loss is unacceptable.
type Producer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewProducer creates a Kafka producer from the bridge configuration.
// The producer is ready to use immediately after construction.
func NewProducer(cfg config.KafkaConfig, logger *slog.Logger) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // -1: wait for all in-sync replicas
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(5),
		kgo.RetryTimeout(30 * time.Second),
		kgo.ProducerBatchMaxBytes(1 << 20), // 1 MiB
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka producer client: %w", err)
	}

	return &Producer{
		client: client,
		logger: logger.With("component", "kafka-producer"),
	}, nil
}

// ProduceSync sends a single record to the specified Kafka topic and waits
// for broker acknowledgement. This is the core method used by the source
// poller to guarantee at-least-once delivery.
//
// The method blocks until:
//   - The broker acknowledges the message (success).
//   - The context is cancelled.
//   - An unrecoverable error occurs.
//
// Parameters:
//   - topic: The Kafka topic to produce to.
//   - key: The message key (used for partitioning). Can be nil for round-robin.
//   - value: The message payload (typically JSON-serialized ServiceNow record).
//   - headers: Optional Kafka headers (e.g., table name, timestamp).
func (p *Producer) ProduceSync(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// Convert map headers to kgo headers.
	for k, v := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	// ProduceSync blocks until the broker acknowledges or error.
	results := p.client.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("producing to %s: %w", topic, err)
	}

	p.logger.Debug("message produced",
		"topic", topic,
		"partition", results[0].Record.Partition,
		"offset", results[0].Record.Offset,
	)
	return nil
}

// ProduceBatchSync sends multiple records to the specified Kafka topic and
// waits for broker acknowledgement of all messages. This is more efficient
// than calling ProduceSync in a loop when publishing a batch of records.
//
// All messages in the batch are sent to the same topic. If any message fails,
// the error is returned and the caller should NOT advance the offset.
func (p *Producer) ProduceBatchSync(ctx context.Context, topic string, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	records := make([]*kgo.Record, len(messages))
	for i, msg := range messages {
		rec := &kgo.Record{
			Topic: topic,
			Key:   msg.Key,
			Value: msg.Value,
		}
		for k, v := range msg.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}
		records[i] = rec
	}

	results := p.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("batch produce to %s: %w", topic, err)
	}

	p.logger.Debug("batch produced",
		"topic", topic,
		"count", len(messages),
	)
	return nil
}

// Close flushes any pending messages and closes the Kafka connection.
func (p *Producer) Close() {
	p.client.Close()
}

// Message represents a single message to be produced to Kafka.
type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// ----- Consumer -----

// Consumer wraps a franz-go client for consuming messages from Kafka topics.
//
// The consumer uses Kafka consumer groups for automatic partition assignment
// and offset management. Each bridge instance joins the group and is assigned
// a subset of partitions to consume from.
type Consumer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewConsumer creates a Kafka consumer for the specified topics and group.
func NewConsumer(cfg config.KafkaConfig, groupID string, topics []string, logger *slog.Logger) (*Consumer, error) {
	if groupID == "" {
		return nil, fmt.Errorf("consumer group ID is required")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka consumer client: %w", err)
	}

	return &Consumer{
		client: client,
		logger: logger.With("component", "kafka-consumer", "group", groupID, "topics", strings.Join(topics, ",")),
	}, nil
}

// Poll fetches the next batch of records from Kafka. Blocks until records
// are available or the context is cancelled. Returns nil, nil if context
// is cancelled.
func (c *Consumer) Poll(ctx context.Context) ([]*kgo.Record, error) {
	fetches := c.client.PollRecords(ctx, 100)
	if errs := fetches.Errors(); len(errs) > 0 {
		// Collect all partition-level errors.
		var errMsgs []string
		for _, e := range errs {
			errMsgs = append(errMsgs, fmt.Sprintf("%s[%d]: %v", e.Topic, e.Partition, e.Err))
		}
		return nil, fmt.Errorf("consumer poll errors: %s", strings.Join(errMsgs, "; "))
	}

	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})

	if len(records) > 0 {
		c.logger.Debug("polled records", "count", len(records))
	}
	return records, nil
}

// CommitOffsets commits the offsets for all consumed records. Should be called
// after the records have been successfully processed (written to ServiceNow).
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}
	return nil
}

// Close leaves the consumer group and releases resources.
func (c *Consumer) Close() {
	c.client.Close()
}
