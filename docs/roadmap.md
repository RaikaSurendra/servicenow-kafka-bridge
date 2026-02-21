# Roadmap

What's been built, what's coming next, and where this project is heading.

## Current State

The bridge is production-ready with these capabilities:

- **Bidirectional sync** between ServiceNow and Kafka
- **At-least-once delivery** with atomic offset persistence
- **Proactive OAuth refresh** — tokens are refreshed before they expire
- **Parallel sink processing** with configurable concurrency
- **Dead Letter Queue** for failed sink writes
- **Avro serialization** with Confluent Schema Registry
- **Hot-reload** — config changes apply without restarting the process
- **Prometheus metrics** for all components
- **TLS/SASL** support for secure Kafka connections
- **Helm chart** for Kubernetes deployments

---

## Up Next

### Multi-Tenancy

- **Problem**: One bridge process connects to one ServiceNow instance.
- **Solution**: Support multiple ServiceNow instances in a single binary, acting as a centralized sync hub.

### Webhook-Based CDC

- **Problem**: Polling introduces inherent latency (seconds to minutes).
- **Solution**: Support ServiceNow Business Rules or webhooks to receive real-time push notifications, supplementing polling for sub-second latency.

### Grafana Dashboard

- **Problem**: Prometheus metrics are available but require manual query setup.
- **Solution**: Ship a pre-built Grafana dashboard JSON that visualizes sync lag, throughput, error rates, and API latency out of the box.

---

## Future Vision

### Kubernetes Operator

Transform the bridge into a full Kubernetes Operator:

- **Custom Resources (CRDs)**: Define `ServiceNowTable` resources in YAML, and the operator manages pollers automatically.
- **Auto-Scaling**: Scale pollers up or down based on the `bridge_offset_lag_seconds` metric using HPA.

### Conflict Resolution Engine

Move beyond simple insert/update logic to a pluggable conflict resolution system:

- **Newest wins** — last write takes precedence
- **System of record** — configurable priority between ServiceNow and Kafka
- **Manual review queue** — flag conflicts for human review

### Stream Processing Integration

Deep integration with ksqlDB, Flink, or similar frameworks for real-time transformations between ServiceNow and Kafka.

---

## Contributing

Have an idea or feature request? Open an issue on [GitHub](https://github.com/RaikaSurendra/servicenow-kafka-bridge/issues) or see [CONTRIBUTING.md](../CONTRIBUTING.md).
