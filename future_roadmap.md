# Future Roadmap: ServiceNow-Kafka Bridge

## 1. Project Review & Current State

The project has successfully achieved its primary objective: a high-performance Go-based rewrite of the legacy Java connector.

### Strengths

- **Proactive Resilience**: The proactive OAuth refresh and exponential backoff strategies make it significantly more stable than the reactive Java implementation.
- **Atomic Correctness**: The dual-clause query logic and atomic offset flushing ensure data integrity even under high load or sudden crashes.
- **Observability**: First-class Prometheus support provides immediate visibility into synchronization lag and API performance.

---

## 2. Short-Term Enhancements (Phase 11)

### 2.1 Schema Registry Integration

- **Problem**: Currently, records are sent as raw JSON.
- **Solution**: Support Avro or Protobuf serialization using a Schema Registry (e.g., Confluent or Apicurio). This enables downstream consumers to rely on type-safe contracts.

### 2.2 Dead Letter Queue (DLQ) for Sink

- **Problem**: Persistent failures in the sink (e.g., a record violates a ServiceNow business rule) currently block the batch or get logged and dropped.
- **Solution**: Route failed records to a dedicated Kafka topic (`{table}.dlq`) with headers containing the error reason.

### 2.3 Parallel Sink Writing

- **Problem**: The sink processes Kafka batches sequentially.
- **Solution**: Use a worker pool within the sink to perform ServiceNow `POST`/`PATCH` operations in parallel for a single Kafka batch, drastically increasing sink throughput.

---

## 3. Mid-Term Architectural Improvements (Phase 12)

### 3.1 Dynamic Configuration

- **Problem**: Adding/removing a table requires a process restart.
- **Solution**: Implement a "Hot Reload" feature that watches the `config.yaml` or a remote config store (like etcd or Consul) and starts/stops pollers dynamically.

### 3.2 Granular Multi-Tenancy

- **Problem**: A single bridge process connects to one ServiceNow instance.
- **Solution**: Update the architecture to support multiple ServiceNow instances within a single bridge binary, allowing for complex multi-instance synchronization hub patterns.

---

## 4. Long-Term Vision (The Future)

### 4.1 Kubernetes Operator

Transform the bridge from a standalone binary into a **ServiceNow-Kafka Operator**.

- **Custom Resources (CRDs)**: Define `ServiceNowTable` objects in Kubernetes.
- **Auto-Scaling**: Automatically scale pollers based on the `bridge_offset_lag_seconds` metric.

### 4.2 Stream Processing Integration

Deeply integrate with stream processing frameworks (like ksqlDB or Flink).

- **Change Data Capture (CDC)**: Explore the use of ServiceNow's "Echo" or webhook features to supplement polling with real-time push events for sub-second latency.

### 4.3 Advanced Conflict Resolution

Move beyond simple `POST`/`PATCH` logic to a pluggable conflict resolution engine (e.g., "newest wins," "weighted system of record," or "manual review queue").

---

## 5. Conclusion

The foundation is rock solid. The next logical step for this project is moving from **Data Movement** to **Data Governance**, focusing on schemas, DLQs, and cloud-native management.
