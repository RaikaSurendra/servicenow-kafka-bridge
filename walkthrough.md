# Walkthrough: ServiceNow-Kafka Bridge (Go)

The ServiceNow-Kafka Bridge has been successfully rewritten from Java to a standalone Go binary. This walkthrough covers the new project structure, key features, and how to operate the bridge.

## 1. Project Overview

The project follows a standard Go layout:

* `cmd/bridge/`: Main entry point and lifecycle management.
* `internal/config/`: Configuration parsing and validation.
* `internal/servicenow/`: ServiceNow API client, query builder, and auth.
* `internal/kafka/`: Kafka producer/consumer wrappers (franz-go).
* `internal/source/`: ServiceNow → Kafka polling logic.
* `internal/sink/`: Kafka → ServiceNow writing logic.

---

## 2. Key Features

### 🚀 Proactive OAuth Refresh

Unlike the Java connector which refreshes tokens only on 401 errors, the Go bridge uses a background goroutine to refresh tokens *before* they expire. This eliminates latency spikes for API calls.

### 🛡️ Atomic Offset Persistence

Offsets are stored in JSON format with an atomic write pattern (Write-to-Temp → Rename). This ensures that even if the system crashes mid-write, the previous valid offset is preserved.

### 📊 Built-in Observability

A dedicated server provides:

* `/healthz`: Liveness probe.
* `/readyz`: Readiness probe.
* `/metrics`: Prometheus exposition format for 10+ custom bridge metrics.

---

## 3. How to Verify

### 3.1 Automated Tests

All packages include comprehensive unit and integration tests. Run them with:

```bash
go test -race -v ./...
```

**Results:**

* **Total Tests**: 50+
* **Coverage**: Core logic (Query Builder, Auth, Poller, Client) verified.
* **Race Detection**: Clean.

### 3.2 Running via Docker Compose

The included `docker-compose.yml` starts a KRaft-mode Kafka cluster and the bridge.

1. Update `config.yaml` with your ServiceNow instance details.
2. Run:

    ```bash
    docker compose up --build
    ```

3. Monitor metrics: `curl http://localhost:8080/metrics`

---

## 4. Phase 11: Resilience & Performance

The bridge has been further enhanced with production-grade features:

### ⚡ Parallel Sink Writing

The sink pipeline now uses a concurrent worker pool (default: 5 goroutines per topic), allowing multiple ServiceNow API calls to run in parallel. This significantly increases throughput for high-volume Kafka clusters.

### 💀 Dead Letter Queue (DLQ)

Failed ServiceNow writes are no longer just logged. If configured, they are wrapped with error metadata and routed to a specialized DLQ Kafka topic for later inspection and replay.

### 📦 Schema Registry & Avro

The bridge now supports Confluent Wire Format Avro serialization. It can automatically generate and register schemas for ServiceNow tables, ensuring data governance and compatibility with downstream analytics systems.

### 🔄 Hot-Reload Configuration

Watching the `config.yaml` file via `fsnotify`, the bridge can now perform soft restarts when the configuration changes (e.g., updating credentials or table mappings) without dropping the process.
