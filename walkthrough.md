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

## 4. GitHub Repository

The project has been pushed to:
[https://github.com/RaikaSurendra/servicenow-kafka-bridge.git](https://github.com/RaikaSurendra/servicenow-kafka-bridge.git)

The `.local/` directory is ignored via `.gitignore` as requested.
