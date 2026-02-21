# ServiceNow-Kafka Bridge

[![CI](https://github.com/RaikaSurendra/servicenow-kafka-bridge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/RaikaSurendra/servicenow-kafka-bridge/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/RaikaSurendra/servicenow-kafka-bridge)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/RaikaSurendra/servicenow-kafka-bridge)](https://goreportcard.com/report/github.com/RaikaSurendra/servicenow-kafka-bridge)

A high-performance, resilient Go binary for bidirectional data synchronization between ServiceNow and Kafka. This project serves as a modern replacement for the legacy IBM Kafka Connect ServiceNow connector.

## Architecture

```mermaid
flowchart LR
    subgraph ServiceNow
        SN_API[Table API]
    end

    subgraph Bridge["ServiceNow-Kafka Bridge"]
        direction TB
        SRC[Source Poller] -->|Produce| KP[Kafka Producer]
        KC[Kafka Consumer] -->|Consume| SINK[Sink Worker]
        OBS[Prometheus /metrics]
        HP[Health /healthz /readyz]
    end

    subgraph Kafka
        TOPIC_SRC[Source Topics]
        TOPIC_SINK[Sink Topics]
        DLQ[DLQ Topic]
    end

    SN_API -- poll records --> SRC
    KP --> TOPIC_SRC
    TOPIC_SINK --> KC
    SINK -- insert/update --> SN_API
    SINK -.-> DLQ
```

## 🚀 Features

- **Bidirectional Pipelines**: Supports both Source (ServiceNow → Kafka) and Sink (Kafka → ServiceNow).
- **Proactive OAuth Refresh**: Background token refresh logic eliminates 401 delays.
- **At-Least-Once Delivery**: Offset updates are strictly synchronized with Kafka acknowledgments.
- **Atomic Offsets**: File-based offset storage with transactional write patterns for crash resilience.
- **Observability**: Built-in Prometheus metrics (`/metrics`) and Kubernetes-friendly probes (`/healthz`, `/readyz`).
- **Flexible Partitioning**: Default (sys_id), Round-Robin, or Hash-based strategies.

## 📋 Prerequisites

- **Go**: 1.24+ (if running natively)
- **Docker**: For containerized deployment (recommended)
- **ServiceNow**: Instance access (Base URL + OAuth/Basic credentials)
- **Kafka**: 3.x+ (KRaft mode supported)

## 🛠️ Quick Start

### 1. Configure the Bridge

Edit the provided `config.yaml` and update your credentials (OAuth/Basic, Kafka brokers, tables/topics).

### 2. Run with Docker Compose

The easiest way to start the bridge along with a Kafka broker:

```bash
docker compose up --build
```

### 3. Verify Operation

- **Health Check**: `curl http://localhost:8080/healthz`
- **Metrics**: `curl http://localhost:8080/metrics`

## 📚 Documentation

- [Getting Started](docs/getting-started.md): Step-by-step setup, configuration, and first run.
- [Architecture](docs/architecture.md): How the bridge works internally — components, data flow, error handling, and metrics.
- [Hybrid Architecture](docs/hybrid-architecture.md): Combine with ServiceNow IntegrationHub Kafka Spoke for real-time + batch.
- [Roadmap](docs/roadmap.md): What's been built and what's coming next.

## 🧪 Testing

Run the exhaustive test suite with race detection:

```bash
go test -race -v ./...
```

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## ⚖️ License

This project is licensed under the [Apache License 2.0](LICENSE).
