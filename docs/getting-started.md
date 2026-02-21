# Getting Started

This guide walks you through setting up and running the ServiceNow-Kafka Bridge from scratch. No prior experience with this project is required.

## What Does This Bridge Do?

The bridge moves data between **ServiceNow** and **Apache Kafka** in both directions:

- **Source** (ServiceNow → Kafka): Polls ServiceNow tables for new or updated records and publishes them to Kafka topics.
- **Sink** (Kafka → ServiceNow): Consumes messages from Kafka topics and creates or updates records in ServiceNow.

## Prerequisites

Before you begin, make sure you have:

| Requirement | Why You Need It |
|---|---|
| **Docker + Docker Compose** | Easiest way to run the bridge and Kafka together |
| **ServiceNow instance** | The system you're syncing data from/to |
| **ServiceNow credentials** | OAuth (client ID/secret + username/password) or Basic auth |

> **Optional**: Go 1.24+ if you want to build and run the bridge without Docker.

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/RaikaSurendra/servicenow-kafka-bridge.git
cd servicenow-kafka-bridge
```

---

## Step 2: Configure the Bridge

Open `config.yaml` in your editor. This is the only file you need to edit.

### 2.1 Set Your ServiceNow Credentials

Replace the placeholder values with your actual ServiceNow instance details:

```yaml
servicenow:
  base_url: "https://YOUR-INSTANCE.service-now.com"
  auth:
    type: oauth                          # or "basic"
    oauth:
      client_id: "your-client-id"
      client_secret: "your-client-secret"
      username: "your-username"
      password: "your-password"
```

> **Tip**: You can also use environment variables instead of putting credentials in the file. See [Environment Variables](#environment-variables) below.

### 2.2 Choose Which Tables to Sync (Source)

Tell the bridge which ServiceNow tables to poll:

```yaml
source:
  enabled: true
  topic_prefix: "servicenow"            # Kafka topic = prefix.tablename
  batch_size: 100                        # Records per poll
  fast_poll_interval: 500ms              # Interval when records are found
  slow_poll_interval: 30s                # Interval when no records found
  tables:
    - name: "incident"                   # ServiceNow table name
      timestamp_field: "sys_updated_on"  # Field used for incremental polling
      identifier_field: "sys_id"         # Unique ID field
      fields:                            # Optional: limit which fields to fetch
        - sys_id
        - number
        - short_description
        - state
```

### 2.3 Configure the Sink (Optional)

If you want to write data **back** to ServiceNow from Kafka:

```yaml
sink:
  enabled: true
  group_id: "servicenow-kafka-bridge-sink"
  concurrency: 5                         # Parallel writes to ServiceNow
  commit_on_partial_failure: true        # Commit offsets even if some records fail
  dlq_topic: "servicenow.dlq"           # Optional: failed records go here
  topics:
    - topic: "servicenow.incident"
      table: "incident"
```

### 2.4 Kafka Connection

By default, the bridge connects to `localhost:9092` (provided by Docker Compose). For external Kafka clusters:

```yaml
kafka:
  brokers:
    - "your-kafka-broker:9092"
  # Uncomment for secure connections:
  # tls:
  #   enabled: true
  #   ca_cert: /path/to/ca.pem
  # sasl:
  #   mechanism: PLAIN
  #   username: kafka-user
  #   password: kafka-password
```

### 2.5 Offset Storage

The bridge remembers where it left off using an offset store:

```yaml
offset:
  storage: "file"                        # "file" or "kafka"
  file_path: "data/offsets.json"
  flush_interval: 5s
```

---

## Step 3: Run the Bridge

### Option A: Docker Compose (Recommended)

This starts both Kafka and the bridge in one command:

```bash
docker compose up --build
```

### Option B: Run Natively

If you have Go installed and your own Kafka cluster:

```bash
go build ./cmd/bridge
./bridge -config config.yaml
```

---

## Step 4: Verify It's Working

### Check Health

```bash
curl http://localhost:8080/healthz
# Expected: {"status":"ok"}
```

### Check Readiness

```bash
curl http://localhost:8080/readyz
# Expected: {"ready":true}
```

### View Metrics

```bash
curl http://localhost:8080/metrics
# Returns Prometheus metrics (records synced, API latency, lag, etc.)
```

### Check Logs

Look for these messages in the output:

```
level=INFO msg="ServiceNow client initialized"
level=INFO msg="starting source poller" table=incident
level=INFO msg="fetched records" count=42
```

---

## Step 5: Run Tests

```bash
go test -race ./...
```

All packages include unit tests covering configuration, API client, polling, sink, and offset management.

---

## Environment Variables

You can use `${VAR_NAME}` placeholders in `config.yaml` to inject secrets at runtime:

| Variable | Maps To |
|---|---|
| `SN_BASE_URL` | `servicenow.base_url` |
| `SN_OAUTH_CLIENT_ID` | `servicenow.auth.oauth.client_id` |
| `SN_OAUTH_CLIENT_SECRET` | `servicenow.auth.oauth.client_secret` |
| `SN_OAUTH_USERNAME` | `servicenow.auth.oauth.username` |
| `SN_OAUTH_PASSWORD` | `servicenow.auth.oauth.password` |

Example in `config.yaml`:

```yaml
servicenow:
  base_url: "${SN_BASE_URL}"
```

Then run with:

```bash
SN_BASE_URL=https://myinstance.service-now.com docker compose up --build
```

---

## Project Structure

```
servicenow-kafka-bridge/
├── cmd/bridge/          # Application entry point
├── internal/
│   ├── config/          # YAML config parsing and validation
│   ├── kafka/           # Kafka producer and consumer (franz-go)
│   ├── observability/   # Prometheus metrics and health probes
│   ├── offset/          # Offset persistence (file or Kafka)
│   ├── partition/       # Kafka partitioning strategies
│   ├── servicenow/      # ServiceNow API client, auth, query builder
│   ├── sink/            # Kafka → ServiceNow pipeline
│   └── source/          # ServiceNow → Kafka pipeline
├── docs/                # Documentation
├── helm/                # Kubernetes Helm chart
├── config.yaml          # Main configuration file
├── docker-compose.yml   # Local development stack
└── Dockerfile           # Container build
```

---

## What's Next?

- Read the [Architecture Guide](architecture.md) to understand how the bridge works internally.
- See [Hybrid Architecture](hybrid-architecture.md) to combine with ServiceNow's IntegrationHub Kafka Spoke.
- Check the [Roadmap](roadmap.md) to see planned features.
- See [CONTRIBUTING.md](../CONTRIBUTING.md) if you'd like to contribute.
