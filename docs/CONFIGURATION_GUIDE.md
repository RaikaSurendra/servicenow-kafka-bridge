# ServiceNow-Kafka Bridge: Configuration Guide

This guide provides step-by-step instructions for configuring the ServiceNow-Kafka Bridge for both Source (ServiceNow to Kafka) and Sink (Kafka to ServiceNow) pipelines.

## 1. Prerequisites

Before starting, ensure you have:
-   **ServiceNow Credentials**: A user account with REST API access or a Client ID/Secret for OAuth.
-   **Kafka Cluster**: Connection details (bootstrap servers) and access to create/write to topics.
-   **Go Environment**: (Optional, if running natively) Go 1.24+ installed.

---

## 2. Configuration File Structure

The bridge uses a `config.yaml` file. You can either edit this file directly or use environment variables to override values.

### 2.1 ServiceNow Connection (`servicenow:`)

| Field | Description | Example |
| :--- | :--- | :--- |
| `base_url` | Your ServiceNow instance URL. | `https://dev12345.service-now.com` |
| `auth.type` | Authentication method: `oauth` or `basic`. | `oauth` |
| `auth.oauth.client_id` | OAuth Client ID from ServiceNow. | `d0fb...` |
| `auth.oauth.client_secret` | OAuth Client Secret. | `P@ss...` |
| `auth.oauth.username` | Service account username. | `bridge_user` |
| `auth.oauth.password` | Service account password. | `...` |

### 2.2 Source Pipeline (`source:`)

The source pipeline pulls data from ServiceNow and pushes to Kafka.

```yaml
source:
  topic_prefix: "sn"
  batch_size: 100
  fast_poll_interval: 500ms
  slow_poll_interval: 30s
  tables:
    - name: "incident"
      timestamp_field: "sys_updated_on"
      identifier_field: "sys_id"
      fields: ["sys_id", "number", "short_description", "state"]
      partitioner: "field_based"
      partition_key_fields: ["sys_id"]
```

#### Step-by-Step Table Setup:
1.  **name**: Exact ServiceNow table name (e.g., `incident`, `change_request`).
2.  **timestamp_field**: The field used for incremental polling (usually `sys_updated_on`).
3.  **identifier_field**: The unique ID field (usually `sys_id`).
4.  **fields**: (Optional) List of specific fields to fetch. If empty, all fields are returned.
5.  **partitioner**: Choose `default`, `round_robin`, or `field_based`.

### 2.3 Kafka Connection (`kafka:`)

Kafka connection settings used by both source and sink pipelines.

```yaml
kafka:
  brokers:
    - "localhost:9092"
  # tls:
  #   enabled: false
  #   ca_cert: /path/to/ca.pem
  #   cert_file: /path/to/cert.pem
  #   key_file: /path/to/key.pem
  # sasl:
  #   mechanism: PLAIN  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
  #   username: kafka-user
  #   password: kafka-password
  # schema_registry_url: "http://registry:8081"
```

### 2.4 Sink Pipeline (`sink:`)

The sink pipeline consumes from Kafka and writes to ServiceNow.

```yaml
sink:
  enabled: true
  group_id: "servicenow-kafka-bridge-sink"
  concurrency: 5
  commit_on_partial_failure: true
  dlq_topic: "servicenow.dlq" # optional
  topics:
    - topic: "servicenow.incident"
      table: "incident"
```

---

## 3. Environment Variable Overrides

Any value in the YAML can be overridden using environment variables with `${VAR_NAME}` placeholders in `config.yaml`.
Common variables used in the default config and docker-compose are:
-   `SN_BASE_URL` -> `servicenow.base_url`
-   `SN_OAUTH_CLIENT_ID` -> `servicenow.auth.oauth.client_id`
-   `SN_OAUTH_CLIENT_SECRET` -> `servicenow.auth.oauth.client_secret`
-   `SN_OAUTH_USERNAME` -> `servicenow.auth.oauth.username`
-   `SN_OAUTH_PASSWORD` -> `servicenow.auth.oauth.password`

---

## 4. Offset Persistence

The bridge tracks its progress in an offset store. By default, it uses a file-based store:

```yaml
offset:
  storage: "file"            # "file" or "kafka"
  file_path: "data/offsets.json"
  kafka_topic: "_bridge_offsets"
  flush_interval: 5s
```

**Note**: Ensure the directory `data/` is writable by the user running the bridge.

---

## 5. Running the Bridge

### Using Docker (Recommended)
```bash
docker compose up --build
```

### Running Natively
```bash
go run ./cmd/bridge -config config.yaml
```

---

## 6. Verification

Check the logs for initialization messages:
-   `level=INFO msg="ServiceNow client initialized"`
-   `level=INFO msg="starting source poller" table=incident`
-   `level=INFO msg="starting sink worker"`

Test the health endpoint:
```bash
curl http://localhost:8080/healthz
```
