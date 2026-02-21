# Architecture Diagrams

Detailed Mermaid diagrams for the ServiceNow-Kafka Bridge. Use these in presentations, blog posts, or README files — they render natively on GitHub.

---

## Full System Architecture

```mermaid
flowchart TB
    subgraph SN["ServiceNow Instance"]
        SN_API["Table API\n/api/now/table/{table}"]
        SN_AUTH["OAuth Token Endpoint\n/oauth_token.do"]
    end

    subgraph BRIDGE["ServiceNow-Kafka Bridge (Go Binary)"]
        direction TB

        subgraph AUTH["Authentication"]
            BASIC["Basic Auth"]
            OAUTH["OAuth 2.0\nProactive Refresh"]
        end

        subgraph SOURCE["Source Pipeline"]
            direction LR
            POLLER1["Poller\nincident"]
            POLLER2["Poller\ncmdb_ci_server"]
            POLLERN["Poller\n..."]
            PRODUCER["Kafka Producer\nacks=all"]
        end

        subgraph SINK["Sink Pipeline"]
            direction LR
            CONSUMER["Kafka Consumer\nGroup: configurable"]
            WORKER["Sink Workers\nconcurrency: N"]
        end

        subgraph OBS["Observability"]
            METRICS["/metrics\nPrometheus"]
            HEALTH["/healthz\nLiveness"]
            READY["/readyz\nReadiness"]
        end

        subgraph OFFSET["Offset Management"]
            FILESTORE["FileStore\nAtomic JSON"]
            KAFKASTORE["KafkaStore\nCompacted Topic"]
        end

        CONFIG["config.yaml\nHot-Reload via fsnotify"]
    end

    subgraph KAFKA["Apache Kafka"]
        SRC_TOPICS["Source Topics\nservicenow.incident\nservicenow.cmdb_ci_server"]
        SINK_TOPICS["Sink Topics\nservicenow.incident.writeback"]
        DLQ["DLQ Topic\nservicenow.dlq"]
        OFFSET_TOPIC["Offset Topic\n_bridge_offsets"]
    end

    subgraph DOWNSTREAM["Downstream Systems"]
        FLINK["Stream Processing\nFlink / ksqlDB"]
        DW["Data Warehouse\nSnowflake / BigQuery"]
        GRAFANA["Grafana\nDashboards"]
        ALERTS["Alerting\nPagerDuty / Slack"]
    end

    %% Source flow
    SN_API -- "GET /table\n(poll)" --> POLLER1 & POLLER2 & POLLERN
    POLLER1 & POLLER2 & POLLERN --> PRODUCER
    PRODUCER -- "produce" --> SRC_TOPICS

    %% Sink flow
    SINK_TOPICS -- "consume" --> CONSUMER
    CONSUMER --> WORKER
    WORKER -- "POST/PATCH" --> SN_API
    WORKER -. "failed records" .-> DLQ

    %% Auth
    SN_AUTH -- "bearer token" --> OAUTH

    %% Offsets
    POLLER1 & POLLER2 & POLLERN -- "save offset" --> FILESTORE
    FILESTORE -. "or" .-> KAFKASTORE
    KAFKASTORE -. "store" .-> OFFSET_TOPIC

    %% Observability
    METRICS --> GRAFANA
    METRICS --> ALERTS

    %% Downstream
    SRC_TOPICS --> FLINK & DW
    DLQ --> ALERTS

    %% Config
    CONFIG -. "hot-reload" .-> SOURCE & SINK

    %% Styling
    classDef sn fill:#2d6a4f,stroke:#1b4332,color:#fff
    classDef bridge fill:#264653,stroke:#2a9d8f,color:#fff
    classDef kafka fill:#e76f51,stroke:#f4a261,color:#fff
    classDef downstream fill:#457b9d,stroke:#1d3557,color:#fff

    class SN_API,SN_AUTH sn
    class SRC_TOPICS,SINK_TOPICS,DLQ,OFFSET_TOPIC kafka
    class FLINK,DW,GRAFANA,ALERTS downstream
```

---

## Source Pipeline Detail

```mermaid
flowchart LR
    subgraph Poll Cycle
        START([Start]) --> CHECK{Offset\nexists?}
        CHECK -- No --> UNBOUNDED["Unbounded Query\nsys_idISNOTEMPTY\nORDERBY sys_updated_on"]
        CHECK -- Yes --> BOUNDED["Bounded Query\nClause1: ts=last ∧ id>lastID\nClause2: ts>last ∧ ts<through\nUnion: ^NQ"]

        UNBOUNDED --> FETCH["GET /api/now/table/{table}\n?sysparm_query=..."]
        BOUNDED --> FETCH

        FETCH --> RECORDS{Records\nfound?}
        RECORDS -- Yes --> SERIALIZE["Serialize\nJSON or Avro"]
        RECORDS -- No --> SLOW_WAIT["Wait\nslow_poll_interval"]

        SERIALIZE --> PRODUCE["Produce to Kafka\nSync, acks=all"]
        PRODUCE --> ACK{Kafka\nACK?}
        ACK -- Yes --> UPDATE_OFFSET["Update Offset\ntimestamp + sys_id"]
        ACK -- No --> LOG_ERR["Log Error\nIncrement Metric"]

        UPDATE_OFFSET --> FAST_WAIT["Wait\nfast_poll_interval"]
        LOG_ERR --> SLOW_WAIT

        FAST_WAIT --> CHECK
        SLOW_WAIT --> CHECK
    end
```

---

## Sink Pipeline Detail

```mermaid
flowchart LR
    subgraph Batch Processing
        CONSUME["Kafka Consumer\nPoll batch"] --> BATCH["Batch of\nN records"]

        BATCH --> POOL["Worker Pool\nconcurrency: 5"]

        POOL --> W1["Worker 1"] & W2["Worker 2"] & W3["Worker 3"]

        W1 & W2 & W3 --> ROUTE{sys_id\npresent?}

        ROUTE -- Yes --> PATCH["PATCH /table/{sys_id}\nUpdate record"]
        ROUTE -- No --> POST["POST /table\nInsert record"]

        PATCH & POST --> RESULT{Success?}

        RESULT -- Yes --> METRIC_OK["✓ Increment\nsink_records_total"]
        RESULT -- No --> DLQ_CHECK{DLQ\nenabled?}

        DLQ_CHECK -- Yes --> DLQ_SEND["Send to DLQ topic\n+ error metadata"]
        DLQ_CHECK -- No --> LOG["Log error\nContinue batch"]

        DLQ_SEND --> LOG

        METRIC_OK & LOG --> WAIT_BATCH["Wait for\nall workers"]

        WAIT_BATCH --> COMMIT_CHECK{Partial\nfailures?}
        COMMIT_CHECK -- "No failures" --> COMMIT["Commit Kafka\noffsets"]
        COMMIT_CHECK -- "Failures + commit_on_partial=true" --> COMMIT
        COMMIT_CHECK -- "Failures + commit_on_partial=false" --> SKIP["Skip commit\nReprocess batch"]
    end
```

---

## Authentication Flow

```mermaid
sequenceDiagram
    participant P as Poller / Sink
    participant A as OAuth Authenticator
    participant SN as ServiceNow

    Note over A: Background goroutine
    A->>SN: POST /oauth_token.do<br/>(client_id, secret, user, pass)
    SN-->>A: access_token (TTL: 30min)
    Note over A: Store token<br/>Schedule refresh at 90% TTL

    P->>A: Token()
    A-->>P: Bearer {token}
    P->>SN: GET /api/now/table/incident<br/>Authorization: Bearer {token}
    SN-->>P: 200 OK + records

    Note over A: 27 min later...<br/>Token at 90% TTL
    A->>SN: POST /oauth_token.do (refresh)
    SN-->>A: new access_token
    Note over A: Swap token atomically<br/>(RWMutex write lock)

    P->>A: Token()
    A-->>P: Bearer {new_token}
```

---

## Error Handling Flow

```mermaid
flowchart TB
    REQ["HTTP Request to ServiceNow"] --> RESP{Response}

    RESP -- "2xx" --> SUCCESS["✓ Return parsed data"]
    RESP -- "401" --> REFRESH["Force token refresh"] --> RETRY_NOW["Retry immediately\n(no backoff)"]
    RESP -- "429" --> WAIT_RA["Sleep for\nRetry-After header"] --> RETRY_AFTER["Retry"]
    RESP -- "5xx" --> BACKOFF["Exponential backoff\n+ jitter\n100ms → 5min"] --> RETRY_5XX["Retry"]
    RESP -- "Network\nerror" --> BACKOFF
    RESP -- "4xx\n(other)" --> FATAL["✗ Return error\n(no retry)"]

    RETRY_NOW --> ATTEMPT{Attempts\n< max?}
    RETRY_AFTER --> ATTEMPT
    RETRY_5XX --> ATTEMPT

    ATTEMPT -- Yes --> REQ
    ATTEMPT -- No --> EXHAUST["✗ Return last error\nAll retries exhausted"]
```

---

## Deployment Architecture (Kubernetes)

```mermaid
flowchart TB
    subgraph K8S["Kubernetes Cluster"]
        subgraph NS["namespace: servicenow"]
            DEP["Deployment\nservicenow-kafka-bridge\nreplicas: 1"]
            SVC["Service\n:8080"]
            CM["ConfigMap\nconfig.yaml"]
            PVC["PVC\noffsets.json\n1Gi"]
            SA["ServiceAccount"]
            SECRET["Secret\nSN_CLIENT_ID\nSN_CLIENT_SECRET\n..."]
        end

        subgraph MON["namespace: monitoring"]
            PROM["Prometheus\nscrape :8080/metrics"]
            GRAF["Grafana\nBridge Dashboard"]
            AM["AlertManager"]
        end
    end

    subgraph EXT["External"]
        SN_EXT["ServiceNow\nInstance"]
        KAFKA_EXT["Kafka Cluster\n(MSK / Confluent / self-hosted)"]
    end

    CM -- "mount\n/etc/bridge" --> DEP
    PVC -- "mount\n/app/data" --> DEP
    SECRET -- "env vars" --> DEP
    SA --> DEP
    DEP --> SVC
    SVC --> PROM
    PROM --> GRAF
    PROM --> AM

    DEP -- "Table API" --> SN_EXT
    DEP -- "Produce/Consume" --> KAFKA_EXT
```

---

## Hybrid Architecture (Bridge + IntegrationHub)

```mermaid
flowchart TB
    subgraph SN["ServiceNow"]
        API["Table API"]
        FD["Flow Designer"]
        IH["IntegrationHub\nKafka Spoke"]
    end

    subgraph KAFKA["Kafka"]
        RT["Real-Time Topics\n(from Spoke)"]
        BATCH["Batch Topics\n(from Bridge)"]
        WB["Write-Back Topics"]
        DLQ["DLQ"]
    end

    subgraph BRIDGE["Bridge"]
        SRC["Source Poller\n(batch tables only)"]
        SNK["Sink Worker"]
    end

    subgraph DS["Downstream"]
        PROC["Stream Processing"]
        STORE["Data Store"]
    end

    FD -- "record change\ntrigger" --> IH
    IH -- "real-time\nproduce" --> RT
    API -- "poll\n(batch)" --> SRC
    SRC -- "produce" --> BATCH

    RT & BATCH --> PROC & STORE
    PROC --> WB

    WB --> SNK
    SNK -- "POST/PATCH" --> API
    SNK -. "failures" .-> DLQ

    style RT fill:#2d6a4f,color:#fff
    style BATCH fill:#264653,color:#fff
    style WB fill:#e76f51,color:#fff
    style DLQ fill:#9b2226,color:#fff
```
