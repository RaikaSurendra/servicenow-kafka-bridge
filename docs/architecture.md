# ServiceNow-Kafka Bridge: Detailed Design Guide

This document outlines the internal architecture, design patterns, and technical decisions made for the Go implementation of the ServiceNow-Kafka Bridge.

## 1. Architectural Philosophy

The bridge is designed as a **resilient, stateless (except for offsets) data mover**.
-   **Concurrency**: Uses Go's `errgroup` for structured concurrency. Each table poller and sink worker runs in its own goroutine.
-   **Resilience**: Implements exponential backoff with jitter and proactive OAuth token refresh.
-   **Durability**: Guarantees "at-least-once" delivery by acknowledging data consumption only after successful production.

---

## 2. Component Breakdown

### 2.1 Proactive OAuth Authenticator
Located in `internal/servicenow/auth.go`.
-   **Design**: Instead of waiting for a 401 error to refresh tokens (reactive), a background loop monitors the token's TTL.
-   **Refresh Logic**: Triggers a refresh when the token reaches 90% of its lifespan.
-   **Thread Safety**: Uses `sync.RWMutex` to allow multiple API calls to read the token simultaneously while blocking only during the brief write period during refresh.

### 2.2 Table API Query Builder
Located in `internal/servicenow/query.go`.
-   **Fluent API**: A Go port of the Java fluent builder, allowing for readable query construction: `builder.WhereEquals("active", "true").OrderByAsc("sys_updated_on")`.
-   **Sanitization**: Automatically escapes ServiceNow special characters (like `^`) to prevent query injection or syntax errors.

### 2.3 Incremental Polling Logic (Source)
Located in `internal/source/poller.go`.
-   **Bounded Query Strategy**: To ensure no records are missed during high-frequency updates, the poller uses a "Through Timestamp" (Now - Delay).
-   **Dual-Clause Query**:
    -   Clause 1 handles records with the exact same timestamp as the last seen record but higher `sys_id`.
    -   Clause 2 handles all records with a timestamp strictly greater than the last seen record but less than the "Through" boundary.

### 2.4 Sink Pipeline (Kafka to ServiceNow)
Located in `internal/sink/sink.go`.
-   **Intelligent Routing**: Does not blindly POST data. It checks for a `sys_id` in the payload.
    -   **Present**: Executes a `PATCH` (Update) request.
    -   **Absent**: Executes a `POST` (Insert) request.
-   **Batch Integrity**: If one record in a Kafka batch fails, the bridge logs the error but continues processing the rest of the batch to prevent head-of-line blocking.

---

## 3. Data Flow

### Source Flow (SN -> Kafka)
1.  **Poll**: Fetch a batch of N records from SN using the incremental query.
2.  **Produce**: Publish each record to its corresponding Kafka topic.
3.  **Ack**: Wait for Kafka's synchronous acknowledgement (`acks=all`).
4.  **Commit**: Update the local/remote offset store with the latest timestamp and `sys_id`.

### Sink Flow (Kafka -> SN)
1.  **Consume**: Fetch a batch of messages from Kafka.
2.  **Write**: Translate Kafka messages to ServiceNow Table API payloads and send them.
3.  **Commit**: Commit the Kafka offset only after the ServiceNow API returns 200/201.

---

## 4. Error Handling Matrix

| Component | Error Class | Strategy |
| :--- | :--- | :--- |
| **SN Client** | 401 Unauthorized | Immediate token refresh + retry. |
| **SN Client** | 429 Too Many Requests | Respect `Retry-After` header + sleep. |
| **SN Client** | 5xx / Network | Exponential backoff + jitter. |
| **Kafka Producer** | Connection Loss | Automatic retry (franz-go internal). |
| **Offset Store** | Disk Full / IO Error | Log error, stop polling (prevents data loss). |

---

## 5. Performance Optimizations

1.  **Shared HTTP Transport**: All pollers share a single pooling HTTP client to reuse TCP connections.
2.  **JSON Streaming**: Lowers memory overhead by using `json.Decoder` for large API responses (planned improvement).
3.  **Lock-Free Progress**: Pollers are independent; one slow table does not block others.
