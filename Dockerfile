# ============================================================================
# ServiceNow-Kafka Bridge — Multi-stage Docker Build
#
# Stage 1 (build): Compiles the Go binary with version info injected via
#   ldflags. Uses golang:1.24.1-alpine for a minimal build environment.
#
# Stage 2 (runtime): Copies the compiled binary into a scratch-like Alpine
#   image (< 20 MB) with CA certificates for HTTPS connectivity.
#
# Usage:
#   docker build -t servicenow-kafka-bridge .
#   docker run -v $(pwd)/config.yaml:/app/config.yaml servicenow-kafka-bridge
# ============================================================================

# ----- Build Stage -----
FROM golang:1.24.1-alpine AS build

WORKDIR /src

# Install git for module downloads and ca-certificates for HTTPS.
RUN apk add --no-cache git ca-certificates

# Cache Go module downloads separately from source code.
# This layer only rebuilds when go.mod or go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build.
COPY . .

# Build arguments for version injection via ldflags.
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.buildDate=${BUILD_DATE}" \
    -o /bridge \
    ./cmd/bridge

# ----- Runtime Stage -----
FROM alpine:3.19 AS runtime

# Install CA certificates for TLS connections to ServiceNow and Kafka.
RUN apk add --no-cache ca-certificates tzdata

# Create a non-root user for security.
RUN adduser -D -g '' bridge

WORKDIR /app

# Copy the compiled binary from the build stage.
COPY --from=build /bridge /app/bridge

# Copy the example config (users should mount their own).
COPY config.yaml /app/config.yaml

# The offset file will be written to /app/data by default.
RUN mkdir -p /app/data && chown bridge:bridge /app/data
VOLUME ["/app/data"]

# Switch to non-root user.
USER bridge

# Expose the observability port.
EXPOSE 8080

# Health check using the /healthz endpoint.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/bridge"]
CMD ["-config", "/app/config.yaml"]
