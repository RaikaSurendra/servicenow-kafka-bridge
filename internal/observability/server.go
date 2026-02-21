// Package observability provides the HTTP server for health checks and
// Prometheus metrics endpoints.
//
// # Endpoints
//
//   - GET /healthz: Health check endpoint. Returns 200 if the bridge process
//     is running. Used by Docker HEALTHCHECK and Kubernetes liveness probes.
//
//   - GET /readyz: Readiness check endpoint. Returns 200 when the bridge has
//     completed initialization and is actively polling/consuming. Used by
//     Kubernetes readiness probes and load balancers.
//
//   - GET /metrics: Prometheus metrics in text exposition format. Includes
//     both Go runtime metrics and custom bridge metrics.
//
// # Custom Metrics
//
// The following bridge-specific metrics are exported:
//
//	┌──────────────────────────────────────┬─────────┬─────────────────────────────────────┐
//	│ Metric Name                          │ Type    │ Description                         │
//	├──────────────────────────────────────┼─────────┼─────────────────────────────────────┤
//	│ bridge_source_records_total          │ Counter │ Records fetched from ServiceNow     │
//	│ bridge_source_produce_total          │ Counter │ Records produced to Kafka           │
//	│ bridge_source_errors_total           │ Counter │ Source pipeline errors               │
//	│ bridge_source_poll_duration_seconds  │ Hist    │ Duration of ServiceNow API polls     │
//	│ bridge_sink_records_total            │ Counter │ Records written to ServiceNow       │
//	│ bridge_sink_errors_total             │ Counter │ Sink pipeline errors                 │
//	│ bridge_sn_api_requests_total         │ Counter │ Total ServiceNow API requests       │
//	│ bridge_sn_api_errors_total           │ Counter │ ServiceNow API errors (by code)     │
//	│ bridge_sn_api_latency_seconds        │ Hist    │ ServiceNow API response latency     │
//	│ bridge_offset_lag_seconds            │ Gauge   │ Seconds behind real-time (per table)│
//	└──────────────────────────────────────┴─────────┴─────────────────────────────────────┘
//
// # Usage
//
//	srv := observability.NewServer(":8080", logger)
//	go srv.Start(ctx)
//	// When ready:
//	srv.SetReady(true)
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ----- Prometheus Metrics -----

// Metrics holds all Prometheus metrics used by the bridge.
// Using promauto for automatic registration with the default registry.
var Metrics = struct {
	// Source metrics
	SourceRecordsTotal *prometheus.CounterVec
	SourceProduceTotal *prometheus.CounterVec
	SourceErrorsTotal  *prometheus.CounterVec
	SourcePollDuration *prometheus.HistogramVec

	// Sink metrics
	SinkRecordsTotal *prometheus.CounterVec
	SinkErrorsTotal  *prometheus.CounterVec

	// ServiceNow API metrics
	SNAPIRequestsTotal *prometheus.CounterVec
	SNAPIErrorsTotal   *prometheus.CounterVec
	SNAPILatency       *prometheus.HistogramVec

	// Offset metrics
	OffsetLagSeconds *prometheus.GaugeVec
}{
	SourceRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_source_records_total",
		Help: "Total number of records fetched from ServiceNow tables.",
	}, []string{"table"}),

	SourceProduceTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_source_produce_total",
		Help: "Total number of records successfully produced to Kafka.",
	}, []string{"table", "topic"}),

	SourceErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_source_errors_total",
		Help: "Total number of source pipeline errors.",
	}, []string{"table", "error_type"}),

	SourcePollDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bridge_source_poll_duration_seconds",
		Help:    "Duration of ServiceNow Table API poll requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"table"}),

	SinkRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_sink_records_total",
		Help: "Total number of records written to ServiceNow from Kafka.",
	}, []string{"table", "operation"}),

	SinkErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_sink_errors_total",
		Help: "Total number of sink pipeline errors.",
	}, []string{"table", "error_type"}),

	SNAPIRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_sn_api_requests_total",
		Help: "Total number of ServiceNow API requests.",
	}, []string{"method", "endpoint"}),

	SNAPIErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_sn_api_errors_total",
		Help: "Total number of ServiceNow API errors by status code.",
	}, []string{"method", "status_code"}),

	SNAPILatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bridge_sn_api_latency_seconds",
		Help:    "ServiceNow API response latency in seconds.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"method", "endpoint"}),

	OffsetLagSeconds: promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bridge_offset_lag_seconds",
		Help: "Seconds behind real-time for each source table.",
	}, []string{"table"}),
}

// ----- Health/Readiness Server -----

// Server provides HTTP endpoints for health checks, readiness probes,
// and Prometheus metrics.
type Server struct {
	addr   string
	ready  atomic.Bool
	logger *slog.Logger
	srv    *http.Server
}

// NewServer creates a new observability HTTP server.
func NewServer(addr string, logger *slog.Logger) *Server {
	s := &Server{
		addr:   addr,
		logger: logger.With("component", "observability"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.Handle("/metrics", promhttp.Handler())

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start begins listening for HTTP requests. Blocks until the context is
// cancelled, then gracefully shuts down the server.
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info("observability server starting", "addr", s.addr)

	go func() {
		<-ctx.Done()
		s.logger.Info("shutting down observability server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	if err := s.srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("observability server: %w", err)
	}
	return nil
}

// SetReady marks the server as ready (or not ready) for readiness probes.
// Call SetReady(true) after all pollers and consumers have started.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
	s.logger.Info("readiness state changed", "ready", ready)
}

// handleHealth responds with 200 OK — the process is alive.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"healthy"}`)
}

// handleReady responds with 200 if ready, 503 if not yet ready.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready"}`)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"not_ready"}`)
	}
}
