// ServiceNow-Kafka Bridge
//
// A standalone Go binary that replaces the IBM kafka-connect-servicenow Java
// connector. It provides bidirectional data flow between ServiceNow tables and
// Kafka topics:
//
//	Source:  ServiceNow Table API  →  Kafka Topics
//	Sink:   Kafka Topics          →  ServiceNow Table API
//
// # Usage
//
//	servicenow-kafka-bridge [flags]
//
//	Flags:
//	  -config string   Path to config YAML file (default "config.yaml")
//	  -version         Print version information and exit
//
// # Architecture
//
// The bridge starts the following components based on configuration:
//
//  1. Observability server (always): /healthz, /readyz, /metrics
//  2. Offset store: FileStore or KafkaStore
//  3. ServiceNow HTTP client with authentication
//  4. Source pollers (if enabled): One goroutine per configured table
//  5. Sink workers (if enabled): One goroutine per configured topic
//
// All components are managed via errgroup for coordinated lifecycle. On
// shutdown (SIGINT/SIGTERM), all goroutines are cancelled gracefully.
//
// # Signal Handling
//
//	SIGINT/SIGTERM → Cancel context → All pollers/workers stop → Flush offsets → Exit
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/config"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/kafka"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/observability"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/offset"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/servicenow"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/sink"
	"github.com/RaikaSurendra/servicenow-kafka-bridge/internal/source"
)

// Build-time variables injected via ldflags.
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	// Parse command-line flags.
	configPath := flag.String("config", "config.yaml", "Path to configuration YAML file")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("servicenow-kafka-bridge %s (commit: %s, built: %s)\n", version, commit, buildDate)
		os.Exit(0)
	}

	// Initialize structured logging.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting servicenow-kafka-bridge",
		"version", version,
		"commit", commit,
		"build_date", buildDate,
	)

	// Load and validate configuration.
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load configuration", "path", *configPath, "error", err)
		os.Exit(1)
	}

	// Set log level from config.
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Setup signal handling for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("bridge exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("bridge shutdown complete")
}

// run is the main execution function, separated from main() for testability.
// It sets up all components and runs them via errgroup.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	// 1. Start the observability server (always runs).
	obsSrv := observability.NewServer(cfg.Observability.Addr, logger)

	// 2. Initialize the offset store.
	store, err := offset.NewFileStore(cfg.Offset.FilePath)
	if err != nil {
		return fmt.Errorf("initializing offset store: %w", err)
	}
	defer store.Close()

	// Start a periodic offset flush goroutine.
	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go func() {
		ticker := time.NewTicker(cfg.Offset.FlushInterval.Duration)
		defer ticker.Stop()
		for {
			select {
			case <-flushCtx.Done():
				return
			case <-ticker.C:
				if err := store.Flush(); err != nil {
					logger.Error("offset flush failed", "error", err)
				}
			}
		}
	}()

	// 3. Initialize ServiceNow authentication.
	auth, err := servicenow.NewAuthenticator(ctx, cfg.ServiceNow, logger)
	if err != nil {
		return fmt.Errorf("initializing authenticator: %w", err)
	}
	defer auth.Close()

	// 4. Initialize ServiceNow HTTP client.
	var clientOpts []servicenow.ClientOption
	if cfg.ServiceNow.RateLimitRPS > 0 {
		clientOpts = append(clientOpts, servicenow.WithRateLimiter(cfg.ServiceNow.RateLimitRPS))
	}
	snClient := servicenow.NewClient(cfg.ServiceNow, auth, logger, clientOpts...)
	defer snClient.Close()

	// 5. Use errgroup for coordinated goroutine lifecycle.
	g, gCtx := errgroup.WithContext(ctx)

	// Start observability server.
	g.Go(func() error {
		return obsSrv.Start(gCtx)
	})

	// 6. Start source pollers (ServiceNow → Kafka).
	if cfg.Source.Enabled {
		producer, err := kafka.NewProducer(cfg.Kafka, logger)
		if err != nil {
			return fmt.Errorf("creating Kafka producer: %w", err)
		}
		defer producer.Close()

		for _, table := range cfg.Source.Tables {
			table := table // capture loop variable
			poller, err := source.NewPoller(table, cfg.Source, snClient, producer, store, logger)
			if err != nil {
				return fmt.Errorf("creating poller for table %s: %w", table.Name, err)
			}
			g.Go(func() error {
				return poller.Run(gCtx)
			})
			logger.Info("source poller started", "table", table.Name, "topic", table.Topic)
		}
	}

	// 7. Start sink workers (Kafka → ServiceNow).
	if cfg.Sink.Enabled {
		for _, topicCfg := range cfg.Sink.Topics {
			topicCfg := topicCfg // capture loop variable
			consumer, err := kafka.NewConsumer(
				cfg.Kafka,
				"servicenow-kafka-bridge-sink",
				[]string{topicCfg.Topic},
				logger,
			)
			if err != nil {
				return fmt.Errorf("creating Kafka consumer for topic %s: %w", topicCfg.Topic, err)
			}
			defer consumer.Close()

			worker := sink.NewWorker(topicCfg, consumer, snClient, logger)
			g.Go(func() error {
				return worker.Run(gCtx)
			})
			logger.Info("sink worker started", "topic", topicCfg.Topic, "table", topicCfg.Table)
		}
	}

	// Mark as ready — all components are initialized and running.
	obsSrv.SetReady(true)
	logger.Info("bridge is ready",
		"source_enabled", cfg.Source.Enabled,
		"sink_enabled", cfg.Sink.Enabled,
		"observability_addr", cfg.Observability.Addr,
	)

	// Wait for all goroutines to complete (triggered by context cancellation).
	if err := g.Wait(); err != nil && err != context.Canceled {
		return err
	}

	// Final offset flush on shutdown.
	logger.Info("performing final offset flush")
	if err := store.Flush(); err != nil {
		logger.Error("final offset flush failed", "error", err)
	}

	return nil
}
