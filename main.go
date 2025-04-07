package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"confluent-cloud-exporter/collector"
	"confluent-cloud-exporter/config"
	"confluent-cloud-exporter/confluent"
	"confluent-cloud-exporter/discovery"
	"confluent-cloud-exporter/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

func main() {
	// Define command line flags
	configFile := flag.String("config.file", "", "Path to configuration file")
	listenAddr := flag.String("web.listen-address", ":9184", "Address to listen on for HTTP requests")
	logLevel := flag.String("log.level", "info", "Log level (debug, info, warn, error)")
	apiKey := flag.String("confluent.api-key", "", "Confluent Cloud API Key")
	apiSecret := flag.String("confluent.api-secret", "", "Confluent Cloud API Secret")
	discoveryInterval := flag.Duration("discovery.interval", 5*time.Minute, "Interval between resource discovery runs")
	metricsCacheDuration := flag.Duration("metrics.cache-duration", time.Minute, "Duration to cache metrics results")
	targetEnvs := flag.String("discovery.target-environments", "", "Comma-separated list of environment IDs to discover within")

	flag.Parse()

	// Set flag values to viper (important to do this BEFORE loading config)
	if *configFile != "" {
		viper.Set("config.file", *configFile)
		fmt.Printf("Using config file specified via flag: %s\n", *configFile)
	}

	// Then set other CLI params
	viper.Set("listenAddress", *listenAddr)
	viper.Set("logLevel", *logLevel)

	// Use CLI params only if provided (don't override config file)
	if *apiKey != "" {
		viper.Set("apiKey", *apiKey)
	}
	if *apiSecret != "" {
		viper.Set("apiSecret", *apiSecret)
	}
	if *discoveryInterval != 5*time.Minute {
		viper.Set("discoveryInterval", *discoveryInterval)
	}
	if *metricsCacheDuration != time.Minute {
		viper.Set("metricsCacheDuration", *metricsCacheDuration)
	}
	if *targetEnvs != "" {
		viper.Set("targetEnvironmentIDs", strings.Split(*targetEnvs, ","))
	}

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Configure structured logger
	var logLevelVar slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		logLevelVar = slog.LevelDebug
	case "info":
		logLevelVar = slog.LevelInfo
	case "warn":
		logLevelVar = slog.LevelWarn
	case "error":
		logLevelVar = slog.LevelError
	default:
		logLevelVar = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevelVar,
	}))

	// Log effective configuration (excluding secrets)
	logger.Info("Starting Confluent Cloud Exporter",
		"listenAddress", cfg.ListenAddress,
		"logLevel", cfg.LogLevel,
		"discoveryInterval", cfg.DiscoveryInterval,
		"metricsCacheDuration", cfg.MetricsCacheDuration,
		"targetEnvironmentIDs", cfg.TargetEnvironmentIDs,
		"apiKeyConfigured", cfg.ConfluentAPIKey != "",
	)

	// Create a shared rate limiter for all Confluent API clients
	// 1 request per second with burst of 5
	discoveryLimiter := confluent.NewAdaptiveRateLimiter(2, 10) // Start with 2 rps, burst 10
	metricsLimiter := confluent.NewAdaptiveRateLimiter(1, 5)    // Start with 1 rps, burst 5

	// Create the discovery service
	discoveryService, err := discovery.NewDiscoveryService(
		cfg.ConfluentAPIKey,
		cfg.ConfluentAPISecret,
		cfg.TargetEnvironmentIDs,
		logger,
		discoveryLimiter,
	)
	if err != nil {
		logger.Error("Failed to create discovery service", "error", err)
		os.Exit(1)
	}

	// Create clients with the limiter
	metricsClient := metrics.NewClient(cfg.ConfluentAPIKey, cfg.ConfluentAPISecret, metricsLimiter)
	metricsClient.SetLogger(logger)

	// Create the collector with the discovery service
	confluentCollector := collector.NewCollector(
		discoveryService,
		metricsClient,
		cfg.MetricsCacheDuration,
	)
	confluentCollector.SetLogger(logger)

	// Start background resource discovery
	go func() {
		// Initial discovery
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		if err := confluentCollector.RefreshResourcesAsync(ctx); err != nil {
			logger.Error("Initial resource discovery failed", "error", err)
		}
		cancel()

		// Set up ticker for periodic updates
		ticker := time.NewTicker(cfg.DiscoveryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				if err := confluentCollector.RefreshResourcesAsync(ctx); err != nil {
					logger.Error("Background resource discovery failed", "error", err)
				}
				cancel()
			}
		}
	}()

	// Register the collector with Prometheus
	prometheus.MustRegister(confluentCollector)

	// Register Prometheus metrics handler
	http.Handle("/metrics", promhttp.Handler())

	// Add a simple health check endpoint
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Set up signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in a goroutine
	server := &http.Server{
		Addr: cfg.ListenAddress,
	}

	go func() {
		logger.Info("HTTP server listening", "address", cfg.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for termination signal
	sig := <-signalChan
	logger.Info("Received signal, shutting down", "signal", sig)

	// Trigger shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("Exporter shutdown complete")
}
