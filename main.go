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

	// Create metrics descriptors
	metricDescriptors := metrics.NewMetricsDescriptors()

	// Create the discovery service
	discoveryService, err := discovery.NewDiscoveryService(
		cfg.ConfluentAPIKey,
		cfg.ConfluentAPISecret,
		cfg.TargetEnvironmentIDs,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create discovery service", "error", err)
		os.Exit(1)
	}

	// Perform initial discovery (synchronously)
	logger.Info("Running initial resource discovery")
	discoveryCtx, discoveryCancel := context.WithTimeout(context.Background(), 3*time.Minute) // Use 3 minutes instead of 30 seconds
	defer discoveryCancel()

	if err := discoveryService.RefreshResources(discoveryCtx); err != nil {
		logger.Error("Initial resource discovery failed", "error", err)
		// Continue starting up - we'll retry discovery in the background
	} else {
		resources := discoveryService.GetResources()
		logger.Info("Initial resource discovery completed", "resources_found", len(resources))
	}

	// Create a context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Start periodic discovery refresh in background
	go func() {
		ticker := time.NewTicker(cfg.DiscoveryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Debug("Running periodic resource discovery")
				// Use a longer timeout for the refresh
				refreshCtx, refreshCancel := context.WithTimeout(context.Background(), 2*time.Minute)

				if err := discoveryService.RefreshResources(refreshCtx); err != nil {
					logger.Error("Periodic resource discovery failed", "error", err)
				} else {
					resources := discoveryService.GetResources()
					logger.Info("Periodic resource discovery completed", "resources_found", len(resources))
				}

				refreshCancel()
			case <-shutdownCtx.Done():
				logger.Info("Stopping periodic resource discovery")
				return
			}
		}
	}()

	// Create and register the collector
	confluent := collector.NewCollector(cfg, discoveryService, metricDescriptors, logger)
	prometheus.MustRegister(confluent)

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
	shutdownCancel()

	// Shutdown HTTP server gracefully
	shutdownCtx, serverShutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer serverShutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("Exporter shutdown complete")
}
