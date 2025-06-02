package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
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

var (
	version   = "1.0.0"   // Example version, replace with actual version
	buildTime = "unknown" // Example build time, replace with actual build time
)

func registerBuildInfo(logger *slog.Logger) {
	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "confluent_cloud_exporter_build_info",
			Help: "Build information about the exporter",
		},
		[]string{"version", "go_version", "built_at"},
	)
	buildInfo.WithLabelValues(version, runtime.Version(), buildTime).Set(1)
	prometheus.MustRegister(buildInfo)
	logger.Debug("Registered build information metrics")
}

func setupLogger(level string) *slog.Logger {
	var logLevelVar slog.Level
	switch strings.ToLower(level) {
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

	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevelVar,
	}))
}

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

	flag.Parse() // Standard flag parsing happens here

	// Set config file path for Viper if provided via flag
	if *configFile != "" {
		viper.Set("config.file", *configFile)
	}

	// Apply only flags that were explicitly set on the command line to Viper
	// This allows env vars and config file values to take precedence otherwise
	flagsSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		flagsSet[f.Name] = true
	})

	if flagsSet["web.listen-address"] {
		viper.Set("listenAddress", *listenAddr)
	}
	if flagsSet["log.level"] {
		viper.Set("logLevel", *logLevel)
	}
	if flagsSet["confluent.api-key"] {
		viper.Set("apiKey", *apiKey)
	}
	if flagsSet["confluent.api-secret"] {
		viper.Set("apiSecret", *apiSecret)
	}
	if flagsSet["discovery.interval"] {
		viper.Set("discoveryInterval", *discoveryInterval)
	}
	if flagsSet["metrics.cache-duration"] {
		viper.Set("metricsCacheDuration", *metricsCacheDuration)
	}
	if flagsSet["discovery.target-environments"] {
		viper.Set("targetEnvironmentIDs", strings.Split(*targetEnvs, ","))
	}

	// Load configuration (viper reads config file, env vars, defaults, and explicitly set flags)
	cfg, err := config.LoadConfig()
	if err != nil {
		// Use basic logger since structured logger isn't set up yet
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Configure structured logger
	logger := setupLogger(cfg.LogLevel)

	// Log effective configuration (excluding secrets)
	logger.Info("Starting Confluent Cloud Exporter",
		"listenAddress", cfg.ListenAddress,
		"logLevel", cfg.LogLevel,
		"discoveryInterval", cfg.DiscoveryInterval,
		"metricsCacheDuration", cfg.MetricsCacheDuration,
		"targetEnvironmentIDs", cfg.TargetEnvironmentIDs,
		"apiKeyConfigured", cfg.ConfluentAPIKey != "",
	)

	// Register build information metrics
	registerBuildInfo(logger)

	// Create a shared rate limiter for all Confluent API clients
	// Increased rate limits for better performance - was 2 RPS / 10 burst
	discoveryLimiter := confluent.NewAdaptiveRateLimiter(5, 20) // Increased from 2 RPS/10 burst to 5 RPS/20 burst
	metricsLimiter := confluent.NewAdaptiveRateLimiter(2, 10)   // Increased from 1 RPS/5 burst to 2 RPS/10 burst

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

	// Create metrics client with the limiter
	metricsClient := metrics.NewClient(cfg.ConfluentAPIKey, cfg.ConfluentAPISecret, metricsLimiter)
	metricsClient.SetLogger(logger)

	// Create the collector with the discovery service
	confluentCollector := collector.NewCollector(
		discoveryService,
		metricsClient,
		cfg.MetricsCacheDuration,
	)
	confluentCollector.SetLogger(logger)

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

	// Kick off initial discovery and metrics fetch in background
	go func() {
		logger.Info("Starting initial background discovery & metrics fetch")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := discoveryService.RefreshResources(ctx); err != nil {
			logger.Error("Initial background resource discovery failed", "error", err)
		}
		if err := confluentCollector.RefreshMetricsAsync(ctx); err != nil {
			logger.Error("Initial background metrics fetch failed", "error", err)
		}
	}()

	// Start background resource discovery
	go func() {
		// First wait for a delay before starting the periodic updates
		// to avoid hammering the API if the initial discovery just completed
		time.Sleep(cfg.DiscoveryInterval)

		// Set up ticker for periodic updates
		ticker := time.NewTicker(cfg.DiscoveryInterval)
		defer ticker.Stop()

		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			if err := confluentCollector.RefreshResourcesAsync(ctx); err != nil {
				logger.Error("Background resource discovery failed", "error", err)
			}
			cancel()
		}
	}()

	// Wait for termination signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

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
