package main

import (
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"confluent-cloud-exporter/config"

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

	// Set flag values to viper
	viper.Set("config.file", *configFile)
	viper.Set("listenAddress", *listenAddr)
	viper.Set("logLevel", *logLevel)
	viper.Set("apiKey", *apiKey)
	viper.Set("apiSecret", *apiSecret)
	viper.Set("discoveryInterval", *discoveryInterval)
	viper.Set("metricsCacheDuration", *metricsCacheDuration)

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

	// Register Prometheus metrics handler
	http.Handle("/metrics", promhttp.Handler())

	// Start HTTP server
	logger.Info("HTTP server listening", "address", cfg.ListenAddress)
	if err := http.ListenAndServe(cfg.ListenAddress, nil); err != nil {
		logger.Error("HTTP server error", "error", err)
		os.Exit(1)
	}
}
