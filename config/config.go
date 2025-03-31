package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration options for the exporter
type Config struct {
	ConfluentAPIKey      string        `mapstructure:"apiKey"`
	ConfluentAPISecret   string        `mapstructure:"apiSecret"`
	DiscoveryInterval    time.Duration `mapstructure:"discoveryInterval"`
	MetricsCacheDuration time.Duration `mapstructure:"metricsCacheDuration"`
	TargetEnvironmentIDs []string      `mapstructure:"targetEnvironmentIDs"`
	LogLevel             string        `mapstructure:"logLevel"`
	ListenAddress        string        `mapstructure:"listenAddress"`
}

// LoadConfig loads configuration from file, environment variables, and flags
func LoadConfig() (*Config, error) {
	// Set default values
	viper.SetDefault("discoveryInterval", "5m")
	viper.SetDefault("metricsCacheDuration", "1m")
	viper.SetDefault("logLevel", "info")
	viper.SetDefault("listenAddress", ":9184")

	// Load from config file if specified
	configFile := viper.GetString("config.file")
	if configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Environment variables
	viper.SetEnvPrefix("CONFLUENT_EXPORTER")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Parse target environment IDs from comma-separated string if provided via env var
	if envs := viper.GetString("targetEnvironmentIDs"); envs != "" {
		viper.Set("targetEnvironmentIDs", strings.Split(envs, ","))
	}

	// Unmarshal config
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// For development/testing, allow running without credentials
	// In production, we'll validate credentials before making API calls
	if cfg.ConfluentAPIKey == "" && cfg.ConfluentAPISecret == "" {
		fmt.Println("Warning: No Confluent API credentials provided. Running in development mode.")
	}

	// Ensure durations are properly parsed
	if cfg.DiscoveryInterval <= 0 {
		return nil, fmt.Errorf("discovery interval must be positive")
	}
	if cfg.MetricsCacheDuration <= 0 {
		return nil, fmt.Errorf("metrics cache duration must be positive")
	}

	return &cfg, nil
}
