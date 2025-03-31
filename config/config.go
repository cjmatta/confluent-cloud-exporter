package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

// Config holds configuration for the exporter
type Config struct {
	ListenAddress        string        `mapstructure:"listenAddress"`
	LogLevel             string        `mapstructure:"logLevel"`
	ConfluentAPIKey      string        `mapstructure:"apiKey"`
	ConfluentAPISecret   string        `mapstructure:"apiSecret"`
	DiscoveryInterval    time.Duration `mapstructure:"discoveryInterval"`
	MetricsCacheDuration time.Duration `mapstructure:"metricsCacheDuration"`
	TargetEnvironmentIDs []string      `mapstructure:"targetEnvironmentIDs"`
}

// LoadConfig loads configuration from all sources and returns a Config struct
func LoadConfig() (*Config, error) {
	// Set default values
	viper.SetDefault("listenAddress", ":9184")
	viper.SetDefault("logLevel", "info")
	viper.SetDefault("discoveryInterval", 5*time.Minute)
	viper.SetDefault("metricsCacheDuration", 1*time.Minute)
	viper.SetDefault("targetEnvironmentIDs", []string{})

	// Set environment variable prefix and format
	viper.SetEnvPrefix("CONFLUENT_EXPORTER")
	viper.AutomaticEnv()

	// Check for config file specifically passed on command line
	configFile := viper.GetString("config.file")
	if configFile != "" {
		// Read the file directly
		fmt.Printf("Loading configuration from file: %s\n", configFile)

		// Check if file exists
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return nil, fmt.Errorf("specified config file does not exist: %s", configFile)
		}

		// Set the config file path explicitly
		viper.SetConfigFile(configFile)
	} else {
		// Default search paths and names
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/confluent-cloud-exporter/")
	}

	// Read the config file
	if err := viper.ReadInConfig(); err != nil {
		// Only error if a config file was explicitly specified
		if configFile != "" {
			return nil, fmt.Errorf("error reading config file: %w", err)
		} else {
			fmt.Println("No configuration file found, using defaults and environment variables")
		}
	} else {
		fmt.Printf("Using config file: %s\n", viper.ConfigFileUsed())
	}

	// Map environment variables
	viper.BindEnv("apiKey", "CONFLUENT_EXPORTER_API_KEY")
	viper.BindEnv("apiSecret", "CONFLUENT_EXPORTER_API_SECRET")
	viper.BindEnv("listenAddress", "CONFLUENT_EXPORTER_LISTEN_ADDRESS")
	viper.BindEnv("logLevel", "CONFLUENT_EXPORTER_LOG_LEVEL")

	// Parse configuration
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error parsing configuration: %w", err)
	}

	// Validate required configuration
	if config.ConfluentAPIKey == "" {
		fmt.Println("Warning: No Confluent API Key provided. Set 'apiKey' in config file or CONFLUENT_EXPORTER_API_KEY environment variable.")
	}

	if config.ConfluentAPISecret == "" {
		fmt.Println("Warning: No Confluent API Secret provided. Set 'apiSecret' in config file or CONFLUENT_EXPORTER_API_SECRET environment variable.")
	}

	return &config, nil
}
