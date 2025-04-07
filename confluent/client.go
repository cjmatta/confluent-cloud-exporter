package confluent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
)

const (
	baseURL            = "https://api.confluent.cloud"
	environmentsPath   = "/org/v2/environments"
	kafkaClustersPath  = "/cmk/v2/clusters"
	schemaRegistryPath = "/srcm/v2/clusters"
	ksqlPath           = "/ksqldbcm/v2/clusters"
	computePoolsPath   = "/fcpm/v2/compute-pools"
	connectorsBasePath = "/connect/v1/environments/%s/clusters/%s/connectors"
	defaultTimeout     = 60 * time.Second // Increase from 30s to 60s
	defaultPageSize    = 100
	maxRetries         = 3 // Add retry constant
)

// RateLimiter defines the interface for rate limiters
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// Client represents a Confluent Cloud API client
type Client struct {
	httpClient  *http.Client
	apiKey      string
	apiSecret   string
	logger      *slog.Logger
	rateLimiter RateLimiter
}

// Environment represents a Confluent Cloud environment
type Environment struct {
	ID   string `json:"id"`
	Name string `json:"display_name"`
}

// EnvironmentsResponse represents the response from the environments API
type EnvironmentsResponse struct {
	Data     []Environment `json:"data"`
	Metadata struct {
		Pagination struct {
			Total int    `json:"total"`
			Next  string `json:"next"`
		} `json:"pagination"`
	} `json:"metadata"`
}

// Resource represents a Confluent Cloud resource with metadata
type Resource struct {
	ID           string            `json:"id"`
	ResourceType string            `json:"resource_type"`
	Name         string            `json:"name,omitempty"`
	DisplayName  string            `json:"display_name,omitempty"`
	Labels       map[string]string `json:"labels"`
}

// RegionReference represents a complex region object
type RegionReference struct {
	ID           string `json:"id"`
	Related      string `json:"related,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
}

// KafkaClusterSpec represents the specification of a Kafka cluster
type KafkaClusterSpec struct {
	DisplayName string `json:"display_name,omitempty"`
	Cloud       string `json:"cloud,omitempty"`
	Region      string `json:"region,omitempty"`
}

// KafkaCluster represents a Kafka cluster
type KafkaCluster struct {
	ID   string           `json:"id"`
	Spec KafkaClusterSpec `json:"spec"`
	Name string           `json:"display_name,omitempty"`
}

// KafkaClustersResponse represents the response from the Kafka clusters API
type KafkaClustersResponse struct {
	Data     []KafkaCluster `json:"data"`
	Metadata struct {
		Pagination struct {
			Total int    `json:"total"`
			Next  string `json:"next"`
		} `json:"pagination"`
	} `json:"metadata"`
}

// SchemaRegistrySpec represents the specification of a Schema Registry instance
type SchemaRegistrySpec struct {
	DisplayName  string          `json:"display_name,omitempty"`
	Cloud        string          `json:"cloud,omitempty"`
	Region       RegionReference `json:"region"`
	Package      string          `json:"package,omitempty"`
	HttpEndpoint string          `json:"http_endpoint,omitempty"`
}

// SchemaRegistry represents a Schema Registry instance
type SchemaRegistry struct {
	ID   string             `json:"id"`
	Spec SchemaRegistrySpec `json:"spec"`
	Name string             `json:"display_name,omitempty"`
}

// SchemaRegistryResponse represents the response from the Schema Registry API
type SchemaRegistryResponse struct {
	Data     []SchemaRegistry `json:"data"`
	Metadata struct {
		Pagination struct {
			Total int    `json:"total"`
			Next  string `json:"next"`
		} `json:"pagination"`
	} `json:"metadata"`
}

// KafkaClusterReference represents a reference to a Kafka cluster
type KafkaClusterReference struct {
	ID           string `json:"id"`
	Environment  string `json:"environment,omitempty"`
	Related      string `json:"related,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
}

// KsqlDBSpec represents the specification of a KSQL database
type KsqlDBSpec struct {
	DisplayName  string                `json:"display_name,omitempty"`
	Cloud        string                `json:"cloud,omitempty"` // Not in API docs but keep for consistency
	KafkaCluster KafkaClusterReference `json:"kafka_cluster"`
	Csu          int                   `json:"csu,omitempty"`
}

// KsqlDB represents a KSQL database
type KsqlDB struct {
	ID   string     `json:"id"`
	Spec KsqlDBSpec `json:"spec"`
	Name string     `json:"display_name,omitempty"`
}

// KsqlDBResponse represents the response from the KSQL API
type KsqlDBResponse struct {
	Data     []KsqlDB `json:"data"`
	Metadata struct {
		Pagination struct {
			Total int    `json:"total"`
			Next  string `json:"next"`
		} `json:"pagination"`
	} `json:"metadata"`
}

// ComputePoolSpec represents the specification of a Compute Pool
type ComputePoolSpec struct {
	DisplayName string `json:"display_name,omitempty"`
	Cloud       string `json:"cloud,omitempty"`
	Region      string `json:"region,omitempty"`
	MaxCfu      int    `json:"max_cfu,omitempty"`
	MinCfu      int    `json:"min_cfu,omitempty"`
}

// ComputePool represents a Compute Pool
type ComputePool struct {
	ID   string          `json:"id"`
	Spec ComputePoolSpec `json:"spec"`
	Name string          `json:"display_name,omitempty"`
}

// ComputePoolsResponse represents the response from the Compute Pools API
type ComputePoolsResponse struct {
	Data     []ComputePool `json:"data"`
	Metadata struct {
		Pagination struct {
			Total int    `json:"total"`
			Next  string `json:"next"`
		} `json:"pagination"`
	} `json:"metadata"`
}

// Connector represents a connector
type Connector struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Class string `json:"class"`
}

// NewClient creates a new Confluent Cloud API client
func NewClient(apiKey, apiSecret string, rateLimiter RateLimiter) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		apiKey:      apiKey,
		apiSecret:   apiSecret,
		logger:      slog.Default(),
		rateLimiter: rateLimiter,
	}
}

// SetLogger sets a custom logger for the client
func (c *Client) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// createRequest creates an HTTP request with appropriate headers and auth
func (c *Client) createRequest(ctx context.Context, method, path string, queryParams map[string]string) (*http.Request, error) {
	// Build URL with query parameters
	reqURL, err := url.Parse(baseURL + path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add query parameters
	query := reqURL.Query()
	for key, value := range queryParams {
		query.Add(key, value)
	}
	reqURL.RawQuery = query.Encode()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(c.apiKey, c.apiSecret)
	return req, nil
}

// makeRequestWithRetry performs an HTTP request with retries and backoff
func (c *Client) makeRequestWithRetry(ctx context.Context, method, path string, queryParams map[string]string) ([]byte, error) {
	var respBody []byte

	// Define backoff policy for retries
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = 30 * time.Second

	operation := func() error {
		req, err := c.createRequest(ctx, method, path, queryParams) // Use queryParams here!
		if err != nil {
			return backoff.Permanent(fmt.Errorf("failed to create request: %w", err))
		}

		start := time.Now()
		resp, err := c.httpClient.Do(req)
		requestDuration := time.Since(start)

		if err != nil {
			// Network errors are generally retryable
			c.logger.Warn("Request failed",
				"method", method,
				"path", path,
				"duration_ms", requestDuration.Milliseconds(),
				"error", err)
			return err
		}
		defer resp.Body.Close()

		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			c.logger.Error("Failed to read response body",
				"method", method,
				"path", path,
				"status", resp.StatusCode,
				"error", err)
			return backoff.Permanent(fmt.Errorf("failed to read response: %w", err))
		}

		// Log the response details
		c.logger.Debug("API response",
			"method", method,
			"path", path,
			"status", resp.StatusCode,
			"duration_ms", requestDuration.Milliseconds(),
			"body_size_bytes", len(respBody))

		// Check status code
		if resp.StatusCode == http.StatusTooManyRequests {
			c.logger.Warn("Rate limit exceeded",
				"method", method,
				"path", path,
				"status", resp.StatusCode)
			return fmt.Errorf("rate limit exceeded (429): %s", string(respBody))
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.logger.Error("API returned error status",
				"method", method,
				"path", path,
				"status", resp.StatusCode,
				"response", string(respBody))
			return backoff.Permanent(fmt.Errorf("api error status %d: %s", resp.StatusCode, string(respBody)))
		}

		return nil
	}

	err := backoff.Retry(operation, expBackoff)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}

// Helper to determine if an error is retryable
func isRetryableError(err error) bool {
	// Retry on timeout and temporary network errors
	if err != nil {
		// Check for timeout errors
		if strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "deadline exceeded") {
			return true
		}

		// Check for connection errors
		if strings.Contains(err.Error(), "connection") ||
			strings.Contains(err.Error(), "reset by peer") {
			return true
		}

		// Check for certain HTTP status codes
		if strings.Contains(err.Error(), "429") || // Too Many Requests
			strings.Contains(err.Error(), "500") || // Internal Server Error
			strings.Contains(err.Error(), "502") || // Bad Gateway
			strings.Contains(err.Error(), "503") || // Service Unavailable
			strings.Contains(err.Error(), "504") { // Gateway Timeout
			return true
		}
	}

	return false
}

// GetEnvironments retrieves all environments from Confluent Cloud with pagination
func (c *Client) GetEnvironments(ctx context.Context) ([]Environment, error) {
	var allEnvironments []Environment
	nextPage := ""

	for {
		queryParams := make(map[string]string)
		queryParams["page_size"] = fmt.Sprintf("%d", defaultPageSize)

		if nextPage != "" {
			queryParams["page_token"] = nextPage
		}

		// Use the retry function instead
		body, err := c.makeRequestWithRetry(ctx, "GET", environmentsPath, queryParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get environments: %w", err)
		}

		var resp EnvironmentsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse environments response: %w", err)
		}

		allEnvironments = append(allEnvironments, resp.Data...)

		// Check if there are more pages
		nextPage = resp.Metadata.Pagination.Next
		if nextPage == "" {
			break
		}
	}

	return allEnvironments, nil
}

// GetKafkaClusters retrieves all Kafka clusters for a specific environment with pagination
func (c *Client) GetKafkaClusters(ctx context.Context, envID string) ([]KafkaCluster, error) {
	var allClusters []KafkaCluster
	nextPage := ""

	for {
		queryParams := make(map[string]string)
		queryParams["page_size"] = fmt.Sprintf("%d", defaultPageSize)
		queryParams["environment"] = envID

		if nextPage != "" {
			queryParams["page_token"] = nextPage
		}

		body, err := c.makeRequestWithRetry(ctx, "GET", kafkaClustersPath, queryParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get Kafka clusters: %w", err)
		}

		var resp KafkaClustersResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse Kafka clusters response: %w", err)
		}

		allClusters = append(allClusters, resp.Data...)

		// Check if there are more pages
		nextPage = resp.Metadata.Pagination.Next
		if nextPage == "" {
			break
		}
	}

	return allClusters, nil
}

// GetSchemaRegistries retrieves all Schema Registry instances for a specific environment with pagination
func (c *Client) GetSchemaRegistries(ctx context.Context, envID string) ([]SchemaRegistry, error) {
	var allRegistries []SchemaRegistry
	nextPage := ""

	for {
		queryParams := make(map[string]string)
		queryParams["page_size"] = fmt.Sprintf("%d", defaultPageSize)
		queryParams["environment"] = envID

		if nextPage != "" {
			queryParams["page_token"] = nextPage
		}

		body, err := c.makeRequestWithRetry(ctx, "GET", schemaRegistryPath, queryParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get Schema Registry instances: %w", err)
		}

		var resp SchemaRegistryResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse Schema Registry response: %w", err)
		}

		allRegistries = append(allRegistries, resp.Data...)

		// Check if there are more pages
		nextPage = resp.Metadata.Pagination.Next
		if nextPage == "" {
			break
		}
	}

	return allRegistries, nil
}

// GetKsqlDBs retrieves all KSQL databases for a specific environment with pagination
func (c *Client) GetKsqlDBs(ctx context.Context, envID string) ([]KsqlDB, error) {
	var allKsqlDBs []KsqlDB
	nextPage := ""

	for {
		queryParams := make(map[string]string)
		queryParams["page_size"] = fmt.Sprintf("%d", defaultPageSize)
		queryParams["environment"] = envID

		if nextPage != "" {
			queryParams["page_token"] = nextPage
		}

		body, err := c.makeRequestWithRetry(ctx, "GET", ksqlPath, queryParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get KSQL databases: %w", err)
		}

		var resp KsqlDBResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse KSQL databases response: %w", err)
		}

		allKsqlDBs = append(allKsqlDBs, resp.Data...)

		// Check if there are more pages
		nextPage = resp.Metadata.Pagination.Next
		if nextPage == "" {
			break
		}
	}

	return allKsqlDBs, nil
}

// GetComputePools retrieves all Compute Pools for a specific environment with pagination
func (c *Client) GetComputePools(ctx context.Context, envID string) ([]ComputePool, error) {
	var allComputePools []ComputePool
	nextPage := ""

	for {
		queryParams := make(map[string]string)
		queryParams["page_size"] = fmt.Sprintf("%d", defaultPageSize)
		queryParams["environment"] = envID

		if nextPage != "" {
			queryParams["page_token"] = nextPage
		}

		body, err := c.makeRequestWithRetry(ctx, "GET", computePoolsPath, queryParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get Compute Pools: %w", err)
		}

		var resp ComputePoolsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse Compute Pools response: %w", err)
		}

		allComputePools = append(allComputePools, resp.Data...)

		// Check if there are more pages
		nextPage = resp.Metadata.Pagination.Next
		if nextPage == "" {
			break
		}
	}

	return allComputePools, nil
}

// GetConnectors retrieves connectors with their internal IDs for a specific environment and cluster
func (c *Client) GetConnectors(ctx context.Context, envID, clusterID string) ([]Connector, error) {
	// Add expand parameter to get detailed info including IDs
	path := fmt.Sprintf(connectorsBasePath+"?expand=info,status,id", envID, clusterID)

	body, err := c.makeRequestWithRetry(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get connectors: %w", err)
	}

	// The detailed API response is a map of connector names to connector details
	var connectorsDetails map[string]struct {
		ID struct {
			ID     string `json:"id"`
			IDType string `json:"id_type"`
		} `json:"id"`
		Info struct {
			Name   string `json:"name"`
			Type   string `json:"type"` // "sink" or "source"
			Config struct {
				ConnectorClass string `json:"connector.class"`
			} `json:"config"`
		} `json:"info"`
	}

	if err := json.Unmarshal(body, &connectorsDetails); err != nil {
		// If unmarshal fails, try the old format (just names)
		var connectorNames []string
		if err2 := json.Unmarshal(body, &connectorNames); err2 == nil {
			// Fall back to the old behavior
			c.logger.Debug("API returned simple connector names, no internal IDs available")

			connectors := make([]Connector, len(connectorNames))
			for i, name := range connectorNames {
				connectors[i] = Connector{
					Name: name,
					ID:   name, // Use name as ID
				}
			}

			return connectors, nil
		}

		// Both unmarshaling attempts failed
		return nil, fmt.Errorf("failed to parse connectors response: %w", err)
	}

	// Convert the detailed response to Connector objects with internal IDs
	connectors := make([]Connector, 0, len(connectorsDetails))
	for name, details := range connectorsDetails {
		id := name // Default to using name as ID

		// Use internal ID if available
		if details.ID.ID != "" {
			id = details.ID.ID
			c.logger.Debug("Using internal connector ID", "name", name, "id", id)
		}

		connector := Connector{
			Name:  name,
			ID:    id,
			Type:  details.Info.Type,                  // "sink" or "source"
			Class: details.Info.Config.ConnectorClass, // Actual connector class
		}

		connectors = append(connectors, connector)
	}

	return connectors, nil
}

// Extract region ID from a complex region object
func extractRegionID(regionObj interface{}) string {
	// Check if the region is a string (for backward compatibility)
	if region, ok := regionObj.(string); ok {
		return region
	}

	// Check if the region is a map (complex object) with an ID field
	if regionMap, ok := regionObj.(map[string]interface{}); ok {
		if id, ok := regionMap["id"].(string); ok {
			return id
		}
	}

	return "unknown"
}

// GetAllResources fetches all resources and formats them with consistent metadata
func (c *Client) GetAllResources(ctx context.Context, targetEnvIDs []string) ([]Resource, error) {
	var allResources []Resource
	var resourcesMutex sync.Mutex // To protect concurrent writes to allResources

	// Get all environments
	environments, err := c.GetEnvironments(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get environments: %w", err)
	}

	// Filter environments if targetEnvIDs is specified
	filteredEnvs := environments
	if len(targetEnvIDs) > 0 {
		targetMap := make(map[string]bool)
		for _, id := range targetEnvIDs {
			targetMap[id] = true
		}

		filteredEnvs = []Environment{}
		for _, env := range environments {
			if targetMap[env.ID] {
				filteredEnvs = append(filteredEnvs, env)
			}
		}
	}

	c.logger.Info("Discovered environments", "count", len(filteredEnvs))

	// Create an error group for controlled parallelism
	g, gctx := errgroup.WithContext(ctx)
	// Limit concurrency to avoid overwhelming the API
	semaphore := make(chan struct{}, 3) // Limit to 3 concurrent requests per environment

	// Process each environment
	for _, env := range filteredEnvs {
		env := env // Capture for goroutine

		// Add environment itself as a resource (do this synchronously)
		resourcesMutex.Lock()
		allResources = append(allResources, Resource{
			ID:           env.ID,
			ResourceType: "environment",
			Name:         env.Name,
			DisplayName:  env.Name,
			Labels:       make(map[string]string),
		})
		resourcesMutex.Unlock()

		// Process Kafka clusters in parallel
		g.Go(func() error {
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore when done

			// Get Kafka clusters
			kafkaClusters, err := c.GetKafkaClusters(gctx, env.ID)
			if err != nil {
				c.logger.Warn("Failed to get Kafka clusters",
					"environment_id", env.ID,
					"environment_name", env.Name,
					"error", err)
				return nil // Continue with other resources
			}

			// Process each Kafka cluster
			var clusterResources []Resource
			for _, cluster := range kafkaClusters {
				r := Resource{
					ID:           cluster.ID,
					ResourceType: "kafka",
					Name:         cluster.Name,
					DisplayName:  cluster.Spec.DisplayName,
					Labels: map[string]string{
						"environment_id":   env.ID,
						"environment_name": env.Name,
						"cloud":            cluster.Spec.Cloud,
						"region":           cluster.Spec.Region,
					},
				}
				clusterResources = append(clusterResources, r)

				// Process connectors for this cluster in a separate goroutine
				g.Go(func() error {
					semaphore <- struct{}{}        // Acquire semaphore
					defer func() { <-semaphore }() // Release semaphore when done

					connectors, err := c.GetConnectors(gctx, env.ID, cluster.ID)
					if err != nil {
						c.logger.Warn("Failed to get connectors",
							"environment_id", env.ID,
							"cluster_id", cluster.ID,
							"error", err)
						return nil
					}

					var connectorResources []Resource
					for _, connector := range connectors {
						connectorResources = append(connectorResources, Resource{
							ID:           connector.ID,
							ResourceType: "connector",
							Name:         connector.Name,
							DisplayName:  connector.Name,
							Labels: map[string]string{
								"environment_id":   env.ID,
								"environment_name": env.Name,
								"cluster_id":       cluster.ID,
								"connector_type":   connector.Type,
							},
						})
					}

					// Add all connector resources at once
					if len(connectorResources) > 0 {
						resourcesMutex.Lock()
						allResources = append(allResources, connectorResources...)
						resourcesMutex.Unlock()
					}

					return nil
				})
			}

			// Add all Kafka resources at once
			if len(clusterResources) > 0 {
				resourcesMutex.Lock()
				allResources = append(allResources, clusterResources...)
				resourcesMutex.Unlock()
			}

			return nil
		})

		// Process Schema Registry in parallel
		g.Go(func() error {
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore when done

			schemaRegistries, err := c.GetSchemaRegistries(gctx, env.ID)
			if err != nil {
				c.logger.Warn("Failed to get Schema Registry instances",
					"environment_id", env.ID,
					"environment_name", env.Name,
					"error", err)
				return nil
			}

			var registryResources []Resource
			for _, registry := range schemaRegistries {
				registryResources = append(registryResources, Resource{
					ID:           registry.ID,
					ResourceType: "schema_registry",
					Name:         registry.Name,
					DisplayName:  registry.Spec.DisplayName,
					Labels: map[string]string{
						"environment_id":   env.ID,
						"environment_name": env.Name,
						"cloud":            registry.Spec.Cloud,
						"region":           registry.Spec.Region.ID,
						"package":          registry.Spec.Package,
					},
				})
			}

			if len(registryResources) > 0 {
				resourcesMutex.Lock()
				allResources = append(allResources, registryResources...)
				resourcesMutex.Unlock()
			}

			return nil
		})

		// Process Compute Pools in parallel
		g.Go(func() error {
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore when done

			computePools, err := c.GetComputePools(gctx, env.ID)
			if err != nil {
				c.logger.Warn("Failed to get Compute Pools",
					"environment_id", env.ID,
					"environment_name", env.Name,
					"error", err)
				return nil
			}

			var poolResources []Resource
			for _, pool := range computePools {
				poolResources = append(poolResources, Resource{
					ID:           pool.ID,
					ResourceType: "compute_pool",
					Name:         pool.Name,
					DisplayName:  pool.Spec.DisplayName,
					Labels: map[string]string{
						"environment_id":   env.ID,
						"environment_name": env.Name,
						"cloud":            pool.Spec.Cloud,
						"region":           pool.Spec.Region,
						"max_cfu":          fmt.Sprintf("%d", pool.Spec.MaxCfu),
						"min_cfu":          fmt.Sprintf("%d", pool.Spec.MinCfu),
					},
				})
			}

			if len(poolResources) > 0 {
				resourcesMutex.Lock()
				allResources = append(allResources, poolResources...)
				resourcesMutex.Unlock()
			}

			return nil
		})

		// Also add KsqlDB processing in parallel
		g.Go(func() error {
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore when done

			ksqlDBs, err := c.GetKsqlDBs(gctx, env.ID)
			if err != nil {
				c.logger.Warn("Failed to get KSQL databases",
					"environment_id", env.ID,
					"environment_name", env.Name,
					"error", err)
				return nil
			}

			var ksqlResources []Resource
			for _, ksqlDB := range ksqlDBs {
				ksqlResources = append(ksqlResources, Resource{
					ID:           ksqlDB.ID,
					ResourceType: "ksqldb",
					Name:         ksqlDB.Name,
					DisplayName:  ksqlDB.Spec.DisplayName,
					Labels: map[string]string{
						"environment_id":   env.ID,
						"environment_name": env.Name,
						"kafka_cluster_id": ksqlDB.Spec.KafkaCluster.ID,
						"csu":              fmt.Sprintf("%d", ksqlDB.Spec.Csu),
						"cloud":            ksqlDB.Spec.Cloud, // Include if available
					},
				})
			}

			if len(ksqlResources) > 0 {
				resourcesMutex.Lock()
				allResources = append(allResources, ksqlResources...)
				resourcesMutex.Unlock()
			}

			return nil
		})
	}

	// Wait for all goroutines to complete
	if err := g.Wait(); err != nil {
		return allResources, fmt.Errorf("error during parallel resource discovery: %w", err)
	}

	// Add before returning
	resourceCounts := make(map[string]int)
	for _, res := range allResources {
		resourceCounts[res.ResourceType]++
	}

	c.logger.Info("Resource discovery completed",
		"total_resources", len(allResources),
		"resource_counts", fmt.Sprintf("%v", resourceCounts))

	return allResources, nil
}
