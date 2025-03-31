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

// Client represents a Confluent Cloud API client
type Client struct {
	httpClient *http.Client
	apiKey     string
	apiSecret  string
	logger     *slog.Logger
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
	ID   string `json:"id,omitempty"`
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// NewClient creates a new Confluent Cloud API client
func NewClient(apiKey, apiSecret string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		apiKey:    apiKey,
		apiSecret: apiSecret,
		logger:    slog.Default(),
	}
}

// SetLogger sets a custom logger for the client
func (c *Client) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// makeRequest performs an HTTP request and returns the response body
func (c *Client) makeRequest(ctx context.Context, method, path string, queryParams map[string]string) ([]byte, error) {
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

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned non-200 status: %d, body: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

// makeRequestWithRetry performs an HTTP request with retries and backoff
func (c *Client) makeRequestWithRetry(ctx context.Context, method, path string, queryParams map[string]string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// If not the first attempt, add a backoff delay
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s...
			backoffTime := time.Duration(1<<uint(attempt-1)) * time.Second
			c.logger.Debug("Retrying request after backoff",
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"backoff_time", backoffTime.String(),
				"path", path)

			// Create a timer for the backoff
			timer := time.NewTimer(backoffTime)

			// Wait for either the backoff timer or context cancellation
			select {
			case <-timer.C:
				// Backoff completed, continue with retry
			case <-ctx.Done():
				// Context was canceled during backoff
				timer.Stop()
				return nil, ctx.Err()
			}
		}

		// Create a child context with a timeout for this specific request
		reqCtx, cancel := context.WithTimeout(ctx, defaultTimeout)

		// Make the request with the child context
		body, err := c.makeRequest(reqCtx, method, path, queryParams)

		// Always cancel the child context to prevent resource leaks
		cancel()

		// If successful, return the result
		if err == nil {
			return body, nil
		}

		// Log the error
		c.logger.Debug("Request failed, may retry",
			"attempt", attempt+1,
			"max_attempts", maxRetries,
			"path", path,
			"error", err)

		// Save the error for potential return
		lastErr = err

		// Don't retry if context is already canceled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Don't retry certain errors
		if !isRetryableError(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, lastErr)
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

		// Also check specifically the structure of the 'region' field
		var rawResponse map[string]interface{}
		if err := json.Unmarshal(body, &rawResponse); err == nil {
			if data, ok := rawResponse["data"].([]interface{}); ok && len(data) > 0 {
				if item, ok := data[0].(map[string]interface{}); ok {
					if spec, ok := item["spec"].(map[string]interface{}); ok {
						c.logger.Debug("Schema Registry region field structure",
							"region_type", fmt.Sprintf("%T", spec["region"]),
							"region_value", fmt.Sprintf("%v", spec["region"]))
					}
				}
			}
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

// GetConnectors retrieves connectors for a specific environment and cluster
func (c *Client) GetConnectors(ctx context.Context, envID, clusterID string) ([]Connector, error) {
	path := fmt.Sprintf(connectorsBasePath, envID, clusterID)

	body, err := c.makeRequestWithRetry(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get connectors: %w", err)
	}

	// The connector API returns a different format - just a list of connector names
	var connectorNames []string
	if err := json.Unmarshal(body, &connectorNames); err != nil {
		return nil, fmt.Errorf("failed to parse connectors response: %w", err)
	}

	// Convert connector names to Connector objects
	connectors := make([]Connector, len(connectorNames))
	for i, name := range connectorNames {
		connectors[i] = Connector{
			Name: name,
			ID:   name, // Use name as ID if no separate ID is available
		}

		// Try to determine connector type from name (a common pattern is "type-name")
		parts := strings.SplitN(name, "-", 2)
		if len(parts) > 1 {
			connectors[i].Type = parts[0]
		}
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
	semaphore := make(chan struct{}, 5) // Limit to 5 concurrent requests per environment

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
