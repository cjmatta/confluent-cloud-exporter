package confluent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	baseURL            = "https://api.confluent.cloud"
	environmentsPath   = "/org/v2/environments"
	kafkaClustersPath  = "/cmk/v2/clusters"
	schemaRegistryPath = "/srcm/v2/clusters"
	ksqlPath           = "/ksqldbcm/v2/clusters"
	computePoolsPath   = "/fcpm/v2/compute-pools"
	connectorsBasePath = "/connect/v1/environments/%s/clusters/%s/connectors"
	defaultTimeout     = 30 * time.Second
	defaultPageSize    = 100
)

// Client represents a Confluent Cloud API client
type Client struct {
	httpClient *http.Client
	apiKey     string
	apiSecret  string
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
	Labels       map[string]string `json:"labels"`
}

// KafkaClusterSpec represents the specification of a Kafka cluster
type KafkaClusterSpec struct {
	DisplayName  string `json:"display_name"`
	Availability string `json:"availability"`
	Cloud        string `json:"cloud"`
	Region       string `json:"region"`
}

// KafkaCluster represents a Kafka cluster
type KafkaCluster struct {
	ID          string           `json:"id"`
	Spec        KafkaClusterSpec `json:"spec"`
	Environment struct {
		ID string `json:"id"`
	} `json:"environment"`
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
	DisplayName string                 `json:"display_name"`
	Cloud       string                 `json:"cloud"`
	Region      map[string]interface{} `json:"region"` // Region can be a complex object, not just string
	Package     string                 `json:"package,omitempty"`
}

// SchemaRegistry represents a Schema Registry instance
type SchemaRegistry struct {
	ID          string             `json:"id"`
	Spec        SchemaRegistrySpec `json:"spec"`
	Environment struct {
		ID string `json:"id"`
	} `json:"environment"`
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

// KsqlDBSpec represents the specification of a KSQL database
type KsqlDBSpec struct {
	DisplayName string `json:"display_name"`
	Cloud       string `json:"cloud"`
	Region      string `json:"region"`
}

// KsqlDB represents a KSQL database
type KsqlDB struct {
	ID          string     `json:"id"`
	Spec        KsqlDBSpec `json:"spec"`
	Environment struct {
		ID string `json:"id"`
	} `json:"environment"`
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

// ComputePoolSpec represents the specification of a compute pool
type ComputePoolSpec struct {
	DisplayName string `json:"display_name"`
	Cloud       string `json:"cloud"`
	Region      string `json:"region"`
}

// ComputePool represents a compute pool
type ComputePool struct {
	ID          string          `json:"id"`
	Spec        ComputePoolSpec `json:"spec"`
	Environment struct {
		ID string `json:"id"`
	} `json:"environment"`
}

// ComputePoolsResponse represents the response from the compute pools API
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
	ID          string `json:"name"`
	ClusterID   string `json:"cluster_id"`
	Environment string `json:"environment_id"`
}

// NewClient creates a new Confluent Cloud API client
func NewClient(apiKey, apiSecret string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		apiKey:    apiKey,
		apiSecret: apiSecret,
	}
}

// makeRequest performs an HTTP request and returns the response body
func (c *Client) makeRequest(method, path string, queryParams map[string]string) ([]byte, error) {
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

	// Create request
	req, err := http.NewRequest(method, reqURL.String(), nil)
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

	// Read response body - using io.ReadAll instead of ioutil.ReadAll
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

// GetEnvironments retrieves all environments from Confluent Cloud with pagination
func (c *Client) GetEnvironments() ([]Environment, error) {
	log.Println("Fetching environments from Confluent Cloud API")

	var allEnvironments []Environment
	nextPageToken := ""

	for {
		// Prepare query parameters
		queryParams := map[string]string{
			"page_size": fmt.Sprintf("%d", defaultPageSize),
		}

		if nextPageToken != "" {
			queryParams["page_token"] = nextPageToken
		}

		// Make request
		body, err := c.makeRequest(http.MethodGet, environmentsPath, queryParams)
		if err != nil {
			return nil, err
		}

		// Parse response
		var envResp EnvironmentsResponse
		if err := json.Unmarshal(body, &envResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Add environments to results
		allEnvironments = append(allEnvironments, envResp.Data...)

		// Check if there are more pages
		if envResp.Metadata.Pagination.Next == "" {
			break
		}

		// Set next page token
		nextPageToken = envResp.Metadata.Pagination.Next
		log.Printf("Fetching next page of environments with token: %s", nextPageToken)
	}

	log.Printf("Found %d total environments", len(allEnvironments))
	return allEnvironments, nil
}

// GetKafkaClusters retrieves all Kafka clusters for a specific environment with pagination
func (c *Client) GetKafkaClusters(environmentID string) ([]KafkaCluster, error) {
	log.Printf("Fetching Kafka clusters for environment %s", environmentID)

	var allClusters []KafkaCluster
	nextPageToken := ""

	for {
		// Prepare query parameters
		queryParams := map[string]string{
			"environment": environmentID,
			"page_size":   fmt.Sprintf("%d", defaultPageSize),
		}

		if nextPageToken != "" {
			queryParams["page_token"] = nextPageToken
		}

		// Make request
		body, err := c.makeRequest(http.MethodGet, kafkaClustersPath, queryParams)
		if err != nil {
			return nil, err
		}

		// Parse response
		var clustersResp KafkaClustersResponse
		if err := json.Unmarshal(body, &clustersResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Add clusters to results
		allClusters = append(allClusters, clustersResp.Data...)

		// Check if there are more pages
		if clustersResp.Metadata.Pagination.Next == "" {
			break
		}

		// Set next page token
		nextPageToken = clustersResp.Metadata.Pagination.Next
		log.Printf("Fetching next page of Kafka clusters with token: %s", nextPageToken)
	}

	log.Printf("Found %d total Kafka clusters for environment %s", len(allClusters), environmentID)
	return allClusters, nil
}

// GetSchemaRegistries retrieves all Schema Registry instances for a specific environment with pagination
func (c *Client) GetSchemaRegistries(environmentID string) ([]SchemaRegistry, error) {
	log.Printf("Fetching Schema Registry instances for environment %s", environmentID)

	var allSchemaRegistries []SchemaRegistry
	nextPageToken := ""

	for {
		// Prepare query parameters
		queryParams := map[string]string{
			"environment": environmentID,
			"page_size":   fmt.Sprintf("%d", defaultPageSize),
		}

		if nextPageToken != "" {
			queryParams["page_token"] = nextPageToken
		}

		// Make request
		body, err := c.makeRequest(http.MethodGet, schemaRegistryPath, queryParams)
		if err != nil {
			return nil, err
		}

		// Parse response
		var srResp SchemaRegistryResponse
		if err := json.Unmarshal(body, &srResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Add schema registries to results
		allSchemaRegistries = append(allSchemaRegistries, srResp.Data...)

		// Check if there are more pages
		if srResp.Metadata.Pagination.Next == "" {
			break
		}

		// Set next page token
		nextPageToken = srResp.Metadata.Pagination.Next
		log.Printf("Fetching next page of Schema Registry instances with token: %s", nextPageToken)
	}

	log.Printf("Found %d total Schema Registry instances for environment %s", len(allSchemaRegistries), environmentID)
	return allSchemaRegistries, nil
}

// GetKsqlDBs retrieves all KSQL databases for a specific environment with pagination
func (c *Client) GetKsqlDBs(environmentID string) ([]KsqlDB, error) {
	log.Printf("Fetching KSQL databases for environment %s", environmentID)

	var allKsqlDBs []KsqlDB
	nextPageToken := ""

	for {
		// Prepare query parameters
		queryParams := map[string]string{
			"environment": environmentID,
			"page_size":   fmt.Sprintf("%d", defaultPageSize),
		}

		if nextPageToken != "" {
			queryParams["page_token"] = nextPageToken
		}

		// Make request
		body, err := c.makeRequest(http.MethodGet, ksqlPath, queryParams)
		if err != nil {
			return nil, err
		}

		// Parse response
		var ksqlResp KsqlDBResponse
		if err := json.Unmarshal(body, &ksqlResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Add ksql databases to results
		allKsqlDBs = append(allKsqlDBs, ksqlResp.Data...)

		// Check if there are more pages
		if ksqlResp.Metadata.Pagination.Next == "" {
			break
		}

		// Set next page token
		nextPageToken = ksqlResp.Metadata.Pagination.Next
		log.Printf("Fetching next page of KSQL databases with token: %s", nextPageToken)
	}

	log.Printf("Found %d total KSQL databases for environment %s", len(allKsqlDBs), environmentID)
	return allKsqlDBs, nil
}

// GetComputePools retrieves all compute pools for a specific environment with pagination
func (c *Client) GetComputePools(environmentID string) ([]ComputePool, error) {
	log.Printf("Fetching compute pools for environment %s", environmentID)

	var allComputePools []ComputePool
	nextPageToken := ""

	for {
		// Prepare query parameters
		queryParams := map[string]string{
			"environment": environmentID,
			"page_size":   fmt.Sprintf("%d", defaultPageSize),
		}

		if nextPageToken != "" {
			queryParams["page_token"] = nextPageToken
		}

		// Make request
		body, err := c.makeRequest(http.MethodGet, computePoolsPath, queryParams)
		if err != nil {
			return nil, err
		}

		// Parse response
		var poolsResp ComputePoolsResponse
		if err := json.Unmarshal(body, &poolsResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Add compute pools to results
		allComputePools = append(allComputePools, poolsResp.Data...)

		// Check if there are more pages
		if poolsResp.Metadata.Pagination.Next == "" {
			break
		}

		// Set next page token
		nextPageToken = poolsResp.Metadata.Pagination.Next
		log.Printf("Fetching next page of compute pools with token: %s", nextPageToken)
	}

	log.Printf("Found %d total compute pools for environment %s", len(allComputePools), environmentID)
	return allComputePools, nil
}

// GetConnectors retrieves connectors for a specific environment and cluster
// Note: The connector API might not use the same pagination mechanism
func (c *Client) GetConnectors(environmentID, clusterID string) ([]Connector, error) {
	log.Printf("Fetching connectors for environment %s, cluster %s", environmentID, clusterID)

	path := fmt.Sprintf(connectorsBasePath, environmentID, clusterID)
	body, err := c.makeRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var connectorNames []string
	if err := json.Unmarshal(body, &connectorNames); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Convert connector names to connector objects
	connectors := make([]Connector, len(connectorNames))
	for i, name := range connectorNames {
		connectors[i] = Connector{
			ID:          name,
			ClusterID:   clusterID,
			Environment: environmentID,
		}
	}

	log.Printf("Found %d connectors for environment %s, cluster %s", len(connectors), environmentID, clusterID)
	return connectors, nil
}

// GetAllResources fetches all resources and formats them with consistent metadata
func (c *Client) GetAllResources() ([]Resource, error) {
	var resources []Resource

	// Fetch environments with pagination
	environments, err := c.GetEnvironments()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch environments: %w", err)
	}

	// Create a map of environment IDs to names for easier lookup
	envMap := make(map[string]string)
	for _, env := range environments {
		envMap[env.ID] = env.Name
	}

	// Process each environment separately
	for _, env := range environments {
		log.Printf("Processing environment: %s (%s)", env.Name, env.ID)

		// Fetch Kafka clusters for this environment with pagination
		kafkaClusters, err := c.GetKafkaClusters(env.ID)
		if err != nil {
			log.Printf("Warning: failed to fetch Kafka clusters for environment %s: %v", env.ID, err)
		} else {
			for _, cluster := range kafkaClusters {
				// Map cloud provider from cloud field
				cloudProvider := cluster.Spec.Cloud
				if cloudProvider == "" {
					cloudProvider = "unknown"
				}

				resources = append(resources, Resource{
					ID:           cluster.ID,
					ResourceType: "kafka",
					Labels: map[string]string{
						"cloud_provider":   cloudProvider,
						"environment_name": env.Name,
						"cluster_name":     cluster.Spec.DisplayName,
						"region":           cluster.Spec.Region,
					},
				})

				// Fetch connectors for this Kafka cluster
				connectors, err := c.GetConnectors(env.ID, cluster.ID)
				if err != nil {
					log.Printf("Warning: failed to fetch connectors for environment %s, cluster %s: %v",
						env.ID, cluster.ID, err)
				} else {
					for _, connector := range connectors {
						resources = append(resources, Resource{
							ID:           connector.ID,
							ResourceType: "connector",
							Labels: map[string]string{
								"cloud_provider":   cloudProvider, // Use cluster's provider
								"environment_name": env.Name,
								"connector_name":   connector.ID,
								"cluster_id":       connector.ClusterID,
								"region":           cluster.Spec.Region,
							},
						})
					}
				}
			}
		}

		// Fetch Schema Registry instances for this environment with pagination
		schemaRegistries, err := c.GetSchemaRegistries(env.ID)
		if err != nil {
			log.Printf("Warning: failed to fetch Schema Registry instances for environment %s: %v", env.ID, err)
		} else {
			for _, sr := range schemaRegistries {
				// Map cloud provider from cloud field
				cloudProvider := sr.Spec.Cloud
				if cloudProvider == "" {
					cloudProvider = "unknown"
				}

				// Extract region information safely
				var regionStr string
				if regionVal, ok := sr.Spec.Region["id"]; ok {
					if regionStr, ok = regionVal.(string); !ok {
						regionStr = "unknown"
					}
				} else {
					regionStr = "unknown"
				}

				// Create labels map
				labels := map[string]string{
					"cloud_provider":   cloudProvider,
					"environment_name": env.Name,
					"name":             sr.Spec.DisplayName,
					"region":           regionStr,
				}

				// Add package if available
				if sr.Spec.Package != "" {
					labels["package"] = sr.Spec.Package
				}

				resources = append(resources, Resource{
					ID:           sr.ID,
					ResourceType: "schema_registry",
					Labels:       labels,
				})
			}
		}

		// Fetch KSQL databases for this environment with pagination
		ksqlDBs, err := c.GetKsqlDBs(env.ID)
		if err != nil {
			log.Printf("Warning: failed to fetch KSQL databases for environment %s: %v", env.ID, err)
		} else {
			for _, ksql := range ksqlDBs {
				// Map cloud provider from cloud field
				cloudProvider := ksql.Spec.Cloud
				if cloudProvider == "" {
					cloudProvider = "unknown"
				}

				resources = append(resources, Resource{
					ID:           ksql.ID,
					ResourceType: "ksql",
					Labels: map[string]string{
						"cloud_provider":   cloudProvider,
						"environment_name": env.Name,
						"name":             ksql.Spec.DisplayName,
						"region":           ksql.Spec.Region,
					},
				})
			}
		}

		// Fetch compute pools for this environment with pagination
		computePools, err := c.GetComputePools(env.ID)
		if err != nil {
			log.Printf("Warning: failed to fetch compute pools for environment %s: %v", env.ID, err)
		} else {
			for _, pool := range computePools {
				// Map cloud provider from cloud field
				cloudProvider := pool.Spec.Cloud
				if cloudProvider == "" {
					cloudProvider = "unknown"
				}

				resources = append(resources, Resource{
					ID:           pool.ID,
					ResourceType: "compute_pool",
					Labels: map[string]string{
						"cloud_provider":   cloudProvider,
						"environment_name": env.Name,
						"name":             pool.Spec.DisplayName,
						"region":           pool.Spec.Region,
					},
				})
			}
		}
	}

	log.Printf("Found %d total resources across %d environments", len(resources), len(environments))
	return resources, nil
}
