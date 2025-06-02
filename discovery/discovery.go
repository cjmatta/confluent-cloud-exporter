package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"confluent-cloud-exporter/confluent"
)

// DiscoveredResource represents a resource discovered in Confluent Cloud
type DiscoveredResource struct {
	ID              string            // Resource identifier
	Type            string            // Resource type (e.g., "kafka", "schema_registry")
	Name            string            // Human-readable name
	EnvironmentID   string            // Environment identifier
	EnvironmentName string            // Human-readable environment name
	Labels          map[string]string // Additional metadata as key-value pairs
}

// ResourceProvider defines the interface for components that provide discovered resources
type ResourceProvider interface {
	// GetResources returns the current list of discovered resources
	GetResources() []DiscoveredResource
	// RefreshResources discovers resources from Confluent Cloud
	RefreshResources(ctx context.Context) error
	// IsReady indicates if the discovery service has successfully completed at least one refresh
	IsReady() bool
}

// DiscoveryService is responsible for discovering and caching Confluent Cloud resources
// Compile-time check to ensure DiscoveryService implements ResourceProvider
var _ ResourceProvider = (*DiscoveryService)(nil)

type DiscoveryService struct {
	client              *confluent.Client
	targetEnvIDs        []string
	logger              *slog.Logger
	mutex               sync.RWMutex
	discoveredResources []DiscoveredResource
	lastRefreshTime     time.Time
	lastRefreshError    error
}

// NewDiscoveryService creates a new discovery service
func NewDiscoveryService(apiKey, apiSecret string, targetEnvIDs []string, logger *slog.Logger, rateLimiter *confluent.AdaptiveRateLimiter) (*DiscoveryService, error) {
	// Create Confluent client
	client := confluent.NewClient(apiKey, apiSecret, rateLimiter)
	client.SetLogger(logger)

	return &DiscoveryService{
		client:              client,
		targetEnvIDs:        targetEnvIDs,
		logger:              logger,
		discoveredResources: make([]DiscoveredResource, 0),
	}, nil
}

// GetResources implements the ResourceProvider interface
func (d *DiscoveryService) GetResources() []DiscoveredResource {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Return a copy of the resources to prevent races
	resources := make([]DiscoveredResource, len(d.discoveredResources))
	copy(resources, d.discoveredResources)

	return resources
}

// RefreshResources discovers resources from Confluent Cloud
func (d *DiscoveryService) RefreshResources(ctx context.Context) error {
	startTime := time.Now()
	d.logger.Info("Starting resource discovery")

	// Fetch all resources in a single unified call - WITHOUT holding the lock
	// This is the expensive operation that should not block GetResources() calls
	resources, err := d.client.GetAllResources(ctx, d.targetEnvIDs)
	if err != nil {
		// Update error state under lock
		d.mutex.Lock()
		d.lastRefreshError = fmt.Errorf("failed to discover resources: %w", err)
		d.mutex.Unlock()

		d.logger.Error("Resource discovery failed", "error", err)
		return d.lastRefreshError
	}

	// Convert to our internal format - also done outside the lock
	discoveredResources := make([]DiscoveredResource, 0, len(resources))
	resourceCounts := make(map[string]int)

	for _, res := range resources {
		dr := DiscoveredResource{
			ID:              res.ID,
			Type:            res.ResourceType,
			Name:            res.DisplayName,
			EnvironmentID:   res.Labels["environment_id"],
			EnvironmentName: res.Labels["environment_name"],
			Labels:          res.Labels,
		}

		// If display name is empty, use the name or ID
		if dr.Name == "" {
			if res.Name != "" {
				dr.Name = res.Name
			} else {
				dr.Name = res.ID
			}
		}

		discoveredResources = append(discoveredResources, dr)
		resourceCounts[res.ResourceType]++
	}

	// Now acquire the lock ONLY to update the cache atomically
	d.mutex.Lock()
	d.discoveredResources = discoveredResources
	d.lastRefreshTime = time.Now()
	d.lastRefreshError = nil
	d.mutex.Unlock()

	// Log discovery results
	d.logger.Info("Resource discovery completed",
		"duration", time.Since(startTime).String(),
		"total_resources", len(discoveredResources),
		"resource_counts", resourceCounts)

	return nil
}

// GetLastRefreshTime returns the timestamp of the last successful refresh
func (d *DiscoveryService) GetLastRefreshTime() time.Time {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.lastRefreshTime
}

// GetLastRefreshError returns the error from the last refresh attempt
func (d *DiscoveryService) GetLastRefreshError() error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.lastRefreshError
}

func (d *DiscoveryService) IsReady() bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.lastRefreshTime.After(time.Time{}) && d.lastRefreshError == nil
}
