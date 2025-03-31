package discovery

// DiscoveredResource represents a resource discovered in Confluent Cloud
type DiscoveredResource struct {
	ID            string            // Resource identifier
	Type          string            // Resource type (e.g., "kafka", "schema_registry")
	Name          string            // Human-readable name
	EnvironmentID string            // Environment identifier
	Labels        map[string]string // Additional metadata as key-value pairs
}

// ResourceProvider defines the interface for components that provide discovered resources
type ResourceProvider interface {
	// GetResources returns the current list of discovered resources
	GetResources() []DiscoveredResource
}
