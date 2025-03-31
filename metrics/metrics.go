package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Namespace for all metrics in this exporter
	namespace = "confluent"

	// Common label names - using constants ensures consistency
	LabelResourceID      = "resource_id"
	LabelResourceType    = "resource_type"
	LabelResourceName    = "resource_name"
	LabelEnvironmentID   = "environment_id"
	LabelEnvironmentName = "environment_name"
	LabelClusterID       = "cluster_id"
	LabelClusterName     = "cluster_name"
	LabelTopicName       = "topic_name"
	LabelConnectorName   = "connector_name"
	LabelConnectorType   = "connector_type"
	LabelConsumerGroup   = "consumer_group_id"

	// Common Confluent resource types
	ResourceTypeKafka          = "kafka"
	ResourceTypeSchemaRegistry = "schema_registry"
	ResourceTypeConnector      = "connector"
	ResourceTypeKsqlDB         = "ksqldb"
	ResourceTypeConnectCluster = "connect_cluster"
)

// Standard label sets used for different resource types
var (
	// Common labels used across most metrics
	commonLabels = []string{
		LabelResourceID,
		LabelClusterID,
		LabelEnvironmentID,
	}

	// Additional labels for discovered resources
	discoveryLabels = []string{
		LabelResourceType,
		LabelResourceName,
		LabelEnvironmentName,
	}
)

// MetricsDescriptors holds all metric descriptors for the exporter
type MetricsDescriptors struct {
	// Internal exporter metrics
	Up                    *prometheus.Desc
	ScrapeSuccessTotal    *prometheus.Desc
	ScrapeErrorsTotal     *prometheus.Desc
	ScrapeDurationSeconds *prometheus.Desc
	DiscoveredResources   *prometheus.Desc

	// Dynamic metrics map - populated from Confluent Cloud Metrics API responses
	// The key is a combination of metric name and type
	DynamicMetrics map[string]*prometheus.Desc
}

// NewMetricsDescriptors creates and returns new metric descriptors with some common Confluent metrics pre-defined
func NewMetricsDescriptors() *MetricsDescriptors {
	md := &MetricsDescriptors{
		// Internal exporter metrics
		Up: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"1 if the exporter is able to connect to Confluent Cloud API, 0 otherwise",
			nil, nil,
		),
		ScrapeSuccessTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "scrape_success_total"),
			"Total number of successful scrapes",
			nil, nil,
		),
		ScrapeErrorsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "scrape_errors_total"),
			"Total number of scrape errors",
			nil, nil,
		),
		ScrapeDurationSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "scrape_duration_seconds"),
			"Duration of scrape in seconds",
			nil, nil,
		),
		DiscoveredResources: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "discovery", "resources"),
			"Number of resources discovered in Confluent Cloud by type",
			[]string{LabelResourceType}, nil,
		),

		// Initialize empty map for dynamic metrics
		DynamicMetrics: make(map[string]*prometheus.Desc),
	}

	// Pre-define some common Confluent Cloud metrics as examples
	// These will be used in case we collect metrics before discovering new ones from the API

	// Kafka metrics
	md.AddOrUpdateMetricDesc("received_bytes", ResourceTypeKafka,
		"Number of bytes received per second",
		[]string{LabelResourceName, LabelEnvironmentName})

	md.AddOrUpdateMetricDesc("sent_bytes", ResourceTypeKafka,
		"Number of bytes sent per second",
		[]string{LabelResourceName, LabelEnvironmentName})

	md.AddOrUpdateMetricDesc("received_records", ResourceTypeKafka,
		"Number of records received per second",
		[]string{LabelResourceName, LabelEnvironmentName})

	// Topic metrics
	md.AddOrUpdateMetricDesc("retained_bytes", ResourceTypeKafka,
		"Number of retained bytes for a topic",
		[]string{LabelTopicName, LabelResourceName, LabelEnvironmentName})

	// Schema Registry metrics
	md.AddOrUpdateMetricDesc("success_count", ResourceTypeSchemaRegistry,
		"Number of successful schema registry requests",
		[]string{LabelResourceName, LabelEnvironmentName})

	// Connector metrics
	md.AddOrUpdateMetricDesc("task_error_count", ResourceTypeConnector,
		"Number of connector task errors",
		[]string{LabelConnectorName, LabelConnectorType, LabelResourceName, LabelEnvironmentName})

	return md
}

// AddOrUpdateMetricDesc adds or updates a metric descriptor in the dynamic metrics map
func (md *MetricsDescriptors) AddOrUpdateMetricDesc(metricName, metricType, help string, variableLabels []string) *prometheus.Desc {
	key := metricType + "_" + metricName

	// Create a consistent metric naming scheme
	fqName := prometheus.BuildFQName(namespace, metricType, metricName)

	// Combine common labels with variable labels for this specific metric
	allLabels := append(commonLabels, variableLabels...)

	// Create the metric descriptor
	desc := prometheus.NewDesc(fqName, help, allLabels, nil)

	// Store in our map
	md.DynamicMetrics[key] = desc

	return desc
}

// GetMetricDesc retrieves a metric descriptor by name and type
func (md *MetricsDescriptors) GetMetricDesc(metricName, metricType string) (*prometheus.Desc, bool) {
	key := metricType + "_" + metricName
	desc, exists := md.DynamicMetrics[key]
	return desc, exists
}

// GetAllMetricDescs returns all metric descriptors
func (md *MetricsDescriptors) GetAllMetricDescs() []*prometheus.Desc {
	descs := make([]*prometheus.Desc, 0, len(md.DynamicMetrics)+5) // +5 for internal metrics

	// Add internal metrics
	descs = append(descs, md.Up)
	descs = append(descs, md.ScrapeSuccessTotal)
	descs = append(descs, md.ScrapeErrorsTotal)
	descs = append(descs, md.ScrapeDurationSeconds)
	descs = append(descs, md.DiscoveredResources)

	// Add all dynamic metrics
	for _, desc := range md.DynamicMetrics {
		descs = append(descs, desc)
	}

	return descs
}
