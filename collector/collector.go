package collector

import (
	"log/slog"
	"sync"

	"confluent-cloud-exporter/config"
	"confluent-cloud-exporter/discovery"
	"confluent-cloud-exporter/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

// ConfluentCollector implements the prometheus.Collector interface
type ConfluentCollector struct {
	config           *config.Config
	resourceProvider discovery.ResourceProvider
	metrics          *metrics.MetricsDescriptors
	logger           *slog.Logger
	mutex            sync.Mutex
}

// NewCollector creates a new ConfluentCollector
func NewCollector(
	cfg *config.Config,
	rp discovery.ResourceProvider,
	metricDescriptors *metrics.MetricsDescriptors,
	logger *slog.Logger,
) *ConfluentCollector {
	return &ConfluentCollector{
		config:           cfg,
		resourceProvider: rp,
		metrics:          metricDescriptors,
		logger:           logger,
	}
}

// Describe implements prometheus.Collector
func (c *ConfluentCollector) Describe(ch chan<- *prometheus.Desc) {
	// Send all metric descriptors to the channel
	for _, desc := range c.metrics.GetAllMetricDescs() {
		ch <- desc
	}
}

// Collect implements prometheus.Collector
func (c *ConfluentCollector) Collect(ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Report exporter is up
	ch <- prometheus.MustNewConstMetric(
		c.metrics.Up,
		prometheus.GaugeValue,
		1,
	)

	// Get resources from provider
	resources := c.resourceProvider.GetResources()

	// Count resources by type
	resourceCounts := make(map[string]int)
	for _, res := range resources {
		resourceCounts[res.Type]++
	}

	// Report discovered resources by type
	for resType, count := range resourceCounts {
		ch <- prometheus.MustNewConstMetric(
			c.metrics.DiscoveredResources,
			prometheus.GaugeValue,
			float64(count),
			resType,
		)
	}

	// Report scrape success as 0 for now (placeholder)
	ch <- prometheus.MustNewConstMetric(
		c.metrics.ScrapeSuccessTotal,
		prometheus.GaugeValue,
		0,
	)

	c.logger.Info("Collect called",
		"resourceCount", len(resources),
		"message", "metrics fetching not implemented yet")
}
