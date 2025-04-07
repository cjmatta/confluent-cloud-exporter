package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"confluent-cloud-exporter/discovery"
	"confluent-cloud-exporter/metrics"
)

// ConfluentCollector implements prometheus.Collector for Confluent Cloud.
type ConfluentCollector struct {
	discoveryService *discovery.DiscoveryService
	metricsClient    *metrics.Client
	resourcesByID    map[string]discovery.DiscoveredResource
	resourceMutex    sync.RWMutex
	logger           *slog.Logger

	// Configuration
	cacheDuration time.Duration

	// Track last refresh time
	lastRefresh time.Time

	// Internal metrics
	up                  prometheus.Gauge
	scrapeDuration      prometheus.Gauge
	scrapeSuccess       prometheus.Gauge
	discoveredResources *prometheus.GaugeVec
}

// NewCollector creates a new collector for Confluent Cloud metrics.
func NewCollector(discoveryService *discovery.DiscoveryService, metricsClient *metrics.Client, cacheDuration time.Duration) *ConfluentCollector {
	return &ConfluentCollector{
		discoveryService: discoveryService,
		metricsClient:    metricsClient,
		resourcesByID:    make(map[string]discovery.DiscoveredResource),
		cacheDuration:    cacheDuration,
		logger:           slog.Default(),

		// Initialize internal metrics
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "confluent_cloud_exporter_up",
			Help: "1 if the exporter can reach the Confluent Cloud API, 0 otherwise",
		}),
		scrapeDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "confluent_cloud_exporter_scrape_duration_seconds",
			Help: "Duration of the scrape in seconds",
		}),
		scrapeSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "confluent_cloud_exporter_scrape_success",
			Help: "1 if the scrape was successful, 0 otherwise",
		}),
		discoveredResources: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "confluent_cloud_exporter_discovered_resources",
				Help: "Number of resources discovered by type",
			},
			[]string{"resource_type"},
		),
	}
}

// SetLogger sets a custom logger for the collector.
func (c *ConfluentCollector) SetLogger(logger *slog.Logger) {
	c.logger = logger
	c.metricsClient.SetLogger(logger)
}

// updateResourcesFromDiscovery refreshes the resources from the discovery service.
// It returns true if resources were updated, false otherwise.
func (c *ConfluentCollector) updateResourcesFromDiscovery(ctx context.Context) (bool, error) {
	// First check if discovery service is ready
	if !c.discoveryService.IsReady() {
		return false, fmt.Errorf("no resources available yet, initial discovery may still be in progress")
	}

	// If resources are still fresh, don't refresh
	if !c.lastRefresh.IsZero() && time.Since(c.lastRefresh) < c.cacheDuration {
		c.logger.Debug("Using cached resources",
			"age", time.Since(c.lastRefresh).String(),
			"expires_in", (c.cacheDuration - time.Since(c.lastRefresh)).String())
		return false, nil
	}

	// Get fresh resources from discovery service
	resources := c.discoveryService.GetResources()

	if len(resources) == 0 {
		c.logger.Warn("Discovery service returned empty resources list")
		return false, fmt.Errorf("discovery service returned no resources")
	}

	// Update our local cache
	newResourcesByID := make(map[string]discovery.DiscoveredResource)
	resourcesByType := make(map[string]int)

	for _, resource := range resources {
		resourcesByType[resource.Type]++
		newResourcesByID[resource.ID] = resource
	}

	// Update metrics about discovered resources
	for resType, count := range resourcesByType {
		c.discoveredResources.WithLabelValues(resType).Set(float64(count))
	}

	// Replace the resources atomically
	c.resourceMutex.Lock()
	c.resourcesByID = newResourcesByID
	c.lastRefresh = time.Now()
	c.resourceMutex.Unlock()

	c.logger.Info("Updated resources from discovery service",
		"resource_count", len(newResourcesByID),
		"types", len(resourcesByType))

	return true, nil
}

// RefreshResourcesAsync refreshes resources from the discovery service in the background
func (c *ConfluentCollector) RefreshResourcesAsync(ctx context.Context) error {
	startTime := time.Now()
	c.logger.Debug("Starting background resource discovery")

	// Perform the discovery
	if err := c.discoveryService.RefreshResources(ctx); err != nil {
		c.logger.Error("Failed to refresh resources", "error", err)
		return err
	}

	// After successful discovery, update our local cache
	_, err := c.updateResourcesFromDiscovery(ctx)
	if err != nil {
		c.logger.Warn("Discovery completed but failed to update collector resources", "error", err)
		// Don't return error here - discovery was successful even if we couldn't update
	}

	c.logger.Info("Background resource discovery completed",
		"duration", time.Since(startTime).String())

	return nil
}

// Describe implements prometheus.Collector.
func (c *ConfluentCollector) Describe(ch chan<- *prometheus.Desc) {
	// Send descriptions of internal metrics
	c.up.Describe(ch)
	c.scrapeDuration.Describe(ch)
	c.scrapeSuccess.Describe(ch)
	c.discoveredResources.Describe(ch)

	// For dynamically generated metrics from the export endpoint,
	// we don't send their descriptors ahead of time.
	// This is a common pattern for exporters dealing with varying metrics.
}

// Collect implements prometheus.Collector.
func (c *ConfluentCollector) Collect(ch chan<- prometheus.Metric) {
	// Start tracking scrape duration
	startTime := time.Now()

	// Set default values
	c.up.Set(1)
	c.scrapeSuccess.Set(0)

	// Send our internal metrics at the end
	defer func() {
		c.scrapeDuration.Set(time.Since(startTime).Seconds())
		c.up.Collect(ch)
		c.scrapeDuration.Collect(ch)
		c.scrapeSuccess.Collect(ch)
		c.discoveredResources.Collect(ch)
	}()

	// Create context with reasonable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Update resources if needed (this handles caching internally)
	resourcesUpdated, err := c.updateResourcesFromDiscovery(ctx)
	if err != nil {
		c.logger.Error("Failed to update resources", "error", err)
		// Don't set up=0 for resource discovery in progress - that's not an API failure
		if !strings.Contains(err.Error(), "initial discovery may still be in progress") {
			c.up.Set(0)
		}
		return
	}

	// Get a read-only copy of the resources
	c.resourceMutex.RLock()
	resources := make([]discovery.DiscoveredResource, 0, len(c.resourcesByID))
	for _, r := range c.resourcesByID {
		resources = append(resources, r)
	}
	c.resourceMutex.RUnlock()

	// If no resources found, nothing to do
	if len(resources) == 0 {
		c.logger.Warn("No resources discovered, nothing to collect")
		return
	}

	// Fetch metrics for all resources
	c.logger.Debug("Fetching metrics for resources", "count", len(resources))
	metricsText, err := c.metricsClient.ExportMetrics(ctx, resources)
	if err != nil {
		c.logger.Error("Failed to fetch metrics", "error", err)
		c.up.Set(0)
		return
	}

	// Process returned metrics
	c.logger.Debug("Processing metrics", "text_size", len(metricsText))
	err = c.processMetrics(metricsText, ch)
	if err != nil {
		c.logger.Error("Failed to process metrics", "error", err)
		return
	}

	// If we got here, the scrape was successful
	c.scrapeSuccess.Set(1)
	c.logger.Debug("Completed metrics collection",
		"duration", time.Since(startTime).String(),
		"updated_resources", resourcesUpdated)
}

// processMetrics parses Prometheus text format metrics and sends them to the channel.
// It also enhances metrics with additional labels from discovered resources when applicable.
func (c *ConfluentCollector) processMetrics(metricsText string, ch chan<- prometheus.Metric) error {

	// Add at the beginning of processMetrics
	allLabels := make(map[string]bool)

	// If there are no metrics, nothing to do
	if len(strings.TrimSpace(metricsText)) == 0 {
		c.logger.Debug("No metrics to process, response was empty")
		return nil
	}

	var parser expfmt.TextParser

	// Parse the Prometheus text format into metric families
	reader := strings.NewReader(metricsText)
	metricFamilies, err := parser.TextToMetricFamilies(reader)
	if err != nil {
		return err
	}

	// Process each metric family
	for name, family := range metricFamilies {
		// Convert the metrics to prometheus.Metric instances
		for _, m := range family.GetMetric() {
			labels := make(map[string]string)

			// Extract all labels from the metric
			for _, label := range m.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}

			// Inside the metric processing loop after extracting labels
			for labelName := range labels {
				allLabels[labelName] = true
			}

			// Check if this metric has a resource ID label we can use to enrich it
			var resourceID string

			// Look for resource ID labels
			for _, pattern := range []string{"kafka_id", "connector_id", "schema_registry_id", "ksqldb_id", "compute_pool_id"} {
				if id, ok := labels[pattern]; ok && id != "" {
					resourceID = id
					break
				}
			}

			// Add to processMetrics at the point where you extract resource IDs:

			if resourceID != "" {
				c.resourceMutex.RLock()
				resource, found := c.resourcesByID[resourceID]

				if !found {
					c.logger.Debug("Resource ID found in metrics but not in discovered resources",
						"resource_id", resourceID,
						"metric_name", name)
				} else {
					c.logger.Debug("Resource ID matched discovered resource",
						"resource_id", resourceID,
						"resource_type", resource.Type,
						"metric_name", name)
				}
				// Rest of your existing code...
			}

			// Create the metric with the appropriate value type
			var metricValue float64
			var valueType prometheus.ValueType

			switch family.GetType() {
			case io_prometheus_client.MetricType_COUNTER:
				metricValue = m.GetCounter().GetValue()
				valueType = prometheus.CounterValue
			case io_prometheus_client.MetricType_GAUGE:
				metricValue = m.GetGauge().GetValue()
				valueType = prometheus.GaugeValue
			default:
				// Skip unsupported types
				c.logger.Debug("Skipping unsupported metric type",
					"name", name,
					"type", family.GetType().String())
				continue
			}
			// Get labelKeys ad labelValues (in the same order!)
			labelKeys := make([]string, 0, len(labels))
			labelValues := make([]string, 0, len(labels))

			for k, v := range labels {
				labelKeys = append(labelKeys, k)
				labelValues = append(labelValues, v)
			}

			// If we found a resource ID, check if we can add more labels from our discovery data
			if resourceID != "" {
				c.resourceMutex.RLock()
				if resource, ok := c.resourcesByID[resourceID]; ok {
					// Add resource metadata as labels
					if resource.Name != "" && !containsLabel(labelKeys, "name") {
						labelKeys = append(labelKeys, "name")
						labelValues = append(labelValues, resource.Name)
					}

					if resource.EnvironmentID != "" && !containsLabel(labelKeys, "environment_id") {
						labelKeys = append(labelKeys, "environment_id")
						labelValues = append(labelValues, resource.EnvironmentID)
					}

					if resource.EnvironmentName != "" && !containsLabel(labelKeys, "environment_name") {
						labelKeys = append(labelKeys, "environment_name")
						labelValues = append(labelValues, resource.EnvironmentName)
					}

					// Add any extra labels from the resource
					for k, v := range resource.Labels {
						if !containsLabel(labelKeys, k) {
							labelKeys = append(labelKeys, k)
							labelValues = append(labelValues, v)
						}
					}
				}
				c.resourceMutex.RUnlock()
			}

			// Create and send the metric
			metric, err := prometheus.NewConstMetric(
				prometheus.NewDesc(name, family.GetHelp(), labelKeys, nil),
				valueType,
				metricValue,
				labelValues...,
			)

			if err != nil {
				c.logger.Warn("Failed to create metric",
					"name", name,
					"error", err)
				continue
			}

			ch <- metric
		}
	}

	// At the end of processing
	labelNames := make([]string, 0, len(allLabels))
	for name := range allLabels {
		labelNames = append(labelNames, name)
	}
	c.logger.Debug("All unique label names found in metrics", "labels", strings.Join(labelNames, ", "))

	return nil
}

// containsLabel checks if a label key exists in a slice of label keys.
func containsLabel(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
