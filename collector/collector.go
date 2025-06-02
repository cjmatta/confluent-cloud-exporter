package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"confluent-cloud-exporter/discovery"
)

// MetricsClientInterface defines the methods the collector needs from a metrics client.
// This allows for mocking the metrics client in tests.
type MetricsClientInterface interface {
	ExportMetrics(ctx context.Context, resources []discovery.DiscoveredResource) (string, error)
	SetLogger(logger *slog.Logger)
}

// ConfluentCollector implements prometheus.Collector for Confluent Cloud.
type ConfluentCollector struct {
	discoveryService discovery.ResourceProvider // Use the interface
	metricsClient    MetricsClientInterface     // Use the interface
	resourcesByID    map[string]discovery.DiscoveredResource
	resourceMutex    sync.RWMutex
	logger           *slog.Logger

	// Configuration
	cacheDuration time.Duration

	// Track last refresh time
	lastRefresh time.Time

	// Metrics cache
	metricsMutex       sync.RWMutex
	metricsCache       []prometheus.Metric
	metricsLastRefresh time.Time

	// Internal metrics
	up                  prometheus.Gauge
	scrapeDuration      prometheus.Gauge
	scrapeSuccess       prometheus.Gauge
	discoveredResources *prometheus.GaugeVec

	isRefreshing atomic.Bool
}

// NewCollector creates a new collector for Confluent Cloud metrics.
// It now accepts interfaces for dependencies.
func NewCollector(discoveryService discovery.ResourceProvider, metricsClient MetricsClientInterface, cacheDuration time.Duration) *ConfluentCollector {
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
	// Also set the logger on the dependencies if they support it
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

// updateDiscoveredResourceMetrics updates the discoveredResources metrics
// based on the current state of the discovery service.
func (c *ConfluentCollector) updateDiscoveredResourceMetrics() {
	// Get the latest resources from the discovery service
	// This call should now be non-blocking since we fixed the discovery service
	resources := c.discoveryService.GetResources()

	// Count resources by type
	resourcesByType := make(map[string]int)
	for _, resource := range resources {
		resourcesByType[resource.Type]++
	}

	// If no resources are discovered yet, we'll just report 0 for all known types
	// This is fine during startup or if discovery is still in progress
	if len(resources) == 0 {
		c.logger.Debug("No resources discovered yet, reporting 0 counts")
		// Don't set any metrics - Prometheus will show them as absent until they have values
		return
	}

	// Update metrics about discovered resources
	for resType, count := range resourcesByType {
		c.discoveredResources.WithLabelValues(resType).Set(float64(count))
	}

	c.logger.Debug("Updated discovered resource metrics",
		"total_resources", len(resources),
		"resource_types", len(resourcesByType))
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
	updated, err := c.updateResourcesFromDiscovery(ctx)
	if err != nil {
		c.logger.Warn("Discovery completed but failed to update collector resources", "error", err)
		// Don't return error here - discovery was successful even if we couldn't update
	} else if updated {
		// Immediately fetch metrics for new resources so /metrics returns full data
		c.logger.Debug("Resources updated, fetching initial metrics immediately")
		if err := c.RefreshMetricsAsync(ctx); err != nil {
			c.logger.Error("Failed to refresh metrics after discovery", "error", err)
		}
	}

	c.logger.Info("Background resource discovery completed",
		"duration", time.Since(startTime).String())

	return nil
}

// RefreshMetricsAsync fetches metrics in the background and updates the cache.
func (c *ConfluentCollector) RefreshMetricsAsync(ctx context.Context) error {
	start := time.Now()
	c.logger.Debug("Starting background metrics refresh")

	// If we haven't done initial discovery update, do it now
	if c.lastRefresh.IsZero() {
		c.logger.Debug("No resources cached, performing discovery before metrics fetch")
		if _, err := c.updateResourcesFromDiscovery(ctx); err != nil {
			c.logger.Error("Failed to update resources before metrics fetch", "error", err)
			// continue anyway, resources may fill later
		}
	}

	// Snapshot resources
	c.resourceMutex.RLock()
	resources := make([]discovery.DiscoveredResource, 0, len(c.resourcesByID))
	for _, r := range c.resourcesByID {
		resources = append(resources, r)
	}
	c.resourceMutex.RUnlock()

	// Fetch metrics text
	metricsText, err := c.metricsClient.ExportMetrics(ctx, resources)
	if err != nil {
		c.logger.Error("Failed to fetch metrics in background", "error", err)
		return err
	}

	// Parse metrics into slice
	ch := make(chan prometheus.Metric, 1024)
	var newMetrics []prometheus.Metric
	go func() {
		for m := range ch {
			newMetrics = append(newMetrics, m)
		}
	}()
	if err := c.processMetrics(metricsText, ch); err != nil {
		c.logger.Error("Failed to process metrics in background", "error", err)
		close(ch)
		return err
	}
	close(ch)

	// Update cache
	c.metricsMutex.Lock()
	c.metricsCache = newMetrics
	c.metricsLastRefresh = time.Now()
	c.metricsMutex.Unlock()

	c.logger.Info("Background metrics refresh completed", "count", len(newMetrics), "duration", time.Since(start).String())
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
	startTime := time.Now()

	// Immediate return path using cached metrics
	c.metricsMutex.RLock()
	cacheIsStale := time.Since(c.metricsLastRefresh) >= c.cacheDuration
	cacheIsEmpty := len(c.metricsCache) == 0
	// Check if cache is valid (not empty AND not stale)
	if !cacheIsEmpty && !cacheIsStale {
		c.logger.Debug("Replaying cached metrics", "cacheAge", time.Since(c.metricsLastRefresh).String())
		for _, m := range c.metricsCache {
			ch <- m
		}
		// Also send internal metrics reflecting the cached scrape
		c.up.Set(1)
		c.scrapeSuccess.Set(1)
		c.scrapeDuration.Set(0) // Duration isn't super relevant for cached replay
		c.up.Collect(ch)
		c.scrapeDuration.Collect(ch)
		c.scrapeSuccess.Collect(ch)
		// c.updateDiscoveredResourceMetrics() // Ensure discovered resource counts are current
		//c.discoveredResources.Collect(ch)
		c.metricsMutex.RUnlock()
		return // Return early as cache is valid
	}
	// If we reach here, cache is either empty or stale (or both)
	c.metricsMutex.RUnlock() // Release read lock before potentially triggering background refresh

	// Log the reason for potentially triggering a refresh
	if cacheIsEmpty {
		c.logger.Debug("Metrics cache is empty, attempting to trigger background refresh")
	} else { // cacheIsStale must be true
		c.logger.Debug("Metrics cache is stale, attempting to trigger background refresh",
			"cacheAge", time.Since(c.metricsLastRefresh).String(),
			"cacheDuration", c.cacheDuration.String())
	}

	// Trigger background metrics refresh only if not already running
	c.logger.Debug("Checking refresh status before CompareAndSwap", "isRefreshing", c.isRefreshing.Load()) // Log before check
	if c.isRefreshing.CompareAndSwap(false, true) {                                                        // Try to acquire the lock/start refresh
		c.logger.Debug("CompareAndSwap succeeded: Starting new background metrics refresh goroutine") // Log success
		go func() {
			// Defer the flag reset using CompareAndSwap for safety
			defer func() {
				reset := c.isRefreshing.CompareAndSwap(true, false)                                                                                                         // Reset only if it was true
				c.logger.Debug("Background metrics refresh goroutine finished, attempted flag reset", "resetSuccessful", reset, "finalIsRefreshing", c.isRefreshing.Load()) // Log reset attempt and final state
			}()

			c.logger.Debug("Inside background metrics refresh goroutine") // Log entry into goroutine

			// Use a separate context for the background task
			ctxBg, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Use a reasonable timeout
			defer cancel()
			if err := c.RefreshMetricsAsync(ctxBg); err != nil {
				c.logger.Error("Background metrics refresh failed", "error", err)
				// Consider setting c.up metric to 0 here if refresh fails consistently?
			} else {
				c.logger.Info("Background metrics refresh goroutine completed successfully")
			}
		}()
	} else {
		c.logger.Debug("CompareAndSwap failed: Metrics refresh already in progress, skipping new background refresh trigger") // Log failure
	}

	// Return basic internal metrics immediately regardless of refresh trigger outcome
	c.logger.Debug("Returning basic internal metrics while refresh runs in background")
	c.up.Set(1)                                           // Exporter is up
	c.scrapeSuccess.Set(0)                                // Indicate scrape didn't return full metrics this time
	c.scrapeDuration.Set(time.Since(startTime).Seconds()) // Record duration of this minimal scrape
	c.up.Collect(ch)
	c.scrapeDuration.Collect(ch)
	c.scrapeSuccess.Collect(ch)

	// Always try to update discovered resource metrics, even if discovery is in progress
	// This should now be non-blocking thanks to our discovery service fix
	c.updateDiscoveredResourceMetrics()
	c.discoveredResources.Collect(ch)

	c.logger.Debug("Collect completed with internal metrics only",
		"duration", time.Since(startTime).String(),
		"cache_empty", cacheIsEmpty,
		"cache_stale", cacheIsStale)
}

// processMetrics parses Prometheus text format metrics and sends them to the channel.
// It also enhances metrics with additional labels from discovered resources when applicable.
func (c *ConfluentCollector) processMetrics(metricsText string, ch chan<- prometheus.Metric) error {
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

			// Track all unique label names for debugging
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
				c.resourceMutex.RUnlock()
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
			// Get labelKeys and labelValues (in the same order!)
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
