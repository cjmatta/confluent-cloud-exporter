package metrics

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

	"confluent-cloud-exporter/confluent"
	"confluent-cloud-exporter/discovery"
)

const (
	baseURL         = "https://api.telemetry.confluent.cloud"
	exportPath      = "/v2/metrics/cloud/export"
	defaultTimeout  = 60 * time.Second
	maxRetries      = 3
	exportBatchSize = 20 // Maximum resources per export request to avoid exceeding URL length limits
)

// Client represents a Confluent Cloud Metrics API client for the export endpoint
type Client struct {
	httpClient        *http.Client
	apiKey            string
	apiSecret         string
	logger            *slog.Logger
	rateLimiter       *confluent.AdaptiveRateLimiter // Global rate limiter
	unauthorizedCache map[string]map[string]bool     // Map of resourceType -> resourceID -> true
	cacheMutex        sync.RWMutex                   // Mutex to protect the cache
}

// NewClient creates a new metrics client for the export endpoint
func NewClient(apiKey, apiSecret string, rateLimiter *confluent.AdaptiveRateLimiter) *Client {
	return &Client{
		httpClient:        &http.Client{Timeout: defaultTimeout},
		apiKey:            apiKey,
		apiSecret:         apiSecret,
		logger:            slog.Default(),
		rateLimiter:       rateLimiter,
		unauthorizedCache: make(map[string]map[string]bool),
		cacheMutex:        sync.RWMutex{},
	}
}

// SetLogger sets a custom logger for the client
func (c *Client) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// ExportMetrics with parallel batch processing
func (c *Client) ExportMetrics(ctx context.Context, resources []discovery.DiscoveredResource) (string, error) {
	// Add detailed debug logging for resource types
	resourceTypeMap := make(map[string]int)
	for _, resource := range resources {
		resourceTypeMap[resource.Type]++
	}

	// Log unique resource types and counts
	for resType, count := range resourceTypeMap {
		c.logger.Debug("Found resources of type",
			"type", resType,
			"count", count,
			"example_id", getExampleID(resources, resType))
	}

	// Group resources by type, filtering out ones we know are unauthorized
	resourcesByType := make(map[string][]string)
	filteredCount := 0

	for _, resource := range resources {
		resourceType := mapResourceTypeToAPIName(resource.Type)
		if resourceType == "" {
			// Skip unsupported resource types
			continue
		}

		// Skip resources we already know are unauthorized
		if c.isUnauthorized(resourceType, resource.ID) {
			filteredCount++
			continue
		}

		resourcesByType[resourceType] = append(resourcesByType[resourceType], resource.ID)
	}

	// Log filtered resources
	if filteredCount > 0 {
		c.logger.Debug("Skipped previously unauthorized resources",
			"count", filteredCount)
	}

	// Log what we're actually going to query
	c.logger.Debug("Exporting metrics after type mapping",
		"mapped_resource_types", len(resourcesByType),
		"total_resources_after_mapping", countTotalResources(resourcesByType))

	for resType, ids := range resourcesByType {
		c.logger.Debug("Mapped resource type",
			"api_type", resType,
			"count", len(ids),
			"example_ids", strings.Join(ids[:min(3, len(ids))], ", "))
	}

	// Process batches in parallel
	var mu sync.Mutex
	var allMetricsBuilder strings.Builder
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Allow up to 5 concurrent API requests
	errCount := 0

	for resourceType, ids := range resourcesByType {
		// Process IDs in batches
		for i := 0; i < len(ids); i += exportBatchSize {
			wg.Add(1)
			go func(rType string, batchStart int) {
				defer wg.Done()

				// Use semaphore for concurrency control
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				end := batchStart + exportBatchSize
				if end > len(ids) {
					end = len(ids)
				}

				batchIDs := ids[batchStart:end]
				metrics, err := c.fetchMetricsForBatch(ctx, rType, batchIDs)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()

					c.logger.Warn("Failed to export metrics for resource batch",
						"resource_type", rType,
						"batch_size", len(batchIDs),
						"error", err)
					return
				}

				// Safely append to the builder
				mu.Lock()
				allMetricsBuilder.WriteString(metrics)
				mu.Unlock()
			}(resourceType, i)
		}
	}

	// Wait for all batches to complete
	wg.Wait()

	c.logger.Debug("Completed parallel metrics fetching",
		"total_batches", countTotalResources(resourcesByType)/exportBatchSize+1,
		"error_batches", errCount)

	// Deduplicate HELP/TYPE lines
	return deduplicateMetricsText(allMetricsBuilder.String()), nil
}

// fetchMetricsForBatch exports metrics for a specific batch of resources
func (c *Client) fetchMetricsForBatch(ctx context.Context, resourceType string, ids []string) (string, error) {
	// Debug log the exact details of this batch
	c.logger.Debug("Fetching metrics batch",
		"resource_type", resourceType,
		"count", len(ids),
		"first_few_ids", strings.Join(ids[:min(3, len(ids))], ", "))

	// Create query parameters
	queryParams := url.Values{}

	// The parameter name depends on the resource type
	paramName := fmt.Sprintf("resource.%s.id", resourceType)

	// Add each ID as a separate parameter with the same name
	for _, id := range ids {
		queryParams.Add(paramName, id)
	}

	// Build the full URL
	fullURL := fmt.Sprintf("%s%s?%s", baseURL, exportPath, queryParams.Encode())

	// Log the full URL for debugging (but obscure API key in logs)
	safeURL := strings.Split(fullURL, "?")[0] + "?" + queryParams.Encode()
	c.logger.Debug("API request URL", "url", safeURL)

	var body []byte
	var lastErr error

	// Implement retry logic
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Add exponential backoff for retries
		if attempt > 0 {
			backoffTime := time.Duration(1<<uint(attempt-1)) * time.Second
			c.logger.Debug("Retrying export request after backoff",
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"backoff_time", backoffTime.String(),
				"resource_type", resourceType)

			// Add jitter to prevent thundering herd
			jitter := time.Duration(float64(backoffTime) * (0.5 + 0.5*float64(time.Now().Nanosecond())/float64(1000000000)))

			select {
			case <-time.After(jitter):
				// Continue after backoff
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		// Wait for rate limiter before sending request
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return "", fmt.Errorf("rate limiter wait failed: %w", err)
		}

		// Create a request with context
		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create request: %w", err)
		}

		// Add authentication and content headers
		req.SetBasicAuth(c.apiKey, c.apiSecret)
		req.Header.Add("Accept", "text/plain;version=0.0.4") // Request Prometheus format

		// Execute the request
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if isRetryableError(err) {
				continue
			}
			return "", fmt.Errorf("failed to execute request: %w", err)
		}

		// Read and close response body
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		// Check status code
		if resp.StatusCode != http.StatusOK {
			// Special handling for 403 Unauthorized
			if resp.StatusCode == http.StatusForbidden {
				// Parse the error response to get the list of unauthorized resources
				var errorResponse struct {
					Errors []struct {
						Status string `json:"status"`
						Detail string `json:"detail"`
					} `json:"errors"`
				}

				// Try to parse the error body
				if err := json.Unmarshal(body, &errorResponse); err == nil {
					// Extract unauthorized resource IDs
					for _, errObj := range errorResponse.Errors {
						if strings.Contains(errObj.Detail, "Unauthorized for the following resources:") {
							// Extract the resource IDs from the error message
							resourcePart := strings.TrimPrefix(errObj.Detail, "Unauthorized for the following resources: ")
							unauthorizedIDs := strings.Split(resourcePart, ",")

							// Add these to our cache of unauthorized resources
							c.markUnauthorized(resourceType, unauthorizedIDs)

							c.logger.Debug("Added unauthorized resources to cache",
								"resource_type", resourceType,
								"count", len(unauthorizedIDs))
						}
					}
				}

				// For 403 errors, log at debug level and return empty string (not an error)
				c.logger.Debug("No access to resource batch",
					"resource_type", resourceType,
					"status", resp.StatusCode,
					"batch_size", len(ids))

				return "", nil // Return empty string but not an error
			}

			// For other status codes, handle as before
			lastErr = fmt.Errorf("API returned non-200 status: %d, body: %s",
				resp.StatusCode, string(body))

			// Check if we should retry based on status code
			if resp.StatusCode == http.StatusTooManyRequests || // 429
				(resp.StatusCode >= 500 && resp.StatusCode < 600) { // 5xx

				// Check for rate limit headers
				resetHeader := resp.Header.Get("rateLimit-reset")
				if resetHeader != "" {
					if resetTime, err := time.ParseDuration(resetHeader + "s"); err == nil && resetTime > 0 {
						// Apply rate limit reset time
						c.logger.Debug("Rate limited, waiting for reset",
							"reset_seconds", resetHeader,
							"resource_type", resourceType)

						select {
						case <-time.After(resetTime):
							// Continue after rate limit reset
						case <-ctx.Done():
							return "", ctx.Err()
						}
					}
				}

				continue
			}

			// For other status codes, don't retry
			return "", lastErr
		}

		// Success! Break out of retry loop
		break
	}

	if body == nil {
		return "", fmt.Errorf("failed to get metrics after %d attempts: %w", maxRetries, lastErr)
	}

	return string(body), nil
}

// Helper to determine if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Timeout errors
	if strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "deadline exceeded") {
		return true
	}

	// Connection errors
	if strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "reset by peer") ||
		strings.Contains(errStr, "broken pipe") {
		return true
	}

	// DNS or lookup errors
	if strings.Contains(errStr, "lookup") ||
		strings.Contains(errStr, "no such host") {
		return true
	}

	return false
}

// Helper functions for debugging

// mapResourceTypeToAPIName converts discovery resource types to API parameter names
func mapResourceTypeToAPIName(resourceType string) string {
	// Map discovery service types to API parameter names
	mapping := map[string]string{
		"kafka":           "kafka",
		"connector":       "connector",
		"ksqldb":          "ksql",
		"schema_registry": "schema_registry",
		"compute_pool":    "compute_pool",
	}

	if apiName, ok := mapping[resourceType]; ok {
		return apiName
	}

	// Skip these types entirely - they don't have metrics
	skipTypes := map[string]bool{
		"environment": true,
	}

	if skipTypes[resourceType] {
		return ""
	}

	// For unknown types, log a warning but return the original
	return resourceType
}

// getExampleID gets an example ID for a resource type
func getExampleID(resources []discovery.DiscoveredResource, targetType string) string {
	for _, r := range resources {
		if r.Type == targetType {
			return r.ID
		}
	}
	return "none"
}

// countTotalResources counts the total number of resources across all types
func countTotalResources(resourcesByType map[string][]string) int {
	count := 0
	for _, ids := range resourcesByType {
		count += len(ids)
	}
	return count
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Add a method to check and update the unauthorized cache:
func (c *Client) isUnauthorized(resourceType, resourceID string) bool {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	if resourceMap, exists := c.unauthorizedCache[resourceType]; exists {
		return resourceMap[resourceID]
	}
	return false
}

func (c *Client) markUnauthorized(resourceType string, resourceIDs []string) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	if _, exists := c.unauthorizedCache[resourceType]; !exists {
		c.unauthorizedCache[resourceType] = make(map[string]bool)
	}

	for _, id := range resourceIDs {
		c.unauthorizedCache[resourceType][id] = true
	}
}

// Add this helper function to deduplicate help lines - optimized version
func deduplicateMetricsText(metricsText string) string {
	if len(metricsText) == 0 {
		return ""
	}

	// Faster implementation with pre-allocation
	lines := strings.Split(metricsText, "\n")
	seenHelp := make(map[string]bool, len(lines)/10) // Pre-allocate with estimated size
	seenType := make(map[string]bool, len(lines)/10)
	result := make([]string, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			result = append(result, line)
			continue
		}

		// Fast check for help/type lines
		if len(line) > 7 && line[0] == '#' {
			if line[2] == 'H' && strings.HasPrefix(line, "# HELP ") {
				// For HELP lines
				idx := strings.IndexByte(line[7:], ' ')
				if idx > 0 {
					metricName := line[7 : 7+idx]
					if seenHelp[metricName] {
						continue
					}
					seenHelp[metricName] = true
				}
			} else if line[2] == 'T' && strings.HasPrefix(line, "# TYPE ") {
				// For TYPE lines
				idx := strings.IndexByte(line[7:], ' ')
				if idx > 0 {
					metricName := line[7 : 7+idx]
					if seenType[metricName] {
						continue
					}
					seenType[metricName] = true
				}
			}
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
