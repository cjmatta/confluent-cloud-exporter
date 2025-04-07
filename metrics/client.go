package metrics

import (
	"bufio"
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
	baseURL        = "https://api.telemetry.confluent.cloud"
	exportPath     = "/v2/metrics/cloud/export"
	defaultTimeout = 60 * time.Second
	maxRetries     = 3
	// Allow this to be adjusted dynamically based on resource count
	defaultBatchSize = 50 // Base batch size
)

// Client represents a Confluent Cloud Metrics API client for the export endpoint
type Client struct {
	httpClient        *http.Client
	apiKey            string
	apiSecret         string
	logger            *slog.Logger
	rateLimiter       confluent.ResponseAwareRateLimiter
	unauthorizedCache map[string]map[string]bool // Map of resourceType -> resourceID -> true
	cacheMutex        sync.RWMutex               // Mutex to protect the cache
}

// NewClient creates a new metrics client for the export endpoint
func NewClient(apiKey, apiSecret string, rateLimiter confluent.ResponseAwareRateLimiter) *Client {
	// Use a transport with connection pooling and HTTP/2 support
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false, // Enable compression
		ForceAttemptHTTP2:   true,  // Try HTTP/2 for better multiplexing
	}

	return &Client{
		httpClient: &http.Client{
			Timeout:   defaultTimeout,
			Transport: transport,
		},
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
	// Add timing information to overall process
	startTime := time.Now()
	defer func() {
		c.logger.Debug("Export metrics completed",
			"duration_ms", time.Since(startTime).Milliseconds())
	}()

	// Skip detailed debug logging in production mode
	if c.logger.Enabled(ctx, slog.LevelDebug) {
		// Only log resource details at debug level
		resourceTypeMap := make(map[string]int)
		for _, resource := range resources {
			resourceTypeMap[resource.Type]++
		}

		for resType, count := range resourceTypeMap {
			c.logger.Debug("Found resources of type",
				"type", resType,
				"count", count,
				"example_id", getExampleID(resources, resType))
		}
	}

	// Group resources by type more efficiently
	resourcesByType := make(map[string][]string)
	filteredCount := 0

	// Prefilter resources by known unauthorized resources
	c.cacheMutex.RLock()
	for _, resource := range resources {
		resourceType := mapResourceTypeToAPIName(resource.Type)
		if resourceType == "" {
			continue // Skip unsupported types
		}

		// Fast path check for unauthorized resources
		if resourceMap, exists := c.unauthorizedCache[resourceType]; exists && resourceMap[resource.ID] {
			filteredCount++
			continue
		}

		resourcesByType[resourceType] = append(resourcesByType[resourceType], resource.ID)
	}
	c.cacheMutex.RUnlock()

	if c.logger.Enabled(ctx, slog.LevelDebug) {
		// Only log filtered resources at debug level
		if filteredCount > 0 {
			c.logger.Debug("Skipped previously unauthorized resources", "count", filteredCount)
		}

		c.logger.Debug("Exporting metrics after type mapping",
			"mapped_resource_types", len(resourcesByType),
			"total_resources_after_mapping", countTotalResources(resourcesByType))
	}

	// Determine optimal batch size based on total resource count
	totalResources := countTotalResources(resourcesByType)
	batchSize := defaultBatchSize

	// For small resource counts, use larger batches to reduce HTTP overhead
	if totalResources < 100 {
		batchSize = 100 // Larger batch for fewer resources
	} else if totalResources > 1000 {
		batchSize = 30 // Smaller batch for many resources to avoid URL length limits
	}

	// Determine optimal concurrency based on resource type count
	concurrencyLimit := 10 // Default
	if len(resourcesByType) > 5 {
		// If we have many resource types, increase concurrency to prevent bottlenecks
		concurrencyLimit = 20
	}

	c.logger.Debug("Optimized batch parameters",
		"batch_size", batchSize,
		"concurrency", concurrencyLimit,
		"total_resources", totalResources)

	// Increase concurrent API requests for better throughput
	var mu sync.Mutex
	var allMetricsBuilder strings.Builder
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrencyLimit)

	// Process batches in parallel
	for resourceType, ids := range resourcesByType {
		// Process IDs in dynamically sized batches
		for i := 0; i < len(ids); i += batchSize {
			wg.Add(1)
			go func(rType string, batchStart int) {
				defer wg.Done()

				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				end := batchStart + batchSize
				if end > len(ids) {
					end = len(ids)
				}

				batchIDs := ids[batchStart:end]
				metrics, err := c.fetchMetricsForBatch(ctx, rType, batchIDs)
				if err != nil {
					c.logger.Warn("Failed to export metrics",
						"resource_type", rType,
						"batch_size", len(batchIDs),
						"error", err)
					return
				}

				if metrics != "" {
					mu.Lock()
					allMetricsBuilder.WriteString(metrics)
					mu.Unlock()
				}
			}(resourceType, i)
		}
	}

	wg.Wait()

	// Return result with optimized deduplication
	return deduplicateMetricsText(allMetricsBuilder.String()), nil
}

// fetchMetricsForBatch exports metrics for a specific batch of resources
func (c *Client) fetchMetricsForBatch(ctx context.Context, resourceType string, ids []string) (string, error) {
	// Add timing information for performance monitoring
	startTime := time.Now()
	defer func() {
		c.logger.Debug("Batch metrics fetch completed",
			"resource_type", resourceType,
			"count", len(ids),
			"duration_ms", time.Since(startTime).Milliseconds())
	}()

	// Create a timeout specifically for this batch request
	requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

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
			case <-requestCtx.Done():
				return "", requestCtx.Err()
			}
		}

		// Wait for rate limiter before sending request
		if err := c.rateLimiter.Wait(requestCtx); err != nil {
			return "", fmt.Errorf("rate limiter wait failed: %w", err)
		}

		// Create a request with context
		req, err := http.NewRequestWithContext(requestCtx, "GET", fullURL, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create request: %w", err)
		}

		// Add authentication and content headers
		req.SetBasicAuth(c.apiKey, c.apiSecret)
		req.Header.Add("Accept", "text/plain;version=0.0.4") // Request Prometheus format

		// Execute the request with timing
		reqStartTime := time.Now()
		resp, err := c.httpClient.Do(req)
		reqDuration := time.Since(reqStartTime)

		if reqDuration > 5*time.Second {
			c.logger.Warn("Slow API request",
				"resource_type", resourceType,
				"duration_ms", reqDuration.Milliseconds(),
				"resource_count", len(ids))
		}

		if err != nil {
			lastErr = err
			if isRetryableError(err) {
				continue
			}
			return "", fmt.Errorf("failed to execute request: %w", err)
		}

		// Update rate limiter with response headers
		if c.rateLimiter != nil {
			adaptiveLimiter, ok := c.rateLimiter.(*confluent.AdaptiveRateLimiter)
			if ok {
				adaptiveLimiter.UpdateFromResponse(resp)
			}
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
						case <-requestCtx.Done():
							return "", requestCtx.Err()
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

// Memory-optimized deduplication with buffered scanning
func deduplicateMetricsText(metricsText string) string {
	// Skip deduplication for small responses entirely
	if len(metricsText) < 5000 { // Increased threshold from 1000 to 5000
		return metricsText
	}

	// Pre-calculate approximate size
	estimatedLines := strings.Count(metricsText, "\n") + 1
	seenHelp := make(map[string]struct{}, estimatedLines/10)
	seenType := make(map[string]struct{}, estimatedLines/10)

	// Use a scanner for more memory-efficient processing
	var sb strings.Builder
	sb.Grow(len(metricsText)) // Pre-allocate approximate size

	scanner := bufio.NewScanner(strings.NewReader(metricsText))

	// Increase scanner buffer for larger responses
	const maxScannerBuffer = 1024 * 1024 // 1MB
	buf := make([]byte, maxScannerBuffer)
	scanner.Buffer(buf, maxScannerBuffer)

	for scanner.Scan() {
		line := scanner.Text()

		if len(line) == 0 {
			sb.WriteString("\n")
			continue
		}

		// Fast path for non-help, non-type lines
		if len(line) < 8 || line[0] != '#' {
			sb.WriteString(line)
			sb.WriteString("\n")
			continue
		}

		// Check for HELP lines
		if strings.HasPrefix(line, "# HELP ") {
			metricName := extractMetricName(line, 7)
			if _, seen := seenHelp[metricName]; seen {
				continue
			}
			seenHelp[metricName] = struct{}{}
		} else if strings.HasPrefix(line, "# TYPE ") {
			metricName := extractMetricName(line, 7)
			if _, seen := seenType[metricName]; seen {
				continue
			}
			seenType[metricName] = struct{}{}
		}

		sb.WriteString(line)
		sb.WriteString("\n")
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		// If scanner fails, return the original text
		return metricsText
	}

	return sb.String()
}

// Helper to extract metric name efficiently
func extractMetricName(line string, offset int) string {
	spacePos := strings.IndexByte(line[offset:], ' ')
	if spacePos <= 0 {
		return line[offset:] // No space found, use rest of line
	}
	return line[offset : offset+spacePos]
}
