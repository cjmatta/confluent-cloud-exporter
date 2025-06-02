package collector_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"confluent-cloud-exporter/collector"
	"confluent-cloud-exporter/discovery"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks ---

// MockDiscoveryService simulates the discovery service
type MockDiscoveryService struct {
	resources       []discovery.DiscoveredResource
	mutex           sync.RWMutex
	refreshStarted  chan struct{} // Signals when RefreshResources starts
	unblockRefresh  chan struct{} // Signals RefreshResources to finish
	isReady         bool
	refreshDuration time.Duration // How long RefreshResources should block
}

func NewMockDiscoveryService() *MockDiscoveryService {
	return &MockDiscoveryService{
		resources:      []discovery.DiscoveredResource{},
		refreshStarted: make(chan struct{}, 1),
		unblockRefresh: make(chan struct{}),
	}
}

func (m *MockDiscoveryService) GetResources() []discovery.DiscoveredResource {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	// Return a copy
	resCopy := make([]discovery.DiscoveredResource, len(m.resources))
	copy(resCopy, m.resources)
	return resCopy
}

func (m *MockDiscoveryService) RefreshResources(ctx context.Context) error {
	m.refreshStarted <- struct{}{} // Signal that refresh has started
	slog.Debug("MockDiscoveryService: RefreshResources started, waiting for unblock signal or context cancel...")

	select {
	case <-m.unblockRefresh:
		slog.Debug("MockDiscoveryService: Unblock signal received.")
		// Simulate adding resources after refresh
		m.mutex.Lock()
		m.resources = []discovery.DiscoveredResource{
			{ID: "lkc-123", Type: "kafka", Name: "test-kafka"},
		}
		m.isReady = true // Mark as ready after successful refresh
		m.mutex.Unlock()
		slog.Debug("MockDiscoveryService: RefreshResources finished.")
		return nil
	case <-ctx.Done():
		slog.Debug("MockDiscoveryService: Context cancelled during refresh.")
		return ctx.Err()
	case <-time.After(m.refreshDuration): // Allow timeout for blocking behavior
		if m.refreshDuration > 0 {
			slog.Debug("MockDiscoveryService: RefreshResources timed out (simulated block).")
			// Simulate adding resources even on timeout for some test cases if needed
			m.mutex.Lock()
			m.resources = []discovery.DiscoveredResource{
				{ID: "lkc-123", Type: "kafka", Name: "test-kafka"},
			}
			m.isReady = true
			m.mutex.Unlock()
			return nil // Or return an error if timeout should fail discovery
		}
		// If duration is 0, proceed as if unblocked immediately (for non-blocking tests)
		slog.Debug("MockDiscoveryService: RefreshResources finished immediately (duration 0).")
		m.mutex.Lock()
		m.isReady = true
		m.mutex.Unlock()
		return nil
	}
}

func (m *MockDiscoveryService) IsReady() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.isReady
}

// MockMetricsClient simulates the metrics client
type MockMetricsClient struct {
	exportMetricsFunc func(ctx context.Context, resources []discovery.DiscoveredResource) (string, error)
	logger            *slog.Logger
}

func (m *MockMetricsClient) ExportMetrics(ctx context.Context, resources []discovery.DiscoveredResource) (string, error) {
	if m.exportMetricsFunc != nil {
		return m.exportMetricsFunc(ctx, resources)
	}
	// Default behavior: return some basic metric text if resources are provided
	if len(resources) > 0 {
		return `
# HELP confluent_cloud_kafka_server_request_latency_ms Kafka request latency
# TYPE confluent_cloud_kafka_server_request_latency_ms gauge
confluent_cloud_kafka_server_request_latency_ms{kafka_id="` + resources[0].ID + `",request_type="fetch"} 5.5
`, nil
	}
	return "", nil // No metrics if no resources
}

func (m *MockMetricsClient) SetLogger(logger *slog.Logger) {
	m.logger = logger
}

// --- Test ---

func TestCollector_ImmediateResponseDuringBackgroundRefresh(t *testing.T) {
	// Configure logging for tests
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})) // Discard logs by default
	// logger := setupTestLogger(t) // Use this for debugging the test itself

	// 1. Setup Mocks
	mockDiscovery := NewMockDiscoveryService()
	mockDiscovery.refreshDuration = 5 * time.Second // Ensure refresh blocks long enough

	mockMetrics := &MockMetricsClient{}

	// 2. Create Collector and HTTP Server
	testCollector := collector.NewCollector(mockDiscovery, mockMetrics, 1*time.Minute) // Use real collector
	testCollector.SetLogger(logger)

	registry := prometheus.NewRegistry()
	err := registry.Register(testCollector)
	require.NoError(t, err, "Failed to register collector")

	// Use promhttp handler with the test registry
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	server := httptest.NewServer(handler)
	defer server.Close()

	// 3. Start the initial background refresh (which will block in the mock)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Timeout for the background task itself
		defer cancel()
		slog.Info("Test: Starting background RefreshResourcesAsync")
		// Use the collector's method which internally calls discovery.RefreshResources
		// and potentially metrics.ExportMetrics
		err := testCollector.RefreshResourcesAsync(ctx)
		if err != nil {
			slog.Error("Test: Background RefreshResourcesAsync failed", "error", err)
		} else {
			slog.Info("Test: Background RefreshResourcesAsync finished")
		}
	}()

	// Wait briefly for the background refresh to start and hit the blocking point
	select {
	case <-mockDiscovery.refreshStarted:
		slog.Info("Test: Mock discovery refresh started signal received.")
	case <-time.After(2 * time.Second): // Timeout if refresh doesn't start
		t.Fatal("Timed out waiting for mock discovery refresh to start")
	}

	// 4. Make HTTP request while refresh is blocked
	slog.Info("Test: Making GET request to /metrics while refresh is blocked")
	startTime := time.Now()
	resp, err := http.Get(server.URL + "/metrics")
	duration := time.Since(startTime)
	slog.Info("Test: GET request completed", "duration", duration)

	// 5. Assertions
	require.NoError(t, err, "HTTP GET request failed")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP status OK")
	assert.Less(t, duration, 1*time.Second, "HTTP request took too long, should have returned immediately")

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Failed to read response body")
	bodyString := string(bodyBytes)
	slog.Debug("Test: Received /metrics response body", "body", bodyString)

	// Check for presence of basic internal metrics
	assert.Contains(t, bodyString, "confluent_cloud_exporter_up 1", "Response should contain exporter 'up' metric")
	assert.Contains(t, bodyString, "confluent_cloud_exporter_scrape_success 0", "Response should contain scrape success = 0")
	assert.Contains(t, bodyString, "confluent_cloud_exporter_scrape_duration_seconds", "Response should contain scrape duration")
	// Discovered resources gauge might not be present if discovery hasn't returned any resources yet, which is okay for this test.
	// assert.Contains(t, bodyString, "confluent_cloud_exporter_discovered_resources", "Response should contain discovered resources gauge")

	// Check for absence of resource-specific metrics (since refresh hasn't finished)
	assert.NotContains(t, bodyString, "confluent_cloud_kafka_server_request_latency_ms", "Response should NOT contain resource metrics yet")

	// 6. Unblock the background refresh (allow test to clean up)
	slog.Info("Test: Unblocking mock discovery refresh")
	close(mockDiscovery.unblockRefresh)

	// Allow time for the background goroutine to finish
	time.Sleep(100 * time.Millisecond)
}

// Helper to setup logging for debugging tests
// func setupTestLogger(t *testing.T) *slog.Logger {
// 	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
// 		Level: slog.LevelDebug,
// 		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
// 			if a.Key == slog.TimeKey {
// 				a.Value = slog.StringValue(time.Now().Format(time.RFC3339Nano)) // More precise time
// 			}
// 			return a
// 		},
// 	}))
// }
