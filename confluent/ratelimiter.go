package confluent

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ResponseAwareRateLimiter extends RateLimiter with the ability to adapt based on HTTP responses
type ResponseAwareRateLimiter interface {
	RateLimiter
	UpdateFromResponse(resp *http.Response)
}

// AdaptiveRateLimiter implements a rate limiter that adjusts based on Confluent API responses
type AdaptiveRateLimiter struct {
	// Base limiter (fallback when no header info)
	baseLimiter *rate.Limiter

	// Dynamic limiter adjusted by response headers
	dynamicLimiter *rate.Limiter

	// Mutex to protect state updates
	mu sync.RWMutex

	// Timestamp when the rate limit window resets
	resetTime time.Time

	// Whether we're currently in a throttled state
	throttled bool

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewAdaptiveRateLimiter creates a new adaptive rate limiter
func NewAdaptiveRateLimiter(baseRate rate.Limit, burst int) *AdaptiveRateLimiter {
	ctx, cancel := context.WithCancel(context.Background())
	rl := &AdaptiveRateLimiter{
		baseLimiter:    rate.NewLimiter(baseRate, burst),
		dynamicLimiter: rate.NewLimiter(baseRate, burst),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start background cleanup task
	go rl.cleanup()

	return rl
}

// Wait blocks until a token is available or ctx is done
func (rl *AdaptiveRateLimiter) Wait(ctx context.Context) error {
	rl.mu.RLock()
	throttled := rl.throttled
	resetTime := rl.resetTime
	rl.mu.RUnlock()

	// If throttled, wait until reset time or context is done
	if throttled && !resetTime.IsZero() && time.Now().Before(resetTime) {
		waitDuration := time.Until(resetTime)
		timer := time.NewTimer(waitDuration)
		defer timer.Stop()

		select {
		case <-timer.C:
			// Reset period completed
			rl.mu.Lock()
			rl.throttled = false
			rl.mu.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Use dynamic limiter
	rl.mu.RLock()
	limiter := rl.dynamicLimiter
	rl.mu.RUnlock()

	return limiter.Wait(ctx)
}

// UpdateFromResponse updates the rate limiter based on response headers
func (rl *AdaptiveRateLimiter) UpdateFromResponse(resp *http.Response) {
	if resp == nil {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Check for 429 status and Retry-After header
	if resp.StatusCode == http.StatusTooManyRequests {
		rl.throttled = true

		// Try Retry-After header first (most accurate)
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				rl.resetTime = time.Now().Add(time.Duration(seconds) * time.Second)
				return
			}
		}
	}

	// Try to get rate limit information
	limit := rl.parseIntHeader(resp.Header, "X-RateLimit-Limit")
	remaining := rl.parseIntHeader(resp.Header, "X-RateLimit-Remaining")
	reset := rl.parseIntHeader(resp.Header, "X-RateLimit-Reset")

	if limit > 0 && reset > 0 {
		// Calculate the appropriate rate
		newRate := rate.Limit(float64(limit) / float64(reset))

		// Only adjust if the new rate is lower than base rate
		if newRate < rate.Limit(rl.baseLimiter.Limit()) {
			rl.dynamicLimiter.SetLimit(newRate)
			rl.dynamicLimiter.SetBurst(int(limit) / 2) // Half of limit for burst
		}

		// Calculate reset time
		rl.resetTime = time.Now().Add(time.Duration(reset) * time.Second)

		// If we have fewer than 10% of requests remaining, throttle
		if remaining < limit/10 {
			rl.throttled = true
		}
	}
}

// parseIntHeader parses an integer header
func (rl *AdaptiveRateLimiter) parseIntHeader(header http.Header, name string) int {
	if val := header.Get(name); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return 0
}

// cleanup periodically resets the rate limiter to base values
func (rl *AdaptiveRateLimiter) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			// If reset time has passed, revert to base limiter
			if !rl.resetTime.IsZero() && time.Now().After(rl.resetTime) {
				rl.dynamicLimiter.SetLimit(rl.baseLimiter.Limit())
				rl.dynamicLimiter.SetBurst(rl.baseLimiter.Burst())
				rl.throttled = false
				rl.resetTime = time.Time{}
			}
			rl.mu.Unlock()
		case <-rl.ctx.Done():
			return
		}
	}
}

// Close shuts down the rate limiter
func (rl *AdaptiveRateLimiter) Close() {
	rl.cancel()
}
