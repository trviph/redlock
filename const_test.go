package redlock_test

import "time"

// Test duration constants - eliminates magic numbers across all test files.
const (
	// Lock TTL durations
	testTTLShort  = 200 * time.Millisecond // For watch/timing tests
	testTTLMedium = 2 * time.Second        // For extend tests
	testTTLLong   = 10 * time.Second       // For standard lock tests

	// Timeout durations for contexts
	testTimeoutShort  = 200 * time.Millisecond // For quick failure tests
	testTimeoutMedium = 500 * time.Millisecond // For retry/contention tests

	// Retry configuration
	testMaxRetrySmall  = 1 // Minimal retries
	testMaxRetryMedium = 2 // For max retry exceeded tests
	testMinRetryDelay  = 50 * time.Millisecond

	// Concurrency
	testConcurrentClientsSmall = 5  // For distributed lock race tests
	testConcurrentClientsMed   = 10 // For single lock race tests
	testConcurrentContention   = 20 // For watch contention tests

	// Expected values
	testExpectedWinners = 1 // Only one goroutine should win in race tests

	// Timing margins for flaky test prevention
	testTimingMargin = 100 * time.Millisecond
)

// Redis ports for test instances.
const (
	testRedisPort1 = "6379"
	testRedisPort2 = "6380"
	testRedisPort3 = "6381"
)

// quorum returns the quorum size for n locks.
func quorum(n int) int {
	return n/2 + 1
}
