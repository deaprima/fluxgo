// rolling_percentile.go implements a sliding-window rolling percentile engine
// that computes P95 and P99 latency from the most recent N observations.
//
// Algorithm: circular buffer with sort-per-update.
// Time complexity: O(n log n) per Update, where n is the window size.
// Space complexity: O(n).
package fluxgo

import (
	"math"
	"sort"
	"sync"
)

// RollingPercentile computes P95 and P99 latency percentiles over a sliding
// window of the most recent N observations.
type RollingPercentile struct {
	window	[]float64
	size	int 
	count	int 
	head	int 
	mu		sync.RWMutex
}

// NewRollingPercentile returns a new RollingPercentile engine with the
// given window size. windowSize is typically sourced from Config.WindowSize.
func NewRollingPercentile(windowSize int) *RollingPercentile {
	return &RollingPercentile{
		window: make([]float64, windowSize),
		size:	windowSize,
	}
}

// Update records a new latency observation and returns the current P95 and P99 computed from sliding window.
func (r *RollingPercentile) Update(latency float64)(p95, p99 float64){
	r.mu.Lock()
	defer r.mu.Unlock()

	r.window[r.head] = latency
	r.head = (r.head + 1) % r.size
	if r.count < r.size {
		r.count++
	}

	sorted := make([]float64, r.count)
	copy(sorted, r.window[:r.count])
	sort.Float64s(sorted)

	return percentile(sorted, 95), percentile(sorted, 99)
}

// Percentiles returns the current P95 and P99 without recording a new
// observation.
func (r *RollingPercentile) Percentiles() (p95, p99 float64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return 0, 0
	}
	sorted := make([]float64, r.count)
	copy(sorted, r.window[:r.count])
	sort.Float64s(sorted)
	return percentile(sorted, 95), percentile(sorted, 99)
}
// Reset clears all observations from the sliding window.
func (r *RollingPercentile) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.head = 0
	r.count = 0
}

// percentile returns the value at percentile p in a sorted slice using the
// nearest-rank method. p must be in the range [0, 100].
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	index := int(math.Ceil(p/100.0*float64(n))) - 1
	if index < 0 {
		index = 0
	}
	if index >= n {
		index = n - 1
	}
	return sorted[index]
}
