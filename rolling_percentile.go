// rolling_percentile.go implements a sliding-window rolling percentile engine
// that computes P95 and P99 latency from the most recent N observations.
//
// Algorithm: circular buffer with in-place binary insertion sort.
// Time complexity: O(n) per Update (binary search O(log n) + memmove O(n)).
// Space complexity: O(n) pre-allocated, zero-allocation at runtime.
package fluxgo

import (
	"math"
	"sort"
	"sync"
)

// RollingPercentile computes P95 and P99 latency percentiles over a sliding
// window of the most recent N observations.
type RollingPercentile struct {
	window    []float64
	sortedBuf []float64
	size      int
	count     int
	head      int
	p95       float64
	p99       float64
	mu        sync.RWMutex
}

// NewRollingPercentile returns a new RollingPercentile engine with the
// given window size. windowSize is typically sourced from Config.WindowSize.
func NewRollingPercentile(windowSize int) *RollingPercentile {
	return &RollingPercentile{
		window:    make([]float64, windowSize),
		sortedBuf: make([]float64, windowSize),
		size:      windowSize,
	}
}

// Update records a new latency observation and returns the current P95 and P99 computed from sliding window.
func (r *RollingPercentile) Update(latency float64) (p95, p99 float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size <= 0 {
		return 0, 0
	}

	if r.count < r.size {
		idx := sort.Search(r.count, func(i int) bool {
			return r.sortedBuf[i] >= latency
		})
		copy(r.sortedBuf[idx+1:r.count+1], r.sortedBuf[idx:r.count])
		r.sortedBuf[idx] = latency
		r.count++
	} else {
		oldVal := r.window[r.head]
		oldIdx := sort.Search(r.size, func(i int) bool {
			return r.sortedBuf[i] >= oldVal
		})
		if oldIdx < r.size {
			copy(r.sortedBuf[oldIdx:r.size-1], r.sortedBuf[oldIdx+1:r.size])
		}

		newIdx := sort.Search(r.size-1, func(i int) bool {
			return r.sortedBuf[i] >= latency
		})
		copy(r.sortedBuf[newIdx+1:r.size], r.sortedBuf[newIdx:r.size-1])
		r.sortedBuf[newIdx] = latency
	}

	r.window[r.head] = latency
	r.head = (r.head + 1) % r.size

	r.p95 = percentile(r.sortedBuf[:r.count], 95)
	r.p99 = percentile(r.sortedBuf[:r.count], 99)
	return r.p95, r.p99
}

// Percentiles returns the current P95 and P99 without recording a new observation.
func (r *RollingPercentile) Percentiles() (p95, p99 float64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.p95, r.p99
}

// Current returns the current P95 and P99 without recording a new observation.
// It is equivalent to Percentiles and is provided as a convenience alias used by
// the slow-request classifier and observability accessors.
func (r *RollingPercentile) Current() (p95, p99 float64) {
	return r.Percentiles()
}

// Reset clears all observations from the sliding window.
func (r *RollingPercentile) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.head = 0
	r.count = 0
	r.p95 = 0
	r.p99 = 0
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
