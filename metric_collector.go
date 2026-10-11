// metric_collector.go implements MetricCollector, which records per-request
// latency and outcome, forwarding data to the EWMA, rolling percentile, and
// outcome window engines.
package fluxgo

import (
	"sync/atomic"
	"time"
)

// MetricCollector aggregates per-request observations and forwards them to
// the EWMA, rolling percentile, and outcome window engines. Latency and
// outcome are recorded separately so that only latencies of requests that
// actually reached and were served by the downstream feed the latency
// signals. Lifetime counters are updated atomically to minimize contention
// under high concurrency.
type MetricCollector struct {
	totalRequests    int64
	totalFailures    int64
	degradedRequests int64

	ewma     *EWMA
	rolling  *RollingPercentile
	outcomes *OutcomeWindow
}

// NewMetricCollector returns a MetricCollector wired to the given engines.
func NewMetricCollector(ewma *EWMA, rolling *RollingPercentile, outcomes *OutcomeWindow) *MetricCollector {
	return &MetricCollector{
		ewma:     ewma,
		rolling:  rolling,
		outcomes: outcomes,
	}
}

// RecordLatency forwards a latency observation to both signal engines and
// returns the updated P95 and P99 so callers avoid a second sort. It should
// only be called for requests that completed without error.
func (m *MetricCollector) RecordLatency(latency time.Duration) (p95, p99 float64) {
	ms := latency.Seconds() * 1000
	m.ewma.Update(ms)
	return m.rolling.Update(ms)
}

// RecordOutcome adds a request outcome belonging to the given generation to
// the outcome window and updates the lifetime counters.
func (m *MetricCollector) RecordOutcome(generation uint64, success bool) {
	m.outcomes.Add(generation, !success)
	atomic.AddInt64(&m.totalRequests, 1)
	if !success {
		atomic.AddInt64(&m.totalFailures, 1)
	}
}

// FailureRate returns the ratio of failed requests to all requests held in
// the outcome window. It returns 0 if the window is empty.
func (m *MetricCollector) FailureRate() float64 {
	rate, _ := m.outcomes.Stats()
	return rate
}

// WindowStats returns the failure rate and the number of requests currently
// held in the outcome window.
func (m *MetricCollector) WindowStats() (failureRate float64, count int) {
	return m.outcomes.Stats()
}

// AdvanceWindow moves the outcome window to the given generation, discarding
// outcomes from earlier generations. Lifetime counters and latency signals
// are preserved.
func (m *MetricCollector) AdvanceWindow(generation uint64) {
	m.outcomes.Advance(generation)
}

// TotalRequests returns the total number of requests recorded since the last reset.
func (m *MetricCollector) TotalRequests() int64 {
	return atomic.LoadInt64(&m.totalRequests)
}

// TotalFailures returns the total number of failed requests recorded since the last reset.
func (m *MetricCollector) TotalFailures() int64 {
	return atomic.LoadInt64(&m.totalFailures)
}

// RecordDegraded increments the degraded-request counter.
func (m *MetricCollector) RecordDegraded() {
	atomic.AddInt64(&m.degradedRequests, 1)
}

// DegradedRequests returns the total number of requests classified as degraded.
func (m *MetricCollector) DegradedRequests() int64 {
	return atomic.LoadInt64(&m.degradedRequests)
}

// Reset clears all counters, resets both signal engines to their initial
// state, and empties the outcome window.
func (m *MetricCollector) Reset() {
	atomic.StoreInt64(&m.totalRequests, 0)
	atomic.StoreInt64(&m.totalFailures, 0)
	atomic.StoreInt64(&m.degradedRequests, 0)
	m.ewma.Reset()
	m.rolling.Reset()
	m.outcomes.Reset()
}
