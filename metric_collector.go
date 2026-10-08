// metric_collector.go implements MetricCollector, which records per-request
// latency and success status, forwarding data to the EWMA, rolling
// percentile, and outcome window engines.
package fluxgo

import (
	"sync/atomic"
	"time"
)

// MetricCollector aggregates per-request observations and forwards them to
// the EWMA, rolling percentile, and outcome window engines. 
type MetricCollector struct {
	totalRequests	int64
	totalFailures	int64
	degradedRequests	int64

	ewma	*EWMA
	rolling	*RollingPercentile
	outcomes *OutcomeWindow
}

// NewMetricCollector return a MetricCollector wired to given engines.
func NewMetricCollector(ewma *EWMA, rolling *RollingPercentile, outcomes *OutcomeWindow) *MetricCollector {
	return &MetricCollector{
		ewma: ewma,
		rolling: rolling,
		outcomes: outcomes,
	}
}

// Record incorporates a single request observation. latency is forwarded to
// both signal engines and the outcome is added to the outcome window.
func  (m *MetricCollector) Record(latency time.Duration, success bool) (p95, p99 float64){
	ms := latency.Seconds() * 1000
	m.ewma.Update(ms)
	p95, p99 = m.rolling.Update(ms)
	m.outcomes.Add(!success)
	atomic.AddInt64(&m.totalRequests, 1)
	if !success {
		atomic.AddInt64(&m.totalFailures, 1)
	}
	return
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

// ResetWindow discards all outcomes in the outcome window. Lifetime counters
// and latency signals are preserved.
func (m *MetricCollector) ResetWindow() {
	m.outcomes.Reset()
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