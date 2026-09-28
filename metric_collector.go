// metric_collector.go implements MetricCollector, which records per-request
// latency and success status, forwarding data to the EWMA and rolling
// percentile engines.
package fluxgo

import (
	"sync/atomic"
	"time"
)

// MetricCollector aggregates per-request observations and forwards them to
// the EWMA and rolling percentile engines. Counters are updated atomically
// to minimize contention under high concurrency.
type MetricCollector struct {
	totalRequests int64
	totalFailures int64
	degradedRequests int64

	ewma    *EWMA
	rolling *RollingPercentile
}

// NewMetricCollector returns a MetricCollector wired to the given engines.
func NewMetricCollector(ewma *EWMA, rolling *RollingPercentile) *MetricCollector {
	return &MetricCollector{
		ewma:    ewma,
		rolling: rolling,
	}
}

// Record incorporates a single request observation. latency is forwarded to
// both signal engines; success=false increments the failure counter.
func (m *MetricCollector) Record(latency time.Duration, success bool) {
	ms := latency.Seconds() * 1000 // convert to milliseconds for signal engines
	m.ewma.Update(ms)
	m.rolling.Update(ms)
	atomic.AddInt64(&m.totalRequests, 1)
	if !success {
		atomic.AddInt64(&m.totalFailures, 1)
	}
}

// FailureRate returns the ratio of failed requests to total requests observed
// since the last reset. It returns 0 if no requests have been recorded.
func (m *MetricCollector) FailureRate() float64 {
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return 0
	}
	failures := atomic.LoadInt64(&m.totalFailures)
	return float64(failures) / float64(total)
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

// DegradedRequest returns the total number of request classified as degraded
func (m *MetricCollector) DegradedRequests() int64 {
	return atomic.LoadInt64(&m.degradedRequests)
}

// Reset clears all counters and resets both signal engines to their initial state.
func (m *MetricCollector) Reset() {
	atomic.StoreInt64(&m.totalRequests, 0)
	atomic.StoreInt64(&m.totalFailures, 0)
	atomic.StoreInt64(&m.degradedRequests, 0)
	m.ewma.Reset()
	m.rolling.Reset()
}
