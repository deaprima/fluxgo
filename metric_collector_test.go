// metric_collector_test.go tests MetricCollector for correct counter behavior,
// outcome-window based failure rate, and thread-safety under concurrent access.
package fluxgo

import (
	"sync"
	"testing"
	"time"
)

func newTestCollector(size int) *MetricCollector {
	return NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(size), NewOutcomeWindow(size))
}

func TestMetricCollector_RecordOutcome_CountsAccumulate(t *testing.T) {
	mc := newTestCollector(10)

	mc.RecordOutcome(0, true)
	mc.RecordOutcome(0, false)
	mc.RecordOutcome(0, false)

	if got := mc.TotalRequests(); got != 3 {
		t.Errorf("TotalRequests() = %d, want 3", got)
	}
	if got := mc.FailureRate(); got != 2.0/3.0 {
		t.Errorf("FailureRate() = %f, want %f", got, 2.0/3.0)
	}
}

func TestMetricCollector_RecordLatency_FeedsSignals(t *testing.T) {
	mc := newTestCollector(10)

	p95, p99 := mc.RecordLatency(20 * time.Millisecond)

	if got := mc.ewma.Value(); got != 20 {
		t.Errorf("EWMA value = %v, want 20", got)
	}
	if p95 != 20 || p99 != 20 {
		t.Errorf("RecordLatency() = (%v, %v), want (20, 20)", p95, p99)
	}
	if got := mc.TotalRequests(); got != 0 {
		t.Errorf("TotalRequests() = %d, want 0 (latency must not touch outcome counters)", got)
	}
}

func TestMetricCollector_RecordOutcome_DoesNotTouchSignals(t *testing.T) {
	mc := newTestCollector(10)

	mc.RecordOutcome(0, false)

	if got := mc.ewma.Value(); got != 0 {
		t.Errorf("EWMA value = %v, want 0 (outcome must not touch latency signals)", got)
	}
}

func TestMetricCollector_FailureRate_ZeroWhenNoRequests(t *testing.T) {
	mc := newTestCollector(10)
	if got := mc.FailureRate(); got != 0 {
		t.Errorf("FailureRate() = %f, want 0 (no requests)", got)
	}
}

func TestMetricCollector_Reset_ClearsCounters(t *testing.T) {
	mc := newTestCollector(10)
	mc.RecordLatency(10 * time.Millisecond)
	mc.RecordOutcome(0, false)
	mc.Reset()

	if got := mc.TotalRequests(); got != 0 {
		t.Errorf("TotalRequests() after Reset = %d, want 0", got)
	}
	if got := mc.FailureRate(); got != 0 {
		t.Errorf("FailureRate() after Reset = %f, want 0", got)
	}
	if got := mc.ewma.Value(); got != 0 {
		t.Errorf("EWMA value after Reset = %v, want 0", got)
	}
}

func TestMetricCollector_ConcurrentRecord_RaceDetector(t *testing.T) {
	mc := newTestCollector(100)
	const goroutines = 50
	const recordsEach = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < recordsEach; j++ {
				success := j%5 != 0
				if success {
					mc.RecordLatency(time.Duration(j+1) * time.Millisecond)
				}
				mc.RecordOutcome(0, success)
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * recordsEach)
	if got := mc.TotalRequests(); got != want {
		t.Errorf("TotalRequests() = %d, want %d after concurrent access", got, want)
	}
}

func TestMetricCollector_RecordDegraded_Counter(t *testing.T) {
	mc := newTestCollector(10)

	mc.RecordOutcome(0, false)
	mc.RecordDegraded()

	if got := mc.DegradedRequests(); got != 1 {
		t.Errorf("DegradedRequests() = %d, want 1", got)
	}
	if got := mc.TotalFailures(); got != 1 {
		t.Errorf("TotalFailures() = %d, want 1", got)
	}
}

func TestMetricCollector_Reset_ClearsDegradedCounter(t *testing.T) {
	mc := newTestCollector(10)
	mc.RecordDegraded()
	mc.RecordDegraded()
	mc.Reset()

	if got := mc.DegradedRequests(); got != 0 {
		t.Errorf("DegradedRequests() after Reset = %d, want 0", got)
	}
}

// TestMetricCollector_FailureRate_IsWindowed is a regression test for B2:
// FailureRate must reflect only the outcome window, not the lifetime total.
func TestMetricCollector_FailureRate_IsWindowed(t *testing.T) {
	const windowSize = 10
	mc := newTestCollector(windowSize)

	for i := 0; i < 100; i++ {
		mc.RecordOutcome(0, true)
	}
	for i := 0; i < windowSize; i++ {
		mc.RecordOutcome(0, false)
	}

	if got := mc.FailureRate(); got != 1.0 {
		t.Errorf("FailureRate() = %f, want 1.0 (window holds only the last %d failures)", got, windowSize)
	}
	if got := mc.TotalRequests(); got != 100+windowSize {
		t.Errorf("TotalRequests() = %d, want %d (lifetime counter must stay cumulative)", got, 100+windowSize)
	}
}

func TestMetricCollector_AdvanceWindow_PreservesLifetimeCounters(t *testing.T) {
	mc := newTestCollector(10)
	mc.RecordOutcome(0, false)

	mc.AdvanceWindow(1)

	if rate, n := mc.WindowStats(); rate != 0 || n != 0 {
		t.Errorf("WindowStats() after AdvanceWindow = (%v, %d), want (0, 0)", rate, n)
	}
	if got := mc.TotalRequests(); got != 1 {
		t.Errorf("TotalRequests() after AdvanceWindow = %d, want 1 (lifetime counters must be preserved)", got)
	}
}
