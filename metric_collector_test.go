// metric_collector_test.go tests MetricCollector for correct counter behavior,
// outcome-window based failure rate, and thread-safety under concurrent access.
package fluxgo

import (
	"sync"
	"testing"
	"time"
)

func TestMetricCollector_Record_CountsAccumulate(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))

	mc.Record(10*time.Millisecond, true)
	mc.Record(20*time.Millisecond, false)
	mc.Record(30*time.Millisecond, false)

	if got := mc.TotalRequests(); got != 3 {
		t.Errorf("TotalRequests() = %d, want 3", got)
	}
	if got := mc.FailureRate(); got != 2.0/3.0 {
		t.Errorf("FailureRate() = %f, want %f", got, 2.0/3.0)
	}
}

func TestMetricCollector_FailureRate_ZeroWhenNoRequests(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))
	if got := mc.FailureRate(); got != 0 {
		t.Errorf("FailureRate() = %f, want 0 (no requests)", got)
	}
}

func TestMetricCollector_Reset_ClearsCounters(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))
	mc.Record(10*time.Millisecond, false)
	mc.Reset()

	if got := mc.TotalRequests(); got != 0 {
		t.Errorf("TotalRequests() after Reset = %d, want 0", got)
	}
	if got := mc.FailureRate(); got != 0 {
		t.Errorf("FailureRate() after Reset = %f, want 0", got)
	}
}

func TestMetricCollector_ConcurrentRecord_RaceDetector(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(100), NewOutcomeWindow(100))
	const goroutines = 50
	const recordsEach = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsEach; j++ {
				mc.Record(time.Duration(j+1)*time.Millisecond, j%5 != 0)
			}
		}(i)
	}
	wg.Wait()

	want := int64(goroutines * recordsEach)
	if got := mc.TotalRequests(); got != want {
		t.Errorf("TotalRequests() = %d, want %d after concurrent access", got, want)
	}
}

func TestMetricCollector_RecordDegraded_Counter(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))

	mc.Record(10*time.Millisecond, false) // explicit failure
	mc.RecordDegraded()                   // slow request (also counted as failure by caller)

	if got := mc.DegradedRequests(); got != 1 {
		t.Errorf("DegradedRequests() = %d, want 1", got)
	}
	if got := mc.TotalFailures(); got != 1 {
		t.Errorf("TotalFailures() = %d, want 1", got)
	}
}

func TestMetricCollector_Reset_ClearsDegradedCounter(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))
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
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(windowSize), NewOutcomeWindow(windowSize))

	for i := 0; i < 100; i++ {
		mc.Record(time.Millisecond, true) // 100 lifetime successes
	}
	for i := 0; i < windowSize; i++ {
		mc.Record(time.Millisecond, false) // fills the window with failures only
	}

	if got := mc.FailureRate(); got != 1.0 {
		t.Errorf("FailureRate() = %f, want 1.0 (window holds only the last %d failures)", got, windowSize)
	}
	if got := mc.TotalRequests(); got != 100+windowSize {
		t.Errorf("TotalRequests() = %d, want %d (lifetime counter must stay cumulative)", got, 100+windowSize)
	}
}

func TestMetricCollector_ResetWindow_PreservesLatencySignals(t *testing.T) {
	mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10), NewOutcomeWindow(10))
	mc.Record(50*time.Millisecond, false)

	mc.ResetWindow()

	if rate, n := mc.WindowStats(); rate != 0 || n != 0 {
		t.Errorf("WindowStats() after ResetWindow = (%v, %d), want (0, 0)", rate, n)
	}
	if got := mc.TotalRequests(); got != 1 {
		t.Errorf("TotalRequests() after ResetWindow = %d, want 1 (lifetime counters must be preserved)", got)
	}
}