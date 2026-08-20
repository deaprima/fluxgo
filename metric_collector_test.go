// metric_collector_test.go tests MetricCollector for correct counter behavior
// and thread-safety under concurrent access.
package fluxgo

import (
    "sync"
    "testing"
    "time"
)

func TestMetricCollector_Record_CountsAccumulate(t *testing.T) {
    mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10))

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
    mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10))
    if got := mc.FailureRate(); got != 0 {
        t.Errorf("FailureRate() = %f, want 0 (no requests)", got)
    }
}

func TestMetricCollector_Reset_ClearsCounters(t *testing.T) {
    mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(10))
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
    mc := NewMetricCollector(NewEWMA(0.3), NewRollingPercentile(100))
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
