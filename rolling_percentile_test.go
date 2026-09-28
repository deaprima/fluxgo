// rolling_percentile_test.go contains unit tests for the RollingPercentile engine.
package fluxgo

import (
	"sync"
	"testing"
)

func TestRollingPercentileBasicP95(t *testing.T) {
	// 100 values: 1.0, 2.0, ..., 100.0
	// P95 via nearest-rank: index = ceil(0.95*100)-1 = 94 → value 95.0
	rp := NewRollingPercentile(100)
	for i := 1; i <= 100; i++ {
		rp.Update(float64(i))
	}
	p95, _ := rp.Percentiles()
	if p95 != 95.0 {
		t.Errorf("expected P95 = 95.0, got %v", p95)
	}
}

func TestRollingPercentileBasicP99(t *testing.T) {
	// 100 values: 1.0, 2.0, ..., 100.0
	// P99 via nearest-rank: index = ceil(0.99*100)-1 = 98 → value 99.0
	rp := NewRollingPercentile(100)
	for i := 1; i <= 100; i++ {
		rp.Update(float64(i))
	}
	_, p99 := rp.Percentiles()
	if p99 != 99.0 {
		t.Errorf("expected P99 = 99.0, got %v", p99)
	}
}

func TestRollingPercentileSlidingWindowEvictsOldData(t *testing.T) {
	// window size = 5, insert 1..5 (all low), then insert 1000 five times
	// after eviction, window should only contain 1000s → P95 = 1000
	rp := NewRollingPercentile(5)
	for i := 1; i <= 5; i++ {
		rp.Update(float64(i))
	}
	for i := 0; i < 5; i++ {
		rp.Update(1000.0)
	}
	p95, _ := rp.Percentiles()
	if p95 != 1000.0 {
		t.Errorf("expected old data to be evicted, P95 = 1000.0, got %v", p95)
	}
}

func TestRollingPercentilePartialWindow(t *testing.T) {
	// only 1 observation — P95 and P99 must both return that value
	rp := NewRollingPercentile(100)
	p95, p99 := rp.Update(42.0)
	if p95 != 42.0 || p99 != 42.0 {
		t.Errorf("expected P95=P99=42.0 for single observation, got P95=%v P99=%v", p95, p99)
	}
}

func TestRollingPercentileEmptyReturnsZero(t *testing.T) {
	rp := NewRollingPercentile(100)
	p95, p99 := rp.Percentiles()
	if p95 != 0 || p99 != 0 {
		t.Errorf("expected 0,0 for empty window, got %v,%v", p95, p99)
	}
}

func TestRollingPercentileResetClearsWindow(t *testing.T) {
	rp := NewRollingPercentile(5)
	for i := 0; i < 5; i++ {
		rp.Update(100.0)
	}
	rp.Reset()
	p95, p99 := rp.Percentiles()
	if p95 != 0 || p99 != 0 {
		t.Errorf("expected 0,0 after reset, got %v,%v", p95, p99)
	}
}

func TestRollingPercentileConcurrentUpdate(t *testing.T) {
	rp := NewRollingPercentile(100)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(v float64) {
			defer wg.Done()
			rp.Update(v)
		}(float64(i))
	}
	wg.Wait()
	// no panic or deadlock — race detector will catch mutex violations
	rp.Percentiles()
}

func TestRollingPercentileCurrent_EqualsPercentiles(t *testing.T) {
	rp := NewRollingPercentile(10)
	for i := 1; i <= 10; i++ {
		rp.Update(float64(i))
	}
	p95a, p99a := rp.Percentiles()
	p95b, p99b := rp.Current()
	if p95a != p95b || p99a != p99b {
		t.Errorf("Current() != Percentiles(): got (%v,%v) vs (%v,%v)", p95b, p99b, p95a, p99a)
	}
}

func TestRollingPercentileCurrent_EmptyReturnsZero(t *testing.T) {
	rp := NewRollingPercentile(10)
	p95, p99 := rp.Current()
	if p95 != 0 || p99 != 0 {
		t.Errorf("expected (0, 0) for empty window, got (%v, %v)", p95, p99)
	}
}
