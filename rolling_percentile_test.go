// rolling_percentile_test.go contains unit tests for the RollingPercentile engine.
package fluxgo

import (
	"sort"
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

func TestRollingPercentileCount_CapsAtWindowSizeAndResets(t *testing.T) {
	rp := NewRollingPercentile(5)
	for i := 1; i <= 3; i++ {
		rp.Update(float64(i))
	}
	if got := rp.Count(); got != 3 {
		t.Errorf("Count() = %d after 3 updates, want 3", got)
	}

	for i := 0; i < 10; i++ {
		rp.Update(1)
	}
	if got := rp.Count(); got != 5 {
		t.Errorf("Count() = %d after overflowing the window, want 5", got)
	}

	rp.Reset()
	if got := rp.Count(); got != 0 {
		t.Errorf("Count() = %d after Reset, want 0", got)
	}
}

func BenchmarkRollingPercentile_Update(b *testing.B) {
	rp := NewRollingPercentile(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rp.Update(float64(i % 500))
	}
}

func TestRollingPercentile_ZeroSizeWindowIsInert(t *testing.T) {
	rp := NewRollingPercentile(0)
	if p95, p99 := rp.Update(10); p95 != 0 || p99 != 0 {
		t.Errorf("Update() = (%v, %v), want (0, 0) for a zero-size window", p95, p99)
	}
	if got := rp.Count(); got != 0 {
		t.Errorf("Count() = %d, want 0", got)
	}
}

func TestPercentile_EdgeCases(t *testing.T) {
	sorted := []float64{1, 2, 3}
	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
	}{
		{"empty slice", nil, 95, 0},
		{"zero percentile clamps to the first value", sorted, 0, 1},
		{"above 100 clamps to the last value", sorted, 150, 3},
	}
	for _, tt := range tests {
		if got := percentile(tt.sorted, tt.p); got != tt.want {
			t.Errorf("%s: percentile() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// FuzzRollingPercentile compares the in-place sorted buffer with a naive model
// that re-sorts the last windowSize values. The first byte selects the window
// size; every other byte is a latency, except 255, which resets the window.
func FuzzRollingPercentile(f *testing.F) {
	f.Add([]byte{5, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{3, 9, 9, 9, 1, 1, 9, 1})
	f.Add([]byte{4, 7, 255, 2, 2, 255, 255, 8})
	f.Add([]byte{0, 200, 100, 200, 100, 200})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		windowSize := int(data[0])%100 + 1
		rp := NewRollingPercentile(windowSize)
		var model []float64

		for _, b := range data[1:] {
			if b == 255 {
				rp.Reset()
				model = nil
				continue
			}
			v := float64(b)
			gotP95, gotP99 := rp.Update(v)
			model = append(model, v)
			if len(model) > windowSize {
				model = model[1:]
			}

			sorted := append([]float64(nil), model...)
			sort.Float64s(sorted)
			wantP95, wantP99 := percentile(sorted, 95), percentile(sorted, 99)
			if gotP95 != wantP95 || gotP99 != wantP99 {
				t.Fatalf("window %d, values %v: got (%v, %v), want (%v, %v)",
					windowSize, model, gotP95, gotP99, wantP95, wantP99)
			}
			if got := rp.Count(); got != len(model) {
				t.Fatalf("Count() = %d, want %d", got, len(model))
			}
		}
	})
}
