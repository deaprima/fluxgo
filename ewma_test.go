// ewma_test.go contains unit tests for the EWMA engine.
package fluxgo

import (
	"sync"
	"testing"
)

func TestEWMAFirstUpdateSetsInitialValue(t *testing.T) {
	e := NewEWMA(0.3)
	result := e.Update(100.0)
	if result != 100.0 {
		t.Errorf("expected first Update to return 100.0, got %v", result)
	}
}

func TestEWMAConvergesToConstantInput(t *testing.T) {
	e := NewEWMA(0.3)
	var result float64
	for i := 0; i < 200; i++ {
		result = e.Update(50.0)
	}
	// after many identical inputs, value should be very close to 50.0
	if result < 49.99 || result > 50.01 {
		t.Errorf("expected EWMA to converge near 50.0, got %v", result)
	}
}

func TestEWMAValueMatchesLastUpdate(t *testing.T) {
	e := NewEWMA(0.3)
	e.Update(80.0)
	e.Update(90.0)
	last := e.Update(70.0)
	if e.Value() != last {
		t.Errorf("Value() = %v, want %v", e.Value(), last)
	}
}

func TestEWMAResetClearsState(t *testing.T) {
	e := NewEWMA(0.3)
	e.Update(100.0)
	e.Reset()
	result := e.Update(50.0)
	// after reset, first update sets value directly
	if result != 50.0 {
		t.Errorf("expected 50.0 after reset, got %v", result)
	}
}

func TestEWMAConcurrentUpdate(t *testing.T) {
	e := NewEWMA(0.3)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Update(float64(i))
		}()
	}
	wg.Wait()
	// no race condition — just verify it doesn't panic or deadlock
	_ = e.Value()
}
