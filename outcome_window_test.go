package fluxgo

import (
	"sync"
	"testing"
)

func TestOutcomeWindow_EmptyReturnsZero(t *testing.T) {
	w := NewOutcomeWindow(4)
	rate, n := w.Stats()
	if rate != 0 || n != 0 {
		t.Fatalf("expected (0, 0), got (%v, %d)", rate, n)
	}
}

func TestOutcomeWindow_EvictsOldestOutcome(t *testing.T) {
	w := NewOutcomeWindow(4)
	w.Add(true)
	w.Add(true)
	w.Add(false)
	w.Add(false)
	if rate, _ := w.Stats(); rate != 0.5 {
		t.Fatalf("expected 0.5, got %v", rate)
	}
	w.Add(false)
	if rate, n := w.Stats(); rate != 0.25 || n != 4 {
		t.Fatalf("expected (0.25, 4) after evicting one failure, got (%v, %d)", rate, n)
	}
	w.Add(false)
	if rate, _ := w.Stats(); rate != 0 {
		t.Fatalf("expected 0 after evicting both failures, got %v", rate)
	}
}

func TestOutcomeWindow_ResetClearsOutcomes(t *testing.T) {
	w := NewOutcomeWindow(4)
	for i := 0; i < 4; i++ {
		w.Add(true)
	}
	w.Reset()
	w.Add(false)
	if rate, n := w.Stats(); rate != 0 || n != 1 {
		t.Fatalf("expected (0, 1) after reset, got (%v, %d)", rate, n)
	}
}

func TestOutcomeWindow_ConcurrentAdd(t *testing.T) {
	w := NewOutcomeWindow(100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(failed bool) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.Add(failed)
			}
		}(i%2 == 0)
	}
	wg.Wait()
	if _, n := w.Stats(); n != 100 {
		t.Fatalf("expected window to hold 100 outcomes, got %d", n)
	}
}