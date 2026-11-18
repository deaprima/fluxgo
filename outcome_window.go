// outcome_window.go implements OutcomeWindow, a fixed-size count-based
// sliding window of request outcomes used to compute the failure rate
// evaluated by the state machine.

package fluxgo

import "sync"

// outcome is the result of one request as held by OutcomeWindow.
type outcome uint8

const (
	outcomeSuccess outcome = iota
	// outcomeSlow is a successful request classified as slow. It counts as
	// a failure, but not as an error.
	outcomeSlow
	// outcomeError is a request that IsSuccessful reported as failed.
	outcomeError
)

// OutcomeWindow is a fixed-size sliding window of the most recent request
// outcomes. Each transition of the circuit moves it to a new generation, which
// empties the window, and outcomes from older generations are ignored. It is
// safe for concurrent use.
type OutcomeWindow struct {
	mu         sync.Mutex
	buf        []outcome
	next       int
	count      int
	failures   int // slow requests and errors
	errors     int
	generation uint64
}

// NewOutcomeWindow returns a OutcomeWindow that retains the last size outcomes
func NewOutcomeWindow(size int) *OutcomeWindow {
	return &OutcomeWindow{buf: make([]outcome, size)}
}

// Add records a single request outcome belonging to the given generation; a
// failed outcome counts as an error. Outcomes from a generation older than
// the window's current generation are ignored. When the window is full, the
// oldest outcome is evicted before the new one is stored.
func (w *OutcomeWindow) Add(generation uint64, failed bool) {
	if failed {
		w.add(generation, outcomeError)
	} else {
		w.add(generation, outcomeSuccess)
	}
}

// add records an outcome of the given kind, with the same rules as Add.
func (w *OutcomeWindow) add(generation uint64, o outcome) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if generation < w.generation {
		return
	}
	if generation > w.generation {
		w.generation = generation
		w.clear()
	}
	if w.count == len(w.buf) {
		w.untally(w.buf[w.next])
	} else {
		w.count++
	}
	w.buf[w.next] = o
	w.tally(o)
	w.next = (w.next + 1) % len(w.buf)
}

// tally adds o to the failure and error counters. It must be called with
// w.mu held.
func (w *OutcomeWindow) tally(o outcome) {
	if o != outcomeSuccess {
		w.failures++
	}
	if o == outcomeError {
		w.errors++
	}
}

// untally removes o from the failure and error counters. It must be called
// with w.mu held.
func (w *OutcomeWindow) untally(o outcome) {
	if o != outcomeSuccess {
		w.failures--
	}
	if o == outcomeError {
		w.errors--
	}
}

// Advance moves the window to the given generation and discards all held
// outcomes. It is a no-op if generation is not newer than the current one.
func (w *OutcomeWindow) Advance(generation uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if generation > w.generation {
		w.generation = generation
		w.clear()
	}
}

// Stats returns the failure rate and the number of outcomes currently held
// in the window, read under a single lock so both values are consistent.
// Slow requests count as failures.
func (w *OutcomeWindow) Stats() (failureRate float64, count int) {
	failureRate, _, count = w.stats()
	return failureRate, count
}

// stats returns the failure rate, the error rate (failures that are not slow
// requests), and the number of outcomes held in the window, read under a
// single lock.
func (w *OutcomeWindow) stats() (failureRate, errorRate float64, count int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.count == 0 {
		return 0, 0, 0
	}
	n := float64(w.count)
	return float64(w.failures) / n, float64(w.errors) / n, w.count
}

// Reset discards all outcomes held in the window
func (w *OutcomeWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clear()
}

// clear empties the window. It must be called with w.mu held.
func (w *OutcomeWindow) clear() {
	w.next = 0
	w.count = 0
	w.failures = 0
	w.errors = 0
}
