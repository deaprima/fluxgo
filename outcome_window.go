// outcome_window.go implements OutcomeWindow, a fixed-size count-based
// sliding window of request outcomes used to compute the failure rate
// evaluated by the state machine.
package fluxgo

import "sync"

type OutcomeWindow struct {
	mu		sync.Mutex
	buf 	[]bool
	next	int
	count	int
	failures	int
}

// NewOutcomeWindow returns a OutcomeWindow that retains the last size outcomes
func NewOutcomeWindow(size int) *OutcomeWindow {
	return &OutcomeWindow{buf: make([]bool, size)}
}

// Add records a single request outcome
func (w *OutcomeWindow) Add(failed bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count == len(w.buf) {
		if w.buf[w.next]{
			w.failures--
		}
	}else{
		w.count++
	}

	w.buf[w.next] = failed
	if failed {
		w.failures++	
	}

	w.next = (w.next + 1) % len(w.buf)
}

// Stats returns the failure rate and the number of outcomes currently held
// in the window, read under a single lock so both values are consistent.
func (w *OutcomeWindow) Stats() (failureRate float64, count int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.count == 0 {
		return 0, 0
	}
	return float64(w.failures) / float64(w.count), w.count
}

// Reset discards all outcomes held in the window
func (w *OutcomeWindow) Reset(){
	w.mu.Lock()
	defer w.mu.Unlock()
	w.next = 0
	w.count = 0
	w.failures = 0
}