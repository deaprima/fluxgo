// fakeclock_test.go provides a manually advanced clock shared by tests that
// need deterministic time control without real sleeps
package fluxgo

import (
	"sync"
	"time"
)

// fakeClock is a manually advanced clock for deterministic time-based tests
type fakeClock struct {
	mu sync.Mutex
	t time.Time
}

// newFakeClock returns a fakeClock starting at the Unix epoch
func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(0,0)}
}

// Now returns the current fake time
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
} 

// Advance moves the fake time forward by d
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}