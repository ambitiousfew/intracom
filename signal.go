package intracom

import "sync"

// signal is a struct that acts as a way to acknowledge completion
// so a worker running in a go routine can be signaled to stop.
// it is meant to be attached to another instance that performs
// work within a goroutine. calling close() signals close against the stopC
// channel so that a goroutines work can end and completed is marked as true.
type signal struct {
	stopC  chan struct{}
	closed bool
	mu     *sync.RWMutex
}

func newSignal() signal {
	return signal{
		stopC:  make(chan struct{}),
		closed: false,
		mu:     new(sync.RWMutex),
	}
}

func (s signal) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.stopC)
	}
}
