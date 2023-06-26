package intracom

import (
	"sync"
	"sync/atomic"
)

// intraConsumer is an individual consumer subscription. It holds the channel for the consumer.
type intraConsumer[T any] struct {
	signal    signal
	delivered *atomic.Int32 // 0 - undelivered, 1 - delivered
	ch        chan T
	closed    bool
	mu        *sync.RWMutex
}

func newIntraConsumer[T any](ch chan T) *intraConsumer[T] {
	return &intraConsumer[T]{
		ch:        ch,
		delivered: new(atomic.Int32),
		signal:    newSignal(),
		mu:        new(sync.RWMutex),
	}
}

func (c *intraConsumer[T]) send(message T) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return
	}

	if c.delivered.Swap(1) != 0 {
		c.signal.stop()
		c.signal = newSignal()
	} else {
		c.delivered.Add(1)
	}

	go func(s signal) {
		c.mu.RLock()
		if c.closed {
			return
		}
		c.mu.RUnlock()

		select {
		case <-s.stopC:
			// worker being told to stop because a new message is being published.
			// or because we are shutting down.
			return
		case c.ch <- message:
			// worker stopping because successful delivery to the channel.
			return
		}
	}(c.signal)
}

func (c *intraConsumer[T]) close() {
	c.signal.stop()

	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.ch)
	}
	c.mu.Unlock()
}
