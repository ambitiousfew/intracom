package intracom

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Consumer is an individual consumer subscription. It holds the channel for the consumer.
type Consumer[T any] struct {
	ch       chan T
	bufSize  int
	received *atomic.Int64

	closed bool
	mu     *sync.RWMutex
}

func newConsumer[T any](bufSize int) *Consumer[T] {
	if bufSize < 1 {
		bufSize = 1
	}

	ch := make(chan T, bufSize)
	return &Consumer[T]{
		ch:       ch,
		bufSize:  bufSize,
		received: new(atomic.Int64),
		mu:       new(sync.RWMutex),
	}
}

// Fetch will attempt to retrieve a batch of messages up to the size if able or until timeout is reached.
func (c *Consumer[T]) Fetch(size int, duration time.Duration) ([]T, bool) {
	batch := make([]T, 0)
	defer c.received.Add(int64(len(batch) * -1))
	timeout := time.NewTimer(duration)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return batch, len(batch) == 0
		case msg := <-c.ch:
			batch = append(batch, msg)
			if len(batch) == size {
				return batch, true
			}
		}
	}
}

// FetchWithContext will retrieve a batch of messages up to the size and wait forever or until context is cancelled.
func (c *Consumer[T]) FetchWithContext(ctx context.Context, size int) ([]T, bool) {
	batch := make([]T, 0)
	defer c.received.Add(int64(len(batch) * -1))

	for {
		select {
		case <-ctx.Done():
			return nil, false
		case msg := <-c.ch:
			batch = append(batch, msg)
			if len(batch) == size {
				return batch, true
			}
		}
	}
}

// NextMsg will wait to receive next message but only up to the duration given.
func (c *Consumer[T]) NextMsg(duration time.Duration) (T, bool) {
	var empty T

	timeout := time.NewTimer(duration)
	defer timeout.Stop()

	select {
	case <-timeout.C:
		return empty, false
	case msg := <-c.ch:
		c.received.Add(-1)
		return msg, true
	}
}

// NextMsgWithContext will wait to receive next message forever or until context is cancelled.
func (c *Consumer[T]) NextMsgWithContext(ctx context.Context) (T, bool) {
	var empty T
	select {
	case <-ctx.Done():
		return empty, false
	case msg := <-c.ch:
		c.received.Add(-1)
		return msg, true
	}
}

func (c *Consumer[T]) send(message T) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()

	if closed {
		return
	}

	// if we have reached our buffer size we want to discard old
	if count := c.received.Load(); count == int64(c.bufSize) {
		// we dont have room
		<-c.ch          // read 1
		c.ch <- message // push 1
	} else {
		// we have room
		c.ch <- message // push message
		c.received.Add(1)
	}
}

func (c *Consumer[T]) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.ch)
	}
}
