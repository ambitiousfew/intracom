package intracom

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Consumer is an individual consumer subscription. It holds the channel for the consumer.
type Consumer[T any] struct {
	ch      chan T
	bufSize int

	closed *atomic.Bool

	mu *sync.RWMutex
}

func newConsumer[T any](bufSize int) *Consumer[T] {
	if bufSize < 1 {
		bufSize = 1
	}

	ch := make(chan T, bufSize)
	return &Consumer[T]{
		ch:      ch,
		bufSize: bufSize,
		mu:      new(sync.RWMutex),
		closed:  new(atomic.Bool),
	}
}

// Fetch will attempt to retrieve a batch of messages up to the size if able or until timeout is reached.
func (c *Consumer[T]) Fetch(size int, duration time.Duration) ([]T, bool) {
	batch := make([]T, 0)
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
		return msg, true
	}
}

func (c *Consumer[T]) send(message T) {
	if c.closed.Load() {
		// if we are closed, dont try to send.
		return
	}

	select {
	// buffer not full
	case c.ch <- message:
	default:
		// buffer was full
		<-c.ch          // read 1
		c.ch <- message // send 1
	}
}

func (c *Consumer[T]) close() {
	if c.closed.Swap(true) {
		// if we swap to true and old val was false we arent closed yet.
		close(c.ch)
	}
}
