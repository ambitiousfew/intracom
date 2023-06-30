package intracom

import (
	"sync"
)

// channel represents a single channel topic which holds all subscriptions to that topic
type channel[T any] struct {
	consumers   map[string]*Consumer[T]
	lastMessage *T
	mu          *sync.RWMutex
}

func newChannel[T any]() *channel[T] {
	return &channel[T]{
		consumers:   make(map[string]*Consumer[T]),
		lastMessage: nil,
		mu:          new(sync.RWMutex),
	}
}

func (ch *channel[T]) message() T {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return *ch.lastMessage
}

func (ch *channel[T]) get(id string) (*Consumer[T], bool) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	consumer, exists := ch.consumers[id]
	return consumer, exists
}

func (ch *channel[T]) len() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.consumers)
}

func (ch *channel[T]) broadcast(message T) {
	// write lock the last message
	ch.mu.Lock()
	ch.lastMessage = &message
	ch.mu.Unlock()

	// read lock the consumer map
	ch.mu.RLock()
	for _, consumer := range ch.consumers {
		consumer.send(message)
	}
	ch.mu.RUnlock()
}

func (ch *channel[T]) subscribe(id string, consumer *Consumer[T]) {
	c, exists := ch.get(id)
	if !exists {
		// write lock the consumers map
		ch.mu.Lock()
		c = consumer
		ch.consumers[id] = c
		ch.mu.Unlock()
	}

	// read lock the last message
	ch.mu.RLock()
	last := ch.lastMessage
	ch.mu.RUnlock()

	if last != nil {
		c.send(*last)
	}

	return

}

// unsubscribe will remove a consumer from the channel map by its id
func (ch *channel[T]) unsubscribe(id string) {
	consumer, exists := ch.get(id)
	if !exists {
		return
	}
	// consumer cleanup
	consumer.close()

	ch.mu.Lock()
	defer ch.mu.Unlock()
	// channel map cleanup
	delete(ch.consumers, id)

}

// unsubscribe will remove a consumer from the channel map by its id
func (ch *channel[T]) close() {
	// read lock consumer map while we close
	ch.mu.RLock()
	for _, consumer := range ch.consumers {
		consumer.close()
	}
	ch.mu.RUnlock()

	// write lock last message
	ch.mu.Lock()
	ch.lastMessage = nil
	ch.mu.Unlock()

	// write lock consumer map
	ch.mu.Lock()
	ch.consumers = make(map[string]*Consumer[T])
	ch.mu.Unlock()
}
