package intracom

import (
	"sync"
)

// channel represents a single channel topic which holds all subscriptions to that topic
type channel[T any] struct {
	consumers map[string]*Consumer[T]
	// consumer map mutex
	cmu *sync.RWMutex

	lastMessage *T
	// last messages mutex
	lmu *sync.RWMutex
}

func newChannel[T any]() *channel[T] {
	return &channel[T]{
		consumers:   make(map[string]*Consumer[T]),
		cmu:         new(sync.RWMutex),
		lastMessage: nil,
		lmu:         new(sync.RWMutex),
	}
}

func (ch *channel[T]) message() T {
	ch.lmu.RLock()
	defer ch.lmu.RUnlock()
	return *ch.lastMessage
}

func (ch *channel[T]) get(id string) (*Consumer[T], bool) {
	ch.cmu.RLock()
	defer ch.cmu.RUnlock()
	consumer, exists := ch.consumers[id]
	return consumer, exists
}

func (ch *channel[T]) len() int {
	ch.cmu.RLock()
	defer ch.cmu.RUnlock()
	return len(ch.consumers)
}

func (ch *channel[T]) broadcast(message T) {
	// write lock the last message
	ch.lmu.Lock()
	ch.lastMessage = &message
	ch.lmu.Unlock()

	// read lock the consumer map
	ch.cmu.RLock()
	for _, consumer := range ch.consumers {
		consumer.send(message)
	}
	ch.cmu.RUnlock()
}

func (ch *channel[T]) subscribe(id string, consumer *Consumer[T]) {
	c, exists := ch.get(id)
	if !exists {
		// write lock the consumers map
		ch.cmu.Lock()
		c = consumer
		ch.consumers[id] = c
		ch.cmu.Unlock()
	}

	// read lock the last message
	ch.lmu.RLock()
	last := ch.lastMessage
	ch.lmu.RUnlock()

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

	ch.cmu.Lock()
	defer ch.cmu.Unlock()
	// channel map cleanup
	delete(ch.consumers, id)

}

// unsubscribe will remove a consumer from the channel map by its id
func (ch *channel[T]) close() {
	// read lock consumer map while we close
	ch.cmu.RLock()
	for _, consumer := range ch.consumers {
		consumer.close()
	}
	ch.cmu.RUnlock()

	// write lock last message
	ch.lmu.Lock()
	ch.lastMessage = nil
	ch.lmu.Unlock()

	// write lock consumer map
	ch.cmu.Lock()
	ch.consumers = make(map[string]*Consumer[T])
	ch.cmu.Unlock()
}
