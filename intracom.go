package intracom

import (
	"sync"
)

type Intracom[T any] struct {
	manager *manager[T]
	closed  bool
	mu      *sync.RWMutex
}

func New[T any]() *Intracom[T] {
	return &Intracom[T]{
		mu:      new(sync.RWMutex),
		manager: newManager[T](),
		closed:  false,
	}
}

// Subscribe will register a topic and consumer to that topic with a given channel size (buffer).
func (i *Intracom[T]) Subscribe(topic, consumerID string, chanSize int) *Consumer[T] {
	if chanSize < 1 {
		// do not allow negative channel sizes, though we may want to allow unbuffered.
		chanSize = 1
	}

	channel, exists := i.manager.get(topic)
	if !exists {
		consumer := newConsumer[T](chanSize)
		// if the topic does not yet exist, create an empty subs map for that topic.
		channel := newChannel[T]()
		// add the consumer into the channel topic map
		channel.subscribe(consumerID, consumer)
		// add the new channel to our map of all topics.
		i.manager.add(topic, channel)
		return consumer
	}

	// NOTE: Resubscribes to the same topic + same consumer then will trigger an attempt to resend the
	// most recent published message of that topic back to the consumer channel.
	// Tread carefully with resubscribes across a buffered channel of 1 or more.
	if consumer, exists := channel.get(consumerID); exists {
		// attempt resubscribe, already exists so just places last message on chan
		channel.subscribe(consumerID, consumer)
		return consumer
	}

	// channel topic exists, consumer does not.
	consumer := newConsumer[T](chanSize)
	channel.subscribe(consumerID, consumer)
	return consumer
}

func (i *Intracom[T]) Unsubscribe(topic, consumerID string) {
	channel, exists := i.manager.get(topic)
	if !exists {
		// cannot unsubscribe from a topic that never exists, protect against typo'd names causing nil map
		return
	}

	channel.unsubscribe(consumerID)

	if channel.len() == 0 {
		// if our channel has no more subscribers, cleanup the topic
		i.manager.remove(topic)
	}
}

func (i *Intracom[T]) Publish(topic string, message T) {
	channel, exists := i.manager.get(topic)
	if !exists {
		// if the topic does not yet exist, no need to publish.
		return
	}
	// channel takes care of informing all its consumers.
	channel.broadcast(message)
}

// IsClosed checks to see if Close has been previously called returning a bool of true/false
func (i *Intracom[T]) IsClosed() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.closed
}

// Close is safe to call multiple times it will inform the underlying intracom channel manager
// and all its channels and consumers to stop/close their channels.
// This will effectively cleanup all open channels and remove all subscriptions.
// The intracom instance will not be usable after this, a new instance would be required.
func (i *Intracom[T]) Close() {
	i.mu.RLock()
	closed := i.closed
	i.mu.RUnlock()
	if !closed {
		i.mu.Lock()
		i.closed = true
		i.mu.Unlock()
		i.manager.close()
	}
}
