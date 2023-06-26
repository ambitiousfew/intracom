package intracom

import (
	"sync"
)

type Intracom[T any] struct {
	manager *intraManager[T]
	closed  bool
	mu      *sync.Mutex
}

func New[T any]() *Intracom[T] {
	return &Intracom[T]{
		mu:      new(sync.Mutex),
		manager: newIntraManager[T](),
		closed:  false,
	}
}

func (i *Intracom[T]) Subscribe(topic, consumerID string, chanSize int) <-chan T {
	if chanSize < 0 {
		// do not allow negative channel sizes, though we may want to allow unbuffered.
		chanSize = 0
	}

	channel, exists := i.manager.get(topic)
	if !exists {
		ch := make(chan T, chanSize)
		consumer := newIntraConsumer[T](ch)
		// if the topic does not yet exist, create an empty subs map for that topic.
		channel := newIntraChannel[T]()
		// add the consumer into the channel topic map
		channel.subscribe(consumerID, consumer)
		// add the new channel to our map of all topics.
		i.manager.add(topic, channel)
		return ch
	}

	// TODO: How to handle multiple subscribes?
	// place the last message in the queue again?
	if consumer, exists := channel.get(consumerID); exists {
		// attempt resubscribe, already exists so just places last message on chan
		channel.subscribe(consumerID, consumer)
		return consumer.ch
	}

	// channel topic exists, consumer does not.
	ch := make(chan T, chanSize)
	consumer := newIntraConsumer[T](ch)
	channel.subscribe(consumerID, consumer)
	return ch
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

func (i *Intracom[T]) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.closed {
		i.manager.close()
		i.closed = true
	}
}
