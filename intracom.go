package intracom

import (
	"sync"
)

type Intracom[T any] struct {
	mu          *sync.Mutex
	channels    map[string]map[string]chan T
	closed      bool
	lastMessage map[string]*T
}

func New[T any]() *Intracom[T] {
	return &Intracom[T]{
		mu:          new(sync.Mutex),
		channels:    make(map[string]map[string]chan T),
		closed:      false,
		lastMessage: make(map[string]*T),
	}
}

func (i *Intracom[T]) Subscribe(topic, consumerID string, chanSize int) <-chan T {
	if chanSize < 0 {
		// do not allow negative channel sizes, though we may want to allow unbuffered.
		chanSize = 0
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	subs, exists := i.channels[topic]
	if !exists {
		// if the topic does not yet exist, create an empty subs map for that topic.
		i.channels[topic] = make(map[string]chan T)
	}

	ch := make(chan T, chanSize)
	msg := i.lastMessage[topic]

	if msg != nil {
		// if there is a previously stored message for this topic, try to send it upon subscribe.
		select {
		case ch <- *msg:
		default:
		}
	}

	if subCh, exists := subs[consumerID]; exists {
		if msg != nil {
			// multiple resubscribes returns the same channel and places the last message into the buffer.
			select {
			case subCh <- *i.lastMessage[topic]:
				// attempt to place the last message into the channel buffer.
				// if it blocks for any amount of time we fall into default case.
				return subCh
			default:
				// if there was already a last message in the buffer, just return the channel.
				return subCh
			}

		}
		// if the same consumerID tries to subscribe more than once, always return the existing channel.
		return subCh
	}

	i.channels[topic][consumerID] = ch
	return ch
}

func (i *Intracom[T]) Unsubscribe(topic, consumerID string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	subs, exists := i.channels[topic]
	if !exists {
		// cannot unsubscribe from a topic that never exists, protect against typo'd names causing nil map
		return
	}

	ch, found := subs[consumerID]
	if !found {
		// cannot unsubscribe consumer if consumer never existed in the map to begin with.
		// prevent close or delete against a channel or map entry that wouldnt exist.
		return
	}
	close(ch)
	delete(i.channels[topic], consumerID)
}

func (i *Intracom[T]) Publish(topic string, message T) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, ch := range i.channels[topic] {
		select {
		case ch <- message:
		default:
		}
	}

	// store the published message as the previous message sent
	i.lastMessage[topic] = &message
}

func (i *Intracom[T]) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.closed {
		for topic, subs := range i.channels {
			for _, ch := range subs {
				// close each channel for each sub
				close(ch)
			}
			// clean up the nested maps by deleting key/value
			delete(i.channels, topic)
		}
		i.closed = true
	}
}
