package intracom

import "sync"

// intraManager is a struct that holds onto and manages an internal channels map with a mutex to handle read/write locks.
type intraManager[T any] struct {
	channels map[string]*intraChannel[T]
	mu       *sync.RWMutex
}

func newIntraManager[T any]() *intraManager[T] {
	return &intraManager[T]{
		channels: make(map[string]*intraChannel[T]),
		mu:       new(sync.RWMutex),
	}
}

func (ics *intraManager[T]) add(topic string, channel *intraChannel[T]) {
	ics.mu.Lock()
	defer ics.mu.Unlock()
	ics.channels[topic] = channel
}

func (ics *intraManager[T]) get(topic string) (*intraChannel[T], bool) {
	ics.mu.RLock()
	defer ics.mu.RUnlock()
	channel, exists := ics.channels[topic]
	return channel, exists
}

func (ics *intraManager[T]) len() int {
	ics.mu.RLock()
	defer ics.mu.RUnlock()
	return len(ics.channels)
}

func (ics *intraManager[T]) remove(topic string) {
	ics.mu.Lock()
	defer ics.mu.Unlock()
	delete(ics.channels, topic)
}

func (ics *intraManager[T]) close() {
	ics.mu.Lock()
	defer ics.mu.Unlock()
	for _, channel := range ics.channels {
		channel.close()
	}
	ics.channels = make(map[string]*intraChannel[T])
}
