package intracom

import "sync"

// maanger is a struct that holds onto and manages an internal channels map with a mutex to handle read/write locks.
type manager[T any] struct {
	channels map[string]*channel[T]
	mu       *sync.RWMutex
}

func newManager[T any]() *manager[T] {
	return &manager[T]{
		channels: make(map[string]*channel[T]),
		mu:       new(sync.RWMutex),
	}
}

func (m *manager[T]) add(topic string, channel *channel[T]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels[topic] = channel
}

func (m *manager[T]) get(topic string) (*channel[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channel, exists := m.channels[topic]
	return channel, exists
}

func (m *manager[T]) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels)
}

func (m *manager[T]) remove(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, topic)
}

func (m *manager[T]) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, channel := range m.channels {
		channel.close()
	}

	m.channels = make(map[string]*channel[T])

}
