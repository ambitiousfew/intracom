package intracom

import (
	"sync"
)

// intraChannel represents a single channel topic which holds all subscriptions to that topic
type intraChannel[T any] struct {
	consumers   map[string]*intraConsumer[T]
	lastMessage *T
	mu          *sync.RWMutex
}

func newIntraChannel[T any]() *intraChannel[T] {
	return &intraChannel[T]{
		consumers:   make(map[string]*intraConsumer[T]),
		lastMessage: nil,
		mu:          new(sync.RWMutex),
	}
}

func (ic *intraChannel[T]) message() T {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return *ic.lastMessage
}

func (ic *intraChannel[T]) get(id string) (*intraConsumer[T], bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	consumer, exists := ic.consumers[id]
	return consumer, exists
}

func (ic *intraChannel[T]) len() int {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return len(ic.consumers)
}

func (ic *intraChannel[T]) broadcast(message T) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.lastMessage = &message

	for _, consumer := range ic.consumers {
		consumer.send(message)
	}
}

func (ic *intraChannel[T]) subscribe(id string, consumer *intraConsumer[T]) {
	c, exists := ic.get(id)
	if !exists {
		ic.mu.Lock()
		ic.consumers[id] = consumer
		c = consumer
		ic.mu.Unlock()
	}

	ic.mu.RLock()
	// attempt to place the current last message in the consumer channel if exists
	if ic.lastMessage != nil {
		if c.bufSize == 0 {
			// unbuffered channel case
			// This would be a very short lived worker only when a last message exists to deliver.
			go func() {
				select {
				case <-c.workerStopC:
					// if c.close() happened before we were able to send message. stop trying.
					return
				case c.ch <- *ic.lastMessage:
					return
				}
			}()
		} else {
			// buffered channel case - we succeed if there is room in the buffer
			// NOTE: resubscribes into a larger buffer without consumer the channel
			// you can end up with duplicate last messages being queued into the buffer.
			select {
			case c.ch <- *ic.lastMessage:
			default:
			}
		}

	}
	ic.mu.RUnlock()
}

// unsubscribe will remove a consumer from the channel map by its id
func (ic *intraChannel[T]) unsubscribe(id string) {
	ic.mu.RLock()
	consumer, exists := ic.consumers[id]
	ic.mu.RUnlock()
	if !exists {
		return
	}
	// consumer cleanup
	consumer.close()

	ic.mu.Lock()
	// channel map cleanup
	delete(ic.consumers, id)
	ic.mu.Unlock()

}

// unsubscribe will remove a consumer from the channel map by its id
func (ic *intraChannel[T]) close() {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	for _, consumer := range ic.consumers {
		consumer.close()
	}
	ic.lastMessage = nil
	ic.consumers = make(map[string]*intraConsumer[T])
}
