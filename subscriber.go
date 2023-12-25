package intracom

import "time"

type subscriber[T any] struct {
	topic         string
	consumerGroup string
	bufferSize    int
	bufferPolicy  BufferPolicy
	dropTimeout   time.Duration
	timer         *time.Timer

	ch     chan T
	stopC  chan struct{}
	closed bool
}

func newSubscriber[T any](conf SubscriberConfig) *subscriber[T] {
	var timer *time.Timer
	if conf.BufferPolicy == DropNewestAfterTimeout || conf.BufferPolicy == DropOldestAfterTimeout {
		timer = time.NewTimer(conf.DropTimeout)
		timer.Stop()
	}
	return &subscriber[T]{
		topic:         conf.Topic,
		consumerGroup: conf.ConsumerGroup,
		bufferSize:    conf.BufferSize,
		bufferPolicy:  conf.BufferPolicy,
		dropTimeout:   conf.DropTimeout,
		timer:         timer,
		ch:            make(chan T, conf.BufferSize),
		stopC:         make(chan struct{}),
	}
}

// send sends a message to the subscriber's channel.
// if the channel is full, the buffer policy will come into effect on
// how to handle the message.
func (s *subscriber[T]) send(message T) {
	switch s.bufferPolicy {
	case DropNone:
		select {
		case <-s.stopC:
			return
		case s.ch <- message:
		}
		return

	case DropOldest: // if the channel is full, drop the oldest message
		select {
		case <-s.stopC:
			return
		case s.ch <- message:
		default:
			<-s.ch          // pop one
			s.ch <- message // push one
			return
		}

	case DropOldestAfterTimeout:
		select {
		case <-s.stopC:
			return
		case s.ch <- message:
		default:
			s.timer.Reset(s.dropTimeout)
			select {
			case s.ch <- message: // try to push the message again
			case <-s.timer.C: // if the timer expires, drop the oldest message
				<-s.ch
				s.ch <- message
				return
			}
		}

	case DropNewest: // try to push the message, if the channel is full, drop the current message
		select {
		case <-s.stopC:
			return
		case s.ch <- message:
		default:
			return
		}

	case DropNewestAfterTimeout:
		select {
		case <-s.stopC:
			return
		case s.ch <- message:
		default:
			s.timer.Reset(s.dropTimeout)
			select {
			case <-s.stopC:
				return
			case s.ch <- message:
			case <-s.timer.C:
				// do nothing
				return
			}
		}
	}
}

func (s *subscriber[T]) close() {
	if s.closed {
		return
	}

	// if timer is not nil, stop it
	if s.timer != nil {
		s.timer.Stop()
	}

	close(s.stopC)
	close(s.ch)
}
