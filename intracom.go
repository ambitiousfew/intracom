package intracom

import (
	"context"
	"fmt"
)

// Intracom is a thread-safe in-memory pub/sub implementation.
type Intracom[T any] struct {
	// ctx is the context that will be used to cancel all work being done by the Intracom instance.
	ctx context.Context
	// channels is an in-memory map primarily used to lookup the publishing channels by topic name
	channels map[string]*channel[T]

	// requestC is the channel that will be used to send requests to the Intracom instance.
	requestC chan any
}

// New returns a new Intracom instance.
func New[T any](ctx context.Context) *Intracom[T] {
	ic := &Intracom[T]{
		ctx:      ctx,
		channels: make(map[string]*channel[T]),
		requestC: make(chan any, 1),
	}

	// start the Intracom instance request handler
	go ic.start()
	return ic
}

// start is a blocking function that will handle all requests to the Intracom instance.
func (i *Intracom[T]) start() {
	for {
		select {
		case <-i.ctx.Done():
			for _, ch := range i.channels {
				ch.close() // close waiting for signal of completion
			}
			return

		case request := <-i.requestC:
			switch r := request.(type) {
			case intracomUnregisterRequest:
				if ch, exists := i.channels[r.topicID]; !exists {
					r.responseC <- false
				} else {
					// to unregister a topic, we need to stop all work being done.
					ch.close()
					r.responseC <- true
				}

			case intracomLookupRequest[T]:
				c, exists := i.channels[r.topic]
				if exists {
					// topic exists, pass back existing channel
					ch, found := c.get(r.consumer)
					r.responseC <- intracomLookupResponse[T]{ch: ch, found: found}
					continue
				}
				r.responseC <- intracomLookupResponse[T]{ch: nil, found: false}

			case intracomRegisterRequest[T]:
				ch, exists := i.channels[r.topicID]
				if !exists {
					// topic does not exist, create it
					publishC := make(chan T)
					ch = newChannel[T](i.ctx, r.topicID, publishC)
					i.channels[r.topicID] = newChannel[T](i.ctx, r.topicID, publishC)
				}
				// respond to request
				r.responseC <- intracomRegisterResponse[T]{publishC: ch.publishC, found: exists}

			case intracomUnsubscribeRequest[T]:
				ch, exists := i.channels[r.topic]
				if !exists {
					r.responseC <- false
					continue
				}
				err := ch.unsubscribe(r.consumer)
				r.responseC <- err == nil

			case intracomSubscribeRequest[T]:
				ch, exists := i.channels[r.conf.Topic]
				if !exists {
					// channel topic doesnt exist, create new one with subscriber added.
					publishC := make(chan T, 1)
					ch := newChannel[T](i.ctx, r.conf.Topic, publishC)

					i.channels[r.conf.Topic] = ch

					subscriberC := make(chan T, r.conf.BufferSize)
					// subscribe a consumer
					ch.subscribe(r.conf.ConsumerGroup, *r.conf)

					// pass back subscriber response
					r.responseC <- intracomSubscribeResponse[T]{
						ch:      subscriberC,
						success: true,
					}
					continue
				}

				subscriberC, found := ch.get(r.consumer)
				if !found {
					// if the topic existed but the consumer did not.
					subscriberC = ch.subscribe(r.consumer, *r.conf)
				}

				r.responseC <- intracomSubscribeResponse[T]{
					ch:      subscriberC,
					success: true,
				}

			default:
				fmt.Println("error: intracom processing unknown requests")
			}
		}
	}
}

// Register will create a topic and give back a channel to publish on and an unregister function
// that if called will unregister the topic name that was registered with this Register call.
func (i *Intracom[T]) Register(topic string) (chan<- T, func() bool) {
	request := intracomRegisterRequest[T]{
		topicID:   topic,
		responseC: make(chan intracomRegisterResponse[T], 1),
	}
	// send request to register topic
	select {
	case <-i.ctx.Done():
		return nil, i.unregister("")
	case i.requestC <- request:
	}

	// wait for response or cancellation
	select {
	case <-i.ctx.Done():
		return nil, nil
	case response := <-request.responseC:
		close(request.responseC)
		// return publisher channel and an unregister func reference tied to this topic registration.
		return response.publishC, i.unregister(topic)
	}
}

func (i *Intracom[T]) Subscribe(conf *ConsumerConfig) (<-chan T, func() bool) {
	if conf == nil {
		// cant allow nil consumer configs, default it if nil.
		// TODO: log warning
		conf.Topic = ""
		conf.ConsumerGroup = ""
		conf.BufferSize = 1
		conf.BufferPolicy = DropNone
	} else {
		// Buffer size should always be at least 1
		if conf.BufferSize < 1 {
			conf.BufferSize = 1
		}
	}

	// lookup existing consumer first.
	lookupR := intracomLookupRequest[T]{
		topic:     conf.Topic,
		consumer:  conf.ConsumerGroup,
		responseC: make(chan intracomLookupResponse[T], 1),
	}

	// if consumer channel does exist, reuse
	// send lookup request
	select {
	case <-i.ctx.Done():
		return nil, nil
	case i.requestC <- lookupR:
	}

	// wait for lookup response
	select {
	case <-i.ctx.Done():
		return nil, nil
	case lookupResp := <-lookupR.responseC:
		close(lookupR.responseC)
		if lookupResp.found {
			// found existing consumer in channels
			return lookupResp.ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)
		}
	}

	// if existing consumer doesnt exist, then create
	subscribeR := intracomSubscribeRequest[T]{
		conf:      conf,
		topic:     conf.Topic,
		consumer:  conf.ConsumerGroup,
		ch:        make(chan T, conf.BufferSize),
		responseC: make(chan intracomSubscribeResponse[T], 1),
	}

	// send subscribe request
	select {
	case <-i.ctx.Done():
		return nil, i.unsubscribe("", "")
	case i.requestC <- subscribeR:
	}

	// wait for subscribe response
	select {
	case <-i.ctx.Done():
		return nil, nil
	case subscribeResp := <-subscribeR.responseC:
		close(subscribeR.responseC)

		if subscribeResp.success {
			return subscribeResp.ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)
		}
		// could not subscribe for some reason...
		return subscribeResp.ch, i.unsubscribe("", "")
	}

}

// unsubscribe returns a closure that contains the topic and consumer name so if called will cancel
func (i *Intracom[T]) unsubscribe(topic, consumer string) func() bool {
	return func() bool {
		if topic == "" && consumer == "" {
			return false
		}

		request := intracomUnsubscribeRequest[T]{
			topic:     topic,
			consumer:  consumer,
			responseC: make(chan bool, 1),
		}

		// send request
		select {
		case <-i.ctx.Done():
			return false
		case i.requestC <- request:
		}

		// wait for response
		select {
		case <-i.ctx.Done():
			return false
		case success := <-request.responseC: // wait for response
			close(request.responseC)
			return success
		}
	}
}

// unregister returns a closure that contains the topic name so if called will cancel
func (i *Intracom[T]) unregister(topic string) func() bool {
	return func() bool {
		if topic == "" {
			return false
		}

		request := intracomUnregisterRequest{
			topicID:   topic,
			responseC: make(chan bool, 1),
		}

		// send request to unregister topic
		select {
		case <-i.ctx.Done():
			return false
		case i.requestC <- request:
		}

		// wait for response
		select {
		case <-i.ctx.Done():
			return false
		case success := <-request.responseC:
			close(request.responseC)
			return success
		}
	}

}
