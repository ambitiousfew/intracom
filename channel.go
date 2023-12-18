package intracom

import (
	"context"
	"fmt"
)

// channel represents a single topic in the Intracom instance.
type channel[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	topic string

	publishC chan T

	requestC   chan any
	broadcastC chan any
	closed     bool
}

// newChannel returns a new channel instance.
func newChannel[T any](parent context.Context, topic string, publisher chan T) *channel[T] {
	ctx, cancel := context.WithCancel(parent)
	ch := &channel[T]{
		ctx:    ctx,
		cancel: cancel,

		topic:    topic,
		publishC: publisher,

		requestC:   make(chan any, 1),
		broadcastC: make(chan any, 1),
		closed:     false,
	}

	go ch.broadcaster()
	// launch background routine to handle channel requests
	go ch.start()
	return ch
}

// broadcaster is a blocking function that will handle all requests to the channel.
// it will also handle broadcasting messages to all subscribers.
func (c *channel[T]) broadcaster() {
	subscribers := make(map[string]chan T)

	// NOTE: this nil publish channel is conditionally hot-swapped for an active channel
	var publish chan T // reading a nil channel causes blocking until its not nil

	for {
		select {
		case <-c.ctx.Done():
			// close all subscriber channels.
			for _, ch := range subscribers {
				close(ch)
			}
			subscribers = nil // nil for gc
			return

		case request := <-c.broadcastC:
			// if a request comes in during a broadcast, selecting this case will
			// pause publishing long enough to update subscriber map
			// then continue publishing.
			switch r := request.(type) {
			case channelCloseRequest:
				for _, ch := range subscribers {
					close(ch)
				}
				publish = nil     // nil for gc
				subscribers = nil // nil for gc
				return

			case channelUnsubscribeRequest[T]:
				// attempt to remove from subscriber map so the publisher is able to
				//  detach before requester cancels the consumer channel.
				ch, exists := subscribers[r.id]
				if exists {
					delete(subscribers, r.id)
					close(ch)
					if len(subscribers) < 1 {
						publish = nil // stop broadcaster from reading published messages.
					}

					r.responseC <- nil
					continue
				}
				r.responseC <- fmt.Errorf("cannot unsubscribe '%s', does not exist", r.id)

			case channelSubscribeRequest[T]:
				ch, exists := subscribers[r.id]
				if !exists {
					subscriberC := make(chan T, r.conf.BufferSize)
					subscribers[r.id] = subscriberC
					ch = subscriberC
					if len(subscribers) > 0 {
						// swap nil publish channel since we have consumers now.
						publish = c.publishC
					}
				}
				// if channel did not exist then success represents success at adding the NEW channel.
				r.responseC <- channelSubscribeResponse[T]{ch: ch, exists: exists}

			}

		// NOTE: Anytime select chooses ctx.Done or broadcastC, it interrupts the publishing.
		// So ideally we only want to send requests to broadcastC if they are necessary, such as:
		// the creation or deletion of a subscriber because we need to update the subscribers map
		// between publishing.
		case msg := <-publish:
			for _, sub := range subscribers {
				select {
				case <-c.ctx.Done():
					return
				case sub <- msg:
					// pipe the new message to each subscriber
				}
			}
		}
	}

	//
}

// start is a blocking function that will handle all requests to the channel.
func (c *channel[T]) start() {
	subscribers := make(map[string]chan T)

	for {
		select {
		case <-c.ctx.Done():
			subscribers = nil
			return

		case request := <-c.requestC:
			switch r := request.(type) {
			case channelCloseRequest:
				// signal closing of the channel
				c.broadcastC <- r // forward to broadcaster
				subscribers = nil // subscribers cleanup
				return

			case channelLookupRequest[T]:
				// lookup subscribers in local subscribers map cache
				// prevents us from disturbing the broadcaster for lookups
				ch, exists := subscribers[r.id]
				r.responseC <- channelLookupResponse[T]{ch: ch, found: exists}

			case channelSubscribeRequest[T]:
				// check local cache for reference to NOT interrupt broadcaster unless needed.
				if ch, exists := subscribers[r.id]; exists {
					r.responseC <- channelSubscribeResponse[T]{ch: ch, exists: true}
					continue
				}

				bRequest := channelSubscribeRequest[T]{
					id:        r.id,
					conf:      r.conf,
					responseC: make(chan channelSubscribeResponse[T], 1),
				}

				// subscriber id does not exist in local cache, broadcaster needs update too.
				c.broadcastC <- bRequest // forward channel request to broadcaster
				response := <-bRequest.responseC

				if !response.exists {
					// update our local references
					subscribers[r.id] = r.ch
				}

				r.responseC <- channelSubscribeResponse[T]{
					ch:     response.ch,
					exists: response.exists,
				} // reply to requester

			case channelUnsubscribeRequest[T]:
				// check local cache for reference to NOT interrupt broadcaster unless needed.
				if _, exists := subscribers[r.id]; !exists {
					r.responseC <- fmt.Errorf("cannot unsubscribe '%s', does not exist", r.id)
					continue
				}

				bRequest := channelUnsubscribeRequest[T]{
					id:        r.id,
					responseC: make(chan error, 1),
				}

				// subscriber id does not exist in local cache, broadcaster needs update too.
				// forward the broadcaster a unsubscribe request
				c.broadcastC <- bRequest
				err := <-bRequest.responseC
				close(bRequest.responseC) // nil for gc

				if err == nil {
					delete(subscribers, r.id)
				}

				r.responseC <- err // reply to requester

			default:
				fmt.Printf("error: channel '%s' receiving unknown requests\n", c.topic)
			}
		}
	}
}

// get will attempt to retrieve a channel for the given consumer.
func (c *channel[T]) get(consumer string) (chan T, bool) {
	request := channelLookupRequest[T]{
		id:        consumer,
		responseC: make(chan channelLookupResponse[T], 1),
	}

	if c.closed {
		return nil, false
	}

	// send request
	select {
	case <-c.ctx.Done(): // watch for cancellation
		return nil, false
	case c.requestC <- request:
	}

	if c.closed {
		return nil, false
	}
	// wait for response
	select {
	case <-c.ctx.Done(): // watch for cancellation
		return nil, false
	case response := <-request.responseC:
		return response.ch, response.found
	}
}

// subscribe will attempt to subscribe to the channel.
// if the channel does not exist, it will be created.
func (c *channel[T]) subscribe(consumer string, conf ConsumerConfig) chan T {

	request := channelSubscribeRequest[T]{
		id:        consumer,
		conf:      conf,
		responseC: make(chan channelSubscribeResponse[T], 1),
	}

	if c.closed {
		return nil
	}
	// send request
	select {
	case <-c.ctx.Done(): // watch for cancellation
	case c.requestC <- request:
	}

	if c.closed {
		return nil
	}

	// wait for response
	select {
	case <-c.ctx.Done(): // watch for cancellation
		return nil
	case response := <-request.responseC:
		return response.ch
	}
}

// unsubscribe will attempt to unsubscribe from the channel.
// if the channel does not exist, an error will be returned.
func (c *channel[T]) unsubscribe(consumer string) error {
	request := channelUnsubscribeRequest[T]{
		id:        consumer,
		responseC: make(chan error, 1),
	}

	if c.closed {
		return nil
	}

	// send request
	select {
	case <-c.ctx.Done(): // watch for cancellation
	case c.requestC <- request:
	}

	if c.closed {
		return nil
	}

	// wait for response
	select {
	case <-c.ctx.Done(): // watch for cancellation
		return nil
	case err := <-request.responseC:
		return err
	}
}

// close will attempt to close the channel.
// if the channel does not exist, an error will be returned.
func (c *channel[T]) close() {
	// send close request signal
	c.requestC <- channelCloseRequest{}
}
