package intracom

import (
	"context"
	"fmt"
)

// intracomChannel represents a single topic in the Intracom instance.
type intracomChannel[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	topic    string
	publishC chan T
	requestC chan any

	brokerDoneC    chan struct{}
	broadcastDoneC chan struct{}
}

// newChannel returns a new channel instance.
func newIntracomChannel[T any](parent context.Context, topic string, publisher chan T) *intracomChannel[T] {
	ctx, cancel := context.WithCancel(parent)
	channel := &intracomChannel[T]{
		ctx:    ctx,
		cancel: cancel,

		topic:    topic,
		publishC: publisher,
		requestC: make(chan any),

		brokerDoneC:    make(chan struct{}, 1),
		broadcastDoneC: make(chan struct{}, 1),
	}
	// launch background routine to handle channel requests
	go channel.broker()
	return channel
}

// broadcaster is a blocking function that will handle all requests to the channel.
// it will also handle broadcasting messages to all subscribers.
func (c *intracomChannel[T]) broadcaster(broadcastC <-chan any, doneC chan<- struct{}) {
	subscribers := make(map[string]chan T)

	// NOTE: this nil publish channel is conditionally hot-swapped for an active channel
	var publish chan T // reading a nil channel causes blocking until its not nil

	for {
		select {
		case <-c.broadcastDoneC:
			publish = nil // ensure publish case cant be selected
			// close all subscriber channels.
			for _, ch := range subscribers {
				close(ch)
			}
			subscribers = nil   // nil for gc
			doneC <- struct{}{} // signal finished
			return

		case request := <-broadcastC:
			// if a request comes in during a broadcast, selecting this case will
			// pause publishing long enough to update subscriber map
			// then continue publishing.
			switch r := request.(type) {
			case channelCloseRequest:
				publish = nil // ensure publish case cant be selected
				for _, ch := range subscribers {
					close(ch)
				}
				subscribers = nil // nil for gc
				return

			case channelUnsubscribeRequest[T]:
				// attempt to remove from subscriber map so the publisher is able to
				//  detach before requester cancels the consumer channel.
				ch, exists := subscribers[r.consumer]
				if exists {
					delete(subscribers, r.consumer)
					close(ch)
					if len(subscribers) < 1 {
						publish = nil // stop broadcaster from reading published messages.
					}
				}
				r.responseC <- exists

			case channelSubscribeRequest[T]:
				ch, exists := subscribers[r.conf.ConsumerGroup]
				if !exists {
					subscriberC := make(chan T, r.conf.BufferSize)
					subscribers[r.conf.ConsumerGroup] = subscriberC
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
}

// broker is a blocking function that will handle all requests to the channel.
func (c *intracomChannel[T]) broker() {
	subscribers := make(map[string]chan T)

	broadcastC := make(chan any)
	defer close(broadcastC)

	doneC := make(chan struct{}, 1)
	defer close(doneC)

	go c.broadcaster(broadcastC, doneC) // spawn the broadcaster routine

	ctxDone := c.ctx.Done()

	for {
		select {
		case <-c.brokerDoneC:
			close(c.broadcastDoneC) // signal broadcaster to stop
			<-doneC                 // wait for broadcaster to signal finished
			subscribers = nil       // nil for gc
			return

		case <-ctxDone:
			close(c.broadcastDoneC) // signal broadcaster to stop
			<-doneC                 // wait for broadcaster to signal finished
			subscribers = nil       // nil for gc
			ctxDone = nil           // prevent this case from being selected again

		case request := <-c.requestC:
			switch r := request.(type) {
			case channelCloseRequest:
				// signal closing of the channel
				broadcastC <- r   // forward to broadcaster
				subscribers = nil // subscribers cleanup
				// close(c.brokerDoneC)
				r.responseC <- struct{}{}

			case channelLookupRequest[T]:
				// lookup subscribers in local subscribers map cache
				// prevents us from disturbing the broadcaster for lookups
				ch, exists := subscribers[r.consumer]
				r.responseC <- channelLookupResponse[T]{ch: ch, found: exists}

			case channelSubscribeRequest[T]:
				// check local cache for reference to NOT interrupt broadcaster unless needed.
				if ch, exists := subscribers[r.conf.ConsumerGroup]; exists {
					r.responseC <- channelSubscribeResponse[T]{ch: ch, exists: true}
					continue
				}

				bRequest := channelSubscribeRequest[T]{
					conf:      r.conf,
					responseC: make(chan channelSubscribeResponse[T]),
				}

				// subscriber id does not exist in local cache, broadcaster needs update too.
				broadcastC <- bRequest           // send request to broadcaster
				response := <-bRequest.responseC // wait for response

				if !response.exists {
					// update our local subscrbers cache for future lookups
					subscribers[r.conf.ConsumerGroup] = response.ch
				}

				r.responseC <- channelSubscribeResponse[T]{
					ch:     response.ch,
					exists: response.exists,
				} // reply to requester

			case channelUnsubscribeRequest[T]:
				// check local cache for reference to NOT interrupt broadcaster unless needed.
				if _, exists := subscribers[r.consumer]; !exists {
					r.responseC <- false
					continue
				}

				bRequest := channelUnsubscribeRequest[T]{
					consumer:  r.consumer,
					responseC: make(chan bool),
				}

				// subscriber id does not exist in local cache, broadcaster needs update too.
				// forward the broadcaster a unsubscribe request
				broadcastC <- bRequest
				<-bRequest.responseC
				close(bRequest.responseC) // nil for gc
				delete(subscribers, r.consumer)
				r.responseC <- true // reply to requester

			default:
				fmt.Printf("error: channel '%s' receiving unknown requests\n", c.topic)
			}
		}
	}
}

// get will attempt to retrieve a channel for the given consumer.
func (c *intracomChannel[T]) get(consumer string) (chan T, bool) {
	request := channelLookupRequest[T]{
		consumer:  consumer,
		responseC: make(chan channelLookupResponse[T]),
	}

	c.requestC <- request           // send request
	response := <-request.responseC // wait for response

	return response.ch, response.found

}

// subscribe will attempt to subscribe to the channel.
// if the channel does not exist, it will be created.
func (c *intracomChannel[T]) subscribe(conf ConsumerConfig) chan T {
	request := channelSubscribeRequest[T]{
		conf:      conf,
		responseC: make(chan channelSubscribeResponse[T]),
	}

	c.requestC <- request           // send request
	response := <-request.responseC // wait for response

	return response.ch
}

// unsubscribe will attempt to unsubscribe from the channel.
// if the channel does not exist, an error will be returned.
func (c *intracomChannel[T]) unsubscribe(consumer string) bool {
	request := channelUnsubscribeRequest[T]{
		consumer:  consumer,
		responseC: make(chan bool),
	}

	c.requestC <- request           // send request
	response := <-request.responseC // wait for response
	return response
}

// close will attempt to close the channel.
// if the channel does not exist, an error will be returned.
func (c *intracomChannel[T]) close() {
	// send close request signal
	request := channelCloseRequest{responseC: make(chan struct{})}
	c.requestC <- request // send request
	<-request.responseC   // wait for response
	close(c.brokerDoneC)

}
