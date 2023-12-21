package intracom

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Intracom is a thread-safe in-memory pub/sub implementation.
type Intracom[T any] struct {
	// ctx is the context that will be used to cancel all work being done by the Intracom instance.
	ctx context.Context

	// requestC is the channel that will be used to send requests to the Intracom instance.
	requestC    chan any
	brokerDoneC chan struct{}

	closed atomic.Bool
}

// New returns a new Intracom instance.
func New[T any](ctx context.Context) *Intracom[T] {
	ic := &Intracom[T]{
		ctx:         ctx,
		requestC:    make(chan any),
		brokerDoneC: make(chan struct{}, 1),
		closed:      atomic.Bool{},
	}

	go ic.broker() // start the requests broker
	return ic
}

func (i *Intracom[T]) Register(topic string) (chan<- T, func() bool) {
	request := registerRequest[T]{
		topic:     topic,
		responseC: make(chan chan T),
	}

	i.requestC <- request           // send request
	publishC := <-request.responseC // wait for response

	close(request.responseC)
	// return publisher channel and an unregister func reference tied to this topic registration.
	return publishC, i.unregister(topic)

}

func (i *Intracom[T]) Subscribe(conf *SubscriberConfig) (<-chan T, func() error) {
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

	// no existing consumer found, send subscribe request
	request := subscribeRequest[T]{
		conf:      *conf,
		responseC: make(chan subscribeResponse[T]),
	}

	i.requestC <- request // send subscribe request
	response := <-request.responseC

	return response.ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)
}

// Close will close the Intracom instance and all of its subscriptions.
// Intracom instance will no longer be usable after calling this function.
// NOTE: Calling Register or Subscribe after calling Close will panic.
func (i *Intracom[T]) Close() {
	i.closed.Store(true) // mark instance as closed and unuseable.

	request := closeRequest{responseC: make(chan struct{})}
	i.requestC <- request
	<-request.responseC

	close(i.requestC)    // signal broker to stop processing requests
	close(i.brokerDoneC) // signal broker to shutdown
}

func (i *Intracom[T]) get(topic, consumer string) (<-chan T, bool) {
	request := lookupRequest[T]{
		topic:     topic,
		consumer:  consumer,
		responseC: make(chan lookupResponse[T]),
	}

	i.requestC <- request           // send request
	response := <-request.responseC // wait for response
	close(request.responseC)

	return response.ch, response.found
}

// unsubscribe returns a closure that contains the topic and consumer name so if called will cancel
func (i *Intracom[T]) unsubscribe(topic, consumer string) func() error {
	return func() error {
		request := unsubscribeRequest[T]{
			topic:     topic,
			consumer:  consumer,
			responseC: make(chan error),
		}

		if i.closed.Load() {
			// intracom is closed and unuseable, cant unsubscribe this late
			return fmt.Errorf("cannot unsubscribe from topic '%s' because intracom has already been closed", topic)
		}

		i.requestC <- request      // send request
		err := <-request.responseC // wait for response
		close(request.responseC)
		return err

	}
}

// unregister returns a closure that contains the topic name so if called will cancel
func (i *Intracom[T]) unregister(topic string) func() bool {
	return func() bool {
		if topic == "" {
			return false
		}

		if i.closed.Load() {
			// intracom is closed, so we cant unregister anymore topics.
			return false
		}

		request := unregisterRequest{
			topic:     topic,
			responseC: make(chan bool),
		}

		i.requestC <- request          // send request to unregister topic
		success := <-request.responseC // wait for response

		close(request.responseC)
		return success

	}

}

// broker is a blocking function that will handle all requests to the Intracom instance.
func (i *Intracom[T]) broker() {
	// broker stores its own local cache for lookups to avoid interrupting the broadcaster.
	// yes, duplicating state but its easy enough to keep them in-sync for the benefits.

	broadcasters := make(map[string]map[chan any]chan struct{}) // broadcaster channels
	publishers := make(map[string]chan T)                       // publisher channels
	// NOTE: channels here is only for lookups, dont close channels here. its broadcasters job.
	channels := make(map[string]map[string]chan T) // subscriber lookup (only) channels

	doneC := make(chan struct{}) // channel for broadcaster to signal when done
	defer close(doneC)

	ctxDone := i.ctx.Done()

	for {
		select {
		case <-i.brokerDoneC:
			// signal all broadcasters to stop and wait for them to finish
			for topic, broadcaster := range broadcasters {
				for broadcastC, doneC := range broadcaster {
					broadcastC <- closeRequest{responseC: make(chan struct{})}
					<-doneC
				}
				delete(broadcasters, topic)
			}

			// clean up channels local cache
			for topic := range channels {
				delete(channels, topic)
			}

			for topic, publishC := range publishers {
				close(publishC)
				delete(publishers, topic)
			}

			// after all broadcasters finish, clean up our local cache too.
			broadcasters = nil
			publishers = nil
			channels = nil
			ctxDone = nil
			return

		case <-ctxDone: // context cancelled but we allow broker to run until Close is called.
			// signal all broadcasters to stop and wait for them to finish
			// interrupt all broadcasters and wait for them to finish
			for topic, broadcaster := range broadcasters {
				for broadcastC, doneC := range broadcaster {
					bRequest := closeRequest{responseC: make(chan struct{})}
					broadcastC <- bRequest // send close request to broadcaster
					<-bRequest.responseC   // wait for response from broadcaster
					<-doneC
					close(broadcastC) // stop broadcaster from processing anymore published messages.
					close(doneC)
				}
				delete(broadcasters, topic)
			}

			// clean up channels local cache
			for topic := range channels {
				delete(channels, topic)
			}

			for topic, publishC := range publishers {
				close(publishC)
				delete(publishers, topic)
			}
			// DO NOT nil out the caches, broker handles requests until Close is called.
			ctxDone = nil // prevent signal from being received again

		case request := <-i.requestC:
			switch r := request.(type) {
			case closeRequest:
				// interrupt all broadcasters and wait for them to finish
				for topic, broadcaster := range broadcasters {
					for broadcastC, doneC := range broadcaster {
						broadcastC := broadcastC // remap for var for inline routine
						doneC := doneC           // remap for var for inline routine
						bRequest := closeRequest{responseC: make(chan struct{})}
						broadcastC <- bRequest // send close request to broadcaster
						<-bRequest.responseC   // wait for response from broadcaster
						<-doneC
					}

					delete(broadcasters, topic)
				}

				// clean up channels local cache
				for topic := range channels {
					delete(channels, topic)
				}

				// after all broadcasters finish, clean up our local cache too.
				for topic, publishC := range publishers {
					close(publishC)
					delete(publishers, topic)
				}

				channels = nil            // nil for gc
				publishers = nil          // prevent publishers from publishing anymore
				r.responseC <- struct{}{} // reply to sender
				return

			case unregisterRequest:
				broadcastC, exists := broadcasters[r.topic]
				if !exists {
					// couldn't find topic, so it must not exist.
					r.responseC <- false // reply to sender
					continue
				}

				// interrupt specific broadcaster for unregister request
				// since we are unregistering an entire topic, we can send a close request
				for broadcastC, doneC := range broadcastC {
					// interrupt broadcaster and wait for it to finish
					// bRequest := unregisterRequest{topic: r.topic, responseC: make(chan bool)}
					bRequest := closeRequest{responseC: make(chan struct{})}
					broadcastC <- bRequest // send close request to broadcaster
					<-bRequest.responseC   // wait for response from broadcaster
					close(broadcastC)      // stop broadcaster from processing anymore published messages.
					<-doneC                // wait for broadcaster to signal complete
					close(doneC)           // close done channel
				}

				publishC, exists := publishers[r.topic]
				if exists {
					close(publishC)
				}

				// cleanup local cache
				delete(channels, r.topic)     // remove subscriber lookup from local cache
				delete(publishers, r.topic)   // remove publisher from local cache
				delete(broadcasters, r.topic) // remove broadcaster from local cache
				r.responseC <- true           // reply to sender

			case registerRequest[T]:
				// check if topic exists in local cache, if not then broadcaster routine likely hasn't been created.
				if ch, exists := publishers[r.topic]; exists {
					r.responseC <- ch // reply to sender with existing publisher channel
				} else {
					// create publisher channel
					publishC := make(chan T)
					publishers[r.topic] = publishC

					// create broadcaster channel
					broadcastC := make(chan any)
					doneC := make(chan struct{})

					// create a broadcaster request channel and done channel pair
					broadcasters[r.topic] = make(map[chan any]chan struct{})
					broadcasters[r.topic][broadcastC] = doneC

					// broadcaster will have no subscribers until a subscribe request is received.
					// publishers will be blocked from sending until first subscriber is registered.
					go i.broadcaster(broadcastC, publishC, doneC) // start broadcaster for this topic

					// initialize subscriber lookup cache
					channels[r.topic] = make(map[string]chan T)
					r.responseC <- publishC // reply to sender with new publisher channel
				}

			case lookupRequest[T]:
				// check if topic exists in local cache, if not then broadcaster shouldnt have it either.
				// DO NOT interrupt broadcaster for lookup requests.
				subscribers, exists := channels[r.topic]
				if !exists {
					// topic doesnt exist, so consumer group cant exist either.
					r.responseC <- lookupResponse[T]{ch: nil, found: exists}
					continue
				}
				ch, found := subscribers[r.consumer]
				r.responseC <- lookupResponse[T]{ch: ch, found: found}

			case unsubscribeRequest[T]:
				// check if topic exists in local cache, if not then unsubscribing is unsuccessful.
				subscribers, exists := channels[r.topic]
				if !exists {
					r.responseC <- fmt.Errorf("cannot unsubscribe topic '%s' does not exist", r.topic) // reply to sender with false
					continue
				}

				// check if consumer group exists in local cache, if not then unsubscribing is unsuccessful.
				_, found := subscribers[r.consumer]
				if !found {
					r.responseC <- fmt.Errorf("cannot unsubscribe consumer '%s' has not been subscribed", r.consumer)
					continue
				}

				// consumer group exists, send unsubscribe request to broadcaster
				var err error
				if broadcasters, exists := broadcasters[r.topic]; exists {
					// interrupt broadcaster for unsubscribe request
					for broadcastC := range broadcasters {
						// interrupt broadcaster publish to remove subscriber
						bRequest := unsubscribeRequest[T]{topic: r.topic, consumer: r.consumer, responseC: make(chan error)}
						broadcastC <- bRequest     // send unsubscribe request to broadcaster
						err = <-bRequest.responseC // wait for response from broadcaster
					}
				}

				delete(subscribers, r.consumer) // remove subscriber from local cache
				r.responseC <- err              // reply to sender

			case subscribeRequest[T]:
				// check local cache first to see if topic exists
				subscribers, exists := channels[r.conf.Topic]
				if exists {
					response := subscribeResponse[T]{ch: nil, success: false}
					// check local cache first to see if consumer group exists
					if ch, found := subscribers[r.conf.ConsumerGroup]; found {
						r.responseC <- subscribeResponse[T]{ch: ch, success: found} // reply to sender with existing channel
						continue
					} else {
						// consumer group did not exist, send request to broadcaster
						subReq := subscribeRequest[T]{conf: r.conf, responseC: make(chan subscribeResponse[T])}
						for broadcastC := range broadcasters[r.conf.Topic] {
							broadcastC <- subReq                            // send subscribe request to broadcaster
							response = <-subReq.responseC                   // update response with broadcaster response
							subscribers[r.conf.ConsumerGroup] = response.ch // update local cache
						}
					}

					r.responseC <- subscribeResponse[T]{ch: response.ch, success: response.success} // reply to sender
					// dont continue to next block
					continue
				}

				// topic does not exist in local cache, so it must not exist in broadcaster cache either.
				// so we need perform the same steps as a topic registration and subscribe the consumer group.

				// create publisher channel, update publishers cache
				publishC := make(chan T)
				publishers[r.conf.Topic] = publishC

				// create broadcaster channel, update broadcasters cache
				broadcastC := make(chan any)
				doneC := make(chan struct{})

				// create a broadcaster request channel and done channel pair
				broadcasters[r.conf.Topic] = make(map[chan any]chan struct{})
				broadcasters[r.conf.Topic][broadcastC] = doneC

				// start a broadcaster for this topic.
				go i.broadcaster(broadcastC, publishC, doneC)

				subRequest := subscribeRequest[T]{conf: r.conf, responseC: make(chan subscribeResponse[T])}

				broadcastC <- subRequest // send subscribe request to broadcaster
				response := <-subRequest.responseC
				// wait for response from broadcaster
				channels[r.conf.Topic] = make(map[string]chan T)           // initialize subscriber lookup map
				channels[r.conf.Topic][r.conf.ConsumerGroup] = response.ch // update local cache
				r.responseC <- response                                    // reply to sender

			default:
				fmt.Println("error: intracom processing unknown requests", r)
			}
		}
	}
}

// broadcaster is a blocking function that will handle all requests to the channel.
// it will also handle broadcasting messages to all subscribers.
func (i *Intracom[T]) broadcaster(broadcastC <-chan any, publishC <-chan T, doneC chan<- struct{}) {
	subscribers := make(map[string]chan T)

	// NOTE: this nil publish channel is conditionally hot-swapped for an active channel
	var publish <-chan T // reading a nil channel causes blocking until its not nil

	for {
		select {
		case request := <-broadcastC:
			// if a request comes in during a broadcast, selecting this case will
			// pause publishing long enough to update subscriber map
			// then continue publishing.
			switch r := request.(type) {
			case closeRequest:
				// close all subscriber channels.
				for _, ch := range subscribers {
					close(ch)
				}
				subscribers = nil         // nil for gc
				publish = nil             // ensure we dont receive published messages anymore
				broadcastC = nil          // ensure we dont receive any more requests
				r.responseC <- struct{}{} // reply to sender
				doneC <- struct{}{}       // signal to broker that we are done
				return

			case unsubscribeRequest[T]:
				// attempt to remove from subscriber map so the publisher is able to
				//  detach before requester cancels the consumer channel.
				ch, found := subscribers[r.consumer]
				if found {
					close(ch) // close consumer channel
				}
				delete(subscribers, r.consumer) // remove subscriber from local cache
				r.responseC <- nil

			case subscribeRequest[T]:
				if ch, exists := subscribers[r.conf.ConsumerGroup]; !exists {
					// consumer group doesnt exist, create new one.
					subscriberC := make(chan T, r.conf.BufferSize)
					subscribers[r.conf.ConsumerGroup] = subscriberC
					if len(subscribers) == 1 {
						// first subscriber, hot-swap nil publish channel for active one
						publish = publishC
					}
					r.responseC <- subscribeResponse[T]{ch: subscriberC, success: true} // reply to sender
				} else {
					// consumer group exists, pass back existing channel reference.
					r.responseC <- subscribeResponse[T]{ch: ch, success: false} // reply to sender
				}
			default:
				fmt.Println("error: broadcaster processing unknown requests", r)
			}

		// NOTE: Anytime select chooses ctx.Done or broadcastC, it interrupts the publishing.
		// So ideally we only want to send requests to broadcastC if they are necessary, such as:
		// the creation or deletion of a subscriber because we need to update the subscribers map
		// between publishing.
		case msg := <-publish:
			for _, sub := range subscribers {
				select {
				case <-i.ctx.Done():
					publish = nil // ensure 'publish' case cant be selected anymore
				case sub <- msg:
					// pipe the new message to each subscriber
				}
			}
		}

	}
}
