package intracom

import (
	"context"
	"fmt"
)

// Intracom is a thread-safe in-memory pub/sub implementation.
type Intracom[T any] struct {
	// ctx is the context that will be used to cancel all work being done by the Intracom instance.
	ctx context.Context

	// requestC is the channel that will be used to send requests to the Intracom instance.
	requestC    chan any
	brokerDoneC chan struct{}
}

// New returns a new Intracom instance.
func New[T any](ctx context.Context) *Intracom[T] {
	ic := &Intracom[T]{
		ctx:         ctx,
		requestC:    make(chan any),
		brokerDoneC: make(chan struct{}, 1),
	}

	go ic.broker() // start the requests broker
	return ic
}

// broker is a blocking function that will handle all requests to the Intracom instance.
func (i *Intracom[T]) broker() {
	channels := make(map[string]*intracomChannel[T])

	ctxDone := i.ctx.Done()

	for {
		select {
		case <-i.brokerDoneC:
			for _, channel := range channels {
				channel.close() // close waiting for signal of completion
			}
			return

		case <-ctxDone:
			for _, channel := range channels {
				channel.close() // close waiting for signal of completion
			}

			ctxDone = nil // prevent this case from being selected again
			close(i.brokerDoneC)

		case request := <-i.requestC:
			switch r := request.(type) {
			case intracomUnregisterRequest:
				if channel, exists := channels[r.topic]; !exists {
					r.responseC <- false
				} else {
					// to unregister a topic, we need to stop all work being done.
					channel.close()
					r.responseC <- true
				}

			case intracomLookupRequest[T]:
				channel, exists := channels[r.topic]
				r.responseC <- intracomLookupResponse[T]{channel: channel, found: exists}

			case intracomRegisterRequest[T]:
				channel, exists := channels[r.topic]
				if !exists {
					// topic does not exist, create it
					publishC := make(chan T)
					channel = newIntracomChannel[T](i.ctx, r.topic, publishC)
					channels[r.topic] = channel
				}
				// respond to request
				r.responseC <- intracomRegisterResponse[T]{publishC: channel.publishC, found: exists}

			case intracomUnsubscribeRequest[T]:
				if channel, exists := channels[r.topic]; !exists {
					r.responseC <- false
				} else {
					r.responseC <- channel.unsubscribe(r.consumer)
				}

			case intracomSubscribeRequest[T]:
				channel, exists := channels[r.conf.Topic]
				if !exists {
					// channel topic doesnt exist, create new one with subscriber added.
					publishC := make(chan T)
					channel = newIntracomChannel[T](i.ctx, r.conf.Topic, publishC)
					channels[r.conf.Topic] = channel
				}

				// subscribe a consumer
				ch := channel.subscribe(r.conf)
				// pass back subscriber response
				r.responseC <- intracomSubscribeResponse[T]{
					ch:      ch,
					success: ch != nil,
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
		topic:     topic,
		responseC: make(chan intracomRegisterResponse[T]),
	}

	i.requestC <- request           // send request
	response := <-request.responseC // wait for response

	close(request.responseC)
	// return publisher channel and an unregister func reference tied to this topic registration.
	return response.publishC, i.unregister(topic)

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

	// check if topic already exists
	channel, exists := i.get(conf.Topic)
	if !exists {
		request := intracomSubscribeRequest[T]{
			conf:      *conf,
			responseC: make(chan intracomSubscribeResponse[T]),
		}
		i.requestC <- request           // send request
		response := <-request.responseC // wait for response
		close(request.responseC)

		if response.success {
			return response.ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)
		}
	}

	// found existing consumer in channels
	ch, found := channel.get(conf.ConsumerGroup)
	if !found {
		ch := channel.subscribe(*conf)
		return ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)
	}

	return ch, i.unsubscribe(conf.Topic, conf.ConsumerGroup)

}

func (i *Intracom[T]) get(topic string) (*intracomChannel[T], bool) {
	request := intracomLookupRequest[T]{
		topic:     topic,
		responseC: make(chan intracomLookupResponse[T]),
	}

	i.requestC <- request           // send request
	response := <-request.responseC // wait for response
	close(request.responseC)

	return response.channel, response.found
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
			responseC: make(chan bool),
		}

		i.requestC <- request          // send request
		success := <-request.responseC // wait for response

		close(request.responseC)
		return success

	}
}

// unregister returns a closure that contains the topic name so if called will cancel
func (i *Intracom[T]) unregister(topic string) func() bool {
	return func() bool {
		if topic == "" {
			return false
		}

		request := intracomUnregisterRequest{
			topic:     topic,
			responseC: make(chan bool),
		}

		i.requestC <- request          // send request to unregister topic
		success := <-request.responseC // wait for response

		close(request.responseC)
		return success

	}

}
