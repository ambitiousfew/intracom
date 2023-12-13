package intracom

import "fmt"

type subscriber[T any] struct {
	rxC chan T
	txC chan T

	ch   chan T
	open bool
	// doneC chan struct{}
}

func (s *subscriber[T]) manager() {
	for recvMsg := range s.rxC {
		select {
		case s.ch <- recvMsg:
		default:
			<-s.ch          // pop one
			s.ch <- recvMsg // push one
		}
	}
}

func (s *subscriber[T]) send(message T) {
	if s.open {
		select {
		case s.ch <- message:
		default:
			fmt.Println("pop one")
			_, open := <-s.ch
			if !open {
				fmt.Println("channel was closed")
				return
			}
			fmt.Println("push one")
			// <-s.ch          // pop one
			s.ch <- message // push one
		}
	}
}

// // channel represents a single channel topic which holds all subscriptions to that topic
type channel[T any] struct {
	id          string
	subscribers map[string]*subscriber[T]
	lastMessage *T

	addC     chan addRequest[T]
	removeC  chan string
	lookupC  chan subscriberRequest[T]
	publishC chan T

	doneC chan struct{}
}

func newChannel[T any](name string) *channel[T] {
	ch := &channel[T]{
		id: name,

		subscribers: make(map[string]*subscriber[T]),

		addC:     make(chan addRequest[T], 1),
		removeC:  make(chan string, 1),
		lookupC:  make(chan subscriberRequest[T], 1),
		publishC: make(chan T, 1),

		doneC:       make(chan struct{}, 1),
		lastMessage: nil,
	}

	// manages shared state via message passing
	go ch.startManager()
	return ch
}

func (c *channel[T]) startManager() {
	// defer fmt.Println("exiting channel manager")
	for {
		select {
		case <-c.doneC:
			// receive a done signal, close all subscribers.
			for _, s := range c.subscribers {
				if s.open {
					// if its not already closed, close it.
					s.open = false
					close(s.ch)
				}
			}
			// clear the map
			c.subscribers = make(map[string]*subscriber[T])
			// fmt.Printf("exiting channel %s...", c.id)
			return

		case id := <-c.removeC:
			if sub, exists := c.subscribers[id]; exists {
				if sub.open {
					// c.subscribers[id] = &subscriber[T]{
					// 	open:  false,
					// 	ch:    sub.ch,
					// 	doneC: make(chan struct{}, 1),
					// }
					sub.open = false
					close(sub.ch)
					delete(c.subscribers, id)
				}
			}

		case lookupReq := <-c.lookupC:
			var exists bool
			response := subscriberResponse[T]{}
			// check for consumer existence
			// ensure channel exists first
			sub, exists := c.subscribers[lookupReq.consumerID]
			if exists {
				response.ch = sub.ch
				response.found = exists
			}

			response.lastMessage = c.lastMessage
			lookupReq.ch <- response

		case addReq := <-c.addC:
			// handles adding new subscriptions
			if _, exists := c.subscribers[addReq.consumerID]; !exists {
				c.subscribers[addReq.consumerID] = &subscriber[T]{
					ch:   addReq.ch,
					open: true,
					// doneC: make(chan struct{}, 1),
				}
			}

		case message := <-c.publishC:
			c.lastMessage = &message
			// handles broadcasting a message to all subscribers.
			for _, sub := range c.subscribers {
				go sub.send(message)

			}
		}
	}
}

func (c *channel[T]) Subscribe(consumerID string, bufferSize int) chan T {
	if bufferSize < 1 {
		bufferSize = 1
	}

	var sub chan T
	// attempt to do a lookup to se if it already exists
	request := subscriberRequest[T]{
		consumerID: consumerID,
		ch:         make(chan subscriberResponse[T], 1),
	}

	// fmt.Printf("**DEBUG %s sending lookup request for %s\n", c.id, request.consumerID)
	c.lookupC <- request
	response := <-request.ch

	close(request.ch)

	if !response.found {
		// create the subscriber channel
		// add it and return it.
		sub = make(chan T, bufferSize)
		c.addC <- addRequest[T]{
			consumerID: consumerID,
			ch:         sub,
		}
	} else {
		sub = response.ch
	}

	if response.lastMessage != nil {
		select {
		case sub <- *response.lastMessage:
			// fmt.Println("able to push message")
			// try to push last message in
		default:
			// fmt.Println("not able to push, pop one")
			// buffer was full
			<-sub // pop one
			// fmt.Println("now push one")
			sub <- *response.lastMessage // push one
		}
	}

	return sub
}

func (c *channel[T]) Unsubscribe(consumerID string) {
	c.removeC <- consumerID
}

func (c *channel[T]) Close() {
	c.doneC <- struct{}{}
}

func (c *channel[T]) Publish(message T) {
	c.publishC <- message
}
