package intracom

type Intracom[T any] struct {
	// channel -> subscription -> subscriber channel
	channels map[string]*channel[T]

	lookupC     chan channelRequest[T]
	registerC   chan registerRequest[T]
	unregisterC chan unregisterRequest

	doneC chan struct{}
}

func New[T any]() *Intracom[T] {
	ic := &Intracom[T]{
		channels: make(map[string]*channel[T]),

		lookupC:     make(chan channelRequest[T], 1),
		registerC:   make(chan registerRequest[T], 1),
		unregisterC: make(chan unregisterRequest, 1),

		doneC: make(chan struct{}, 1),
	}

	// manages shared state via message passing
	go ic.startManager()
	return ic
}

func (i *Intracom[T]) startManager() {
	// defer fmt.Println("exiting intracom manager")
	for {
		select {
		case <-i.doneC:
			// fmt.Println("exiting intracom...")
			return
		case unregReq := <-i.unregisterC:
			// TODO:
			channel, exists := i.channels[unregReq.topicID]
			if !exists {
				// channel doesnt exist, we are done consumer cant exist
				// fmt.Println("DEBUG: Channel doesnt exist, cannot remove it.")
				continue
			}

			// send done signal to channel
			channel.Close()
			// fmt.Println("call the channel.unsubscribe() method")
			// channel.unsubscribe(unregReq.consumerID)

		case regReq := <-i.registerC:
			// fmt.Printf("DEBUG: registering %s\n", regReq.topicID)
			_, exists := i.channels[regReq.topicID]
			if !exists {
				i.channels[regReq.topicID] = newChannel[T](regReq.topicID)
				// fmt.Printf("DEBUG: channel '%s' doesnt exist, lookup complete\n", regReq.topicID)
			}

		case lookupReq := <-i.lookupC:
			// check for consumer existence
			// ensure channel exists first
			channel, exists := i.channels[lookupReq.topicID]
			// if !exists {
			// channel doesnt exist, we are done consumer cant exist
			// fmt.Printf("DEBUG: channel '%s' doesnt exist, lookup complete\n", lookupReq.topicID)
			// }
			lookupReq.ch <- channelResponse[T]{
				channel: channel,
				found:   exists,
			}
		}
	}
}

// Register will create a channel topic and consumer.
func (i *Intracom[T]) Register(topic string) *channel[T] {
	// first perform a subscription lookup in case subscriber already exists
	// because we want dont want to accidentally create and replace the old
	// subscriber channel
	request := channelRequest[T]{
		topicID: topic,
		ch:      make(chan channelResponse[T], 1),
	}

	// send request
	// fmt.Printf("DEBUG: Sending request to register %s\n", topic)
	i.lookupC <- request

	// fmt.Printf("DEBUG: Waiting for response %s\n", topic)
	// block, waiting for a response
	response := <-request.ch
	close(request.ch)
	// fmt.Printf("DEBUG: Received response %s found:%v\n", topic, response.found)

	if !response.found {
		// if the channel doesnt exist, create it.
		channel := newChannel[T](topic)
		i.registerC <- registerRequest[T]{
			topicID: topic,
			channel: channel,
		}

		i.registerC <- registerRequest[T]{topicID: topic, channel: channel}

		return channel
	}

	return response.channel
}

func (i *Intracom[T]) Unregister(topic string) {
	i.unregisterC <- unregisterRequest{
		topicID: topic,
		// consumerID: consumerID,
	}
}

// func (i *Intracom[T]) Publish(topic string, message T) {
// 	i.publishC <- publishRequest[T]{
// 		topicID: topic,
// 		message: message,
// 	}
// }

// Close is safe to call multiple times it will inform the underlying intracom channel manager
// and all its channels and consumers to stop/close their channels.
// This will effectively cleanup all open channels and remove all subscriptions.
// The intracom instance will not be usable after this, a new instance would be required.
// func (i *Intracom[T]) Close() {
// 	if !i.closed.Swap(true) {
// 		// if we swap to true and the old val is false, we arent closed yet.
// 		i.manager.close()
// 	}
// }
