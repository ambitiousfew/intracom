package intracom

// func TestNewChannel(t *testing.T) {
// 	channel := newChannel[string]()

// 	if channel == nil {
// 		t.Errorf("channel instance should not be nil")
// 	}

// 	if len(channel.subscribers) != 0 {
// 		t.Errorf("expected empty consumers map, got %d", len(channel.subscribers))
// 	}

// 	if channel.lastMessage != nil {
// 		t.Errorf("expected lastMessage to be nil, got %v", channel.lastMessage)
// 	}

// }

// func TestChannelNoSubscribe(t *testing.T) {
// 	channel := newChannel[string]()

// 	id := "test-1"

// 	want := true

// 	_, got := channel.get(id)

// 	if got {
// 		t.Errorf("want %v, got %v", want, got)
// 	}
// }

// func TestChannelSubscribe(t *testing.T) {
// 	channel := newChannel[string]()
// 	consumer := newConsumer[string](0)

// 	id := "test-1"

// 	channel.subscribe(id, consumer)

// 	want := true

// 	_, got := channel.get(id)

// 	if !got {
// 		t.Errorf("want %v, got %v", want, got)
// 	}
// }

// func TestChannelClose(t *testing.T) {
// 	channel := newChannel[string]()

// 	consumer := newConsumer[string](0)

// 	id := "test-1"

// 	testC := make(chan string)

// 	timeout := time.NewTimer(3 * time.Second)
// 	want := "success"

// 	go func() {
// 		// watch for a close of the subscriber channel
// 		for {
// 			select {
// 			case <-timeout.C:
// 				// send a fail if we dont receive a close fast enough.
// 				testC <- "fail"
// 			case msg := <-consumer.ch:
// 				if msg == "" {
// 					// if we received a close on a string channel.
// 					testC <- "success"
// 				}
// 			case got := <-testC:
// 				// receive a success if the channel closed within the allotted time
// 				// otherwise receive a fail.
// 				if got != want {
// 					t.Errorf("want %s, got %s", want, got)
// 				}
// 				return
// 			}
// 		}
// 	}()

// 	channel.subscribe(id, consumer)
// 	channel.unsubscribe(id)

// 	// nothing has been published, lastMessage stays nil.
// 	if channel.lastMessage != nil {
// 		t.Errorf("want nil, got %v", channel.lastMessage)
// 	}

// 	if channel.mu == nil {
// 		t.Errorf("want non-nil consumer map mutex, got nil")
// 	}

// }
