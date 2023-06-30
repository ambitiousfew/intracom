package intracom

import (
	"testing"
	"time"
)

func TestnewChannel(t *testing.T) {
	channel := newChannel[string]()

	if channel == nil {
		t.Errorf("channel instance should not be nil")
	}

	if len(channel.consumers) != 0 {
		t.Errorf("expected empty consumers map, got %d", len(channel.consumers))
	}

	if channel.lastMessage != nil {
		t.Errorf("expected lastMessage to be nil, got %v", channel.lastMessage)
	}

	if channel.cmu == nil {
		t.Errorf("expected consumer map mutex to be non-nil, got %v", channel.cmu)
	}

	if channel.lmu == nil {
		t.Errorf("expected last message mutex to be non-nil, got %v", channel.lmu)
	}
}

func TestIntraChannelNoSubscribe(t *testing.T) {
	channel := newChannel[string]()

	id := "test-1"

	want := true

	_, got := channel.get(id)

	if got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestIntraChannelSubscribe(t *testing.T) {
	channel := newChannel[string]()
	consumer := newConsumer[string](0)

	id := "test-1"

	channel.subscribe(id, consumer)

	want := true

	_, got := channel.get(id)

	if !got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestIntraChannelClose(t *testing.T) {
	channel := newChannel[string]()

	consumer := newConsumer[string](0)

	id := "test-1"

	testC := make(chan string)

	timeout := time.NewTimer(3 * time.Second)
	want := "success"

	go func() {
		// watch for a close of the subscriber channel
		for {
			select {
			case <-timeout.C:
				// send a fail if we dont receive a close fast enough.
				testC <- "fail"
			case msg := <-consumer.ch:
				if msg == "" {
					// if we received a close on a string channel.
					testC <- "success"
				}
			case got := <-testC:
				// receive a success if the channel closed within the allotted time
				// otherwise receive a fail.
				if got != want {
					t.Errorf("want %s, got %s", want, got)
				}
				return
			}
		}
	}()

	channel.subscribe(id, consumer)
	channel.unsubscribe(id)

	// nothing has been published, lastMessage stays nil.
	if channel.lastMessage != nil {
		t.Errorf("want nil, got %v", channel.lastMessage)
	}

	if channel.cmu == nil {
		t.Errorf("want non-nil consumer map mutex, got nil")
	}

	if channel.lmu == nil {
		t.Errorf("want non-nil last message mutex, got nil")
	}

}
