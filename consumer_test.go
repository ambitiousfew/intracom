package intracom

import (
	"testing"
	"time"
)

func TestnewConsumer(t *testing.T) {
	consumer := newConsumer[string](0)

	if consumer == nil {
		t.Errorf("wanted non-nil, got %v", consumer)
	}

	if consumer.ch == nil {
		t.Errorf("expect non-nil of consumer channel, got %v", consumer.ch)
	}

	if consumer.mu == nil {
		t.Errorf("wanted non-nil for mutex, got %v", consumer.mu)
	}

	if consumer.bufSize == 0 {
		t.Errorf("wanted 1 for bufSize, got %d", consumer.bufSize)
	}
}

func TestConsumerClose(t *testing.T) {
	consumer := newConsumer[string](0)

	if consumer.closed.Load() != false {
		t.Errorf("want %v, got %v", false, consumer.closed)
	}

	consumer.close()

	if consumer.closed.Load() != true {
		t.Errorf("want %v, got %v", true, consumer.closed)
	}
}

func TestConsumerSend(t *testing.T) {
	consumer := newConsumer[string](0)

	testC := make(chan string)

	go func() {
		select {
		case <-testC:
			// signal stop of this worker.
			return
		case <-consumer.ch:
			// message was sent to the channel.
			testC <- "success"
			return
		}

	}()

	consumer.send("hello")

	timeout := time.NewTimer(1 * time.Second)
	want := "success"

	select {
	case got := <-testC:
		if got != want {
			t.Errorf("want %s, got %s", want, got)
		}
		return
	case <-timeout.C:
		testC <- "fail"
	}

	close(testC)
}
