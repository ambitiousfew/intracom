package intracom

import (
	"testing"
	"time"
)

func TestNewIntraConsumer(t *testing.T) {
	ch := make(chan string)
	consumer := newIntraConsumer[string](ch)

	if consumer == nil {
		t.Errorf("wanted non-nil, got %v", consumer)
	}

	if consumer.ch != ch {
		t.Errorf("wanted consumer channels to match")
	}

	want := int32(0)
	got := consumer.delivered.Load()
	if want != got {
		t.Errorf("wanted %d for delivered value, got %d", want, got)
	}

	if consumer.mu == nil {
		t.Errorf("wanted non-nil for mutex, got %v", consumer.mu)
	}

	// check type of a non-interface value.
	switch signalType := interface{}(consumer.signal).(type) {
	case signal:
		/// type matched
		return
	default:
		t.Errorf("wanted a signal literal, got %v", signalType)
	}
}

func TestIntraConsumerClose(t *testing.T) {
	ch := make(chan string)
	consumer := newIntraConsumer[string](ch)

	if consumer.closed != false {
		t.Errorf("want %v, got %v", false, consumer.closed)
	}

	consumer.close()

	if consumer.closed != true {
		t.Errorf("want %v, got %v", true, consumer.closed)
	}
}

func TestIntraConsumerSend(t *testing.T) {
	ch := make(chan string)
	consumer := newIntraConsumer[string](ch)

	testC := make(chan string)

	go func() {
		select {
		case <-testC:
			// signal stop of this worker.
			return
		case <-ch:
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
