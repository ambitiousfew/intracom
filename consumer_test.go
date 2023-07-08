package intracom

import (
	"fmt"
	"sync"
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

func TestConsumerFetch10(t *testing.T) {

	fetchSize := 10
	multiplier := 3

	// produce 30 msgs
	totalToProduce := fetchSize * multiplier
	// create consumer with larger buffer than we want to fetch.
	consumer := newConsumer[string](totalToProduce * 2)

	testC := make(chan string)

	// duration to wait for messages
	duration := 10 * time.Millisecond

	var producedMsgCount int
	// producedMsgCount is shared between producer routine and main routine
	mu := new(sync.RWMutex)

	go func() {
		mu.Lock()
		defer mu.Unlock()
		// producer of constant stream of messages up to totalToProduce
		for producedMsgCount < totalToProduce {
			select {
			case <-testC:
				// signal stop of this worker.
				return
			default:
				if producedMsgCount < multiplier {
					// delay 50ms of time to interfere with our fetches of 10.
					time.Sleep(duration)
				}
				// keep sending into consumer
				consumer.send(fmt.Sprintf("hello %d", producedMsgCount))
				// 50ms * 30 messages = 1500ms - acts as an interrupt of time for the duration
				producedMsgCount++
			}
		}
	}()

	received := make([]string, 0)

	// signal stop of the test after duration(10ms) * multiplier(3) -> 30ms*2 -> 60ms
	// to give our test enough time to fetch and match against how many msgs were produced.
	timeout := time.NewTimer(duration * time.Duration(multiplier*2))
	defer timeout.Stop()

	done := false
	for !done {
		select {
		case <-timeout.C:
			// we should be done fetching in 1 second
			// 32 messages come through within 320ms
			// fetch waits 250ms each fetch.
			done = true
			// stop producing messages and stop counting produced messages.
			close(testC)
			continue
		default:
			msgs, ok := consumer.Fetch(fetchSize, duration)
			if !ok && len(msgs) > 0 {
				// ok should be true if we have messages even on timeout.
				t.Errorf("got %v, want %v", ok, len(msgs) > 0)
				return
			}

			received = append(received, msgs...)
		}
	}

	// even fetching 10 at a time, we should still end up with the msgs sent and delayed sent
	// within the allocated test time.

	mu.RLock()
	defer mu.RUnlock()
	if len(received) != producedMsgCount {
		t.Errorf("want %d, got %d", producedMsgCount, len(received))
	}

}
