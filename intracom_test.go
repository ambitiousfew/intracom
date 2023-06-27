package intracom

import (
	"reflect"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"

	ch := ic.Subscribe(topic, id, 1)
	defer ic.Unsubscribe(topic, id)

	want := true

	channel, topicExists := ic.manager.get(topic)
	if !topicExists {
		t.Errorf("want %v: got %v", want, topicExists)
		return
	}

	_, chanExists := channel.get(id)

	if !chanExists {
		t.Errorf("want %v: got %v", want, chanExists)
		return
	}

	size := channel.len()
	if size != 1 {
		t.Errorf("want %d: got %d", 1, size)
	}

	select {
	case <-ch:
		t.Errorf("should not have received a message, nothing has been published")
	default:
		return
	}
}

func TestUnsubscribe(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"
	ic.Subscribe(topic, id, 1)

	want := true

	// ensure the topic exists first
	channel, topicExists := ic.manager.get(topic)
	if topicExists != want {
		t.Errorf("want %v: got %v", want, topicExists)
		return
	}

	_, chanExists := channel.get(id)

	if chanExists != want {
		t.Errorf("want %v: got %v", want, chanExists)
		return
	}

	size := channel.len()
	if size != 1 {
		t.Errorf("want %d: got %d", 1, size)
	}

	ic.Unsubscribe(topic, id)
	want = false
	channel, topicExists = ic.manager.get(topic)

	if topicExists != want {
		t.Errorf("want %v: got %v", want, topicExists)
		return
	}
}

func TestLastMessageNoSubscribers(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"

	want := true

	ic.Publish(topic, want)

	channel, exists := ic.manager.get(topic)
	if exists {
		t.Errorf("channel topic '%s' exists and should not", topic)
	}

	if channel != nil {
		t.Errorf("channel instance '%s' exists and should not", topic)
	}
}

func TestLastMessageSubscribers(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"
	msg := true

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ch := ic.Subscribe(topic, id, 1)
	// publish first so the message is stored in the last message maps topic.
	ic.Publish(topic, msg)

	channel, exists := ic.manager.get(topic)
	if !exists {
		t.Errorf("channel topic '%s' does not exist but should", topic)
	}

	want := channel.message()

	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()
	select {
	case got := <-ch:
		if got != want {
			t.Errorf("want %v: got %v", want, got)
			return
		}
	case <-timeout.C:
		t.Errorf("didnt receive the last message on subscribe")
		return
	}
}

func TestMultipleUnbufferedSubscribesWithAsyncPublish(t *testing.T) {
	ic := New[string]()

	topic := "test-topic"
	id := "test"

	ch1 := ic.Subscribe(topic, id, 0)
	go ic.Publish(topic, "hello1")

	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()
	var msg1 string
	select {
	case msg1 = <-ch1:
	case <-timeout.C:
		t.Errorf("did not receive the published message")
	}

	timeout.Reset(1 * time.Second)
	ch1Copy := ic.Subscribe(topic, id, 0)
	var msg2 string
	select {
	case msg2 = <-ch1:
	case <-timeout.C:
		t.Errorf("did not receive the published message for the 2nd subscription")
	}

	ic.Publish(topic, "hello2")
	ch2 := ic.Subscribe(topic, id, 0)

	if ch1 != ch2 && ch1 != ch1Copy && ch2 != ch1Copy {
		t.Errorf("subscribing with the same topic and id resulted in different channel")
	}

	if msg1 != msg2 {
		t.Errorf("subscribing multiple times didn't return the same last message: want %s, got %s", msg1, msg2)
	}
}

func TestMultipleBufferedSubscribesWithAsyncPublish(t *testing.T) {
	ic := New[string]()

	topic := "test-topic"
	id := "test"

	ch1 := ic.Subscribe(topic, id, 1)
	go ic.Publish(topic, "hello1")

	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()
	var msg1 string
	select {
	case msg1 = <-ch1:
	case <-timeout.C:
		t.Errorf("did not receive the published message")
	}

	timeout.Reset(1 * time.Second)
	ch1Copy := ic.Subscribe(topic, id, 1)
	var msg2 string
	select {
	case msg2 = <-ch1:
	case <-timeout.C:
		t.Errorf("did not receive the published message for the 2nd subscription")
	}

	ic.Publish(topic, "hello2")
	ch2 := ic.Subscribe(topic, id, 1)

	if ch1 != ch2 && ch1 != ch1Copy && ch2 != ch1Copy {
		t.Errorf("subscribing with the same topic and id resulted in different channel")
	}

	if msg1 != msg2 {
		t.Errorf("subscribing multiple times didn't return the same last message: want %s, got %s", msg1, msg2)
	}
}

func TestMultipleSubscribesWithoutPublish(t *testing.T) {
	ic := New[string]()

	topic := "test-topic"
	id := "test"

	ch1 := ic.Subscribe(topic, id, 1)
	ch1Copy := ic.Subscribe(topic, id, 1)

	if ch1 != ch1Copy {
		t.Errorf("subscribing with the same topic and id resulted in different channel")
	}

	select {
	case <-ch1:
		t.Errorf("subscribing without publish should not receive any last published message for ch1")
	case <-ch1Copy:
		t.Errorf("subscribing without publish should not receive any last published message for ch1Copy")
	default:
		// we should hit this case
		return

	}
}

func TestMultipleUnSubscribes(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(topic, id, 1)

	channel, found := ic.manager.get(topic)
	if !found {
		t.Errorf("channel topic doesnt exist")
	}

	if _, exists := channel.get(id); !exists {
		t.Errorf("consumer '%s' does not exist in channel '%s' doesnt exist", id, topic)
	}

	ic.Unsubscribe(topic, id)

	if _, exists := channel.get(id); exists {
		t.Errorf("consumer '%s' exists after unsubscribe from channel '%s'", id, topic)
	}

	ic.Unsubscribe(topic, id)

	if _, exists := channel.get(id); exists {
		t.Errorf("consumer channel exists after multiple unsubscribes")
		t.Errorf("consumer '%s' still exists after multiple unsubscribes from channel '%s'", id, topic)
	}
}

func TestUnsubscribeNonExistent(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	notTopic := "not-a-topic"
	id := "test"

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(topic, id, 1)
	ic.Unsubscribe(notTopic, id)

	if _, exists := ic.manager.get(topic); !exists {
		t.Errorf("topic '%s' should exist", topic)
	}

	if _, exists := ic.manager.get(notTopic); exists {
		t.Errorf("topic '%s' should NOT exist", notTopic)
	}
}

func TestPubSub(t *testing.T) {
	ic := New[bool]()
	topic := "test-topic"
	id := "test"
	want := true

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ch := ic.Subscribe(topic, id, 0)
	// defer ic.Unsubscribe(topic, id)

	ic.Publish(topic, want)

	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()
	select {
	case msg := <-ch:
		if msg != want {
			t.Errorf("subscriber did not receive correct message: want %v, got %v", want, msg)
		}
		return
	case <-timeout.C:
		t.Errorf("subscriber did not receive published message in time")
	}
}

func TestIntracomClose(t *testing.T) {
	ic := New[bool]()
	topic := "test-topic"
	id := "test"
	want := true

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(topic, id, 1)
	defer ic.Unsubscribe(topic, id)

	if _, exists := ic.manager.get(topic); !exists {
		t.Errorf("topic '%s' should exist", topic)
	}

	ic.Publish(topic, want)

	// ensure we arent yet closed
	if ic.closed {
		t.Errorf("intracom should not be closed already")
	}

	ic.Close()
	if !ic.closed {
		t.Errorf("intracom did not close properly")
	}

	if _, exists := ic.manager.get(topic); exists {
		t.Errorf("topic '%s' should not exist after close", topic)
	}
}

// Testing typed instance creations
func TestNewBoolTyped(t *testing.T) {
	ic := New[bool]()

	want := reflect.TypeOf(new(Intracom[bool])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewStringTyped(t *testing.T) {
	ic := New[string]()

	want := reflect.TypeOf(new(Intracom[string])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewIntTyped(t *testing.T) {
	ic := New[int]()

	want := reflect.TypeOf(new(Intracom[int])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewByteTyped(t *testing.T) {
	ic := New[[]byte]()

	want := reflect.TypeOf(new(Intracom[[]byte])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}
