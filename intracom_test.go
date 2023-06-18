package intracom

import (
	"reflect"
	"testing"
)

func TestSubscribe(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"

	ch := ic.Subscribe(topic, id)
	defer ic.Unsubscribe(topic, id)

	want := true

	consumers, topicExists := ic.channels[topic]
	if !topicExists {
		t.Errorf("want %v: got %v", want, topicExists)
		return
	}

	_, chanExists := consumers[id]

	if !chanExists {
		t.Errorf("want %v: got %v", want, chanExists)
		return
	}

	if len(consumers) != 1 {
		t.Errorf("want %d: got %d", 1, len(consumers))
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
	ic.Subscribe(topic, id)
	ic.Unsubscribe(topic, id)

	want := true

	consumers, topicExists := ic.channels[topic]
	if topicExists != want {
		t.Errorf("want %v: got %v", want, topicExists)
		return
	}

	want = false
	_, chanExists := consumers[id]

	if chanExists != want {
		t.Errorf("want %v: got %v", want, chanExists)
		return
	}

	if len(consumers) != 0 {
		t.Errorf("want %d: got %d", 0, len(consumers))
	}
}

func TestLastMessageNoSubscribers(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"

	want := true

	ic.Publish(topic, want)

	got, exists := ic.lastMessage[topic]
	if !exists {
		t.Errorf("last message topic '%s' did not exists", topic)
	}

	if *got != want {
		t.Errorf("want %v: got %v", want, got)
	}
}

func TestLastMessageSubscribers(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"
	msg := true
	// publish first so the message is stored in the last message maps topic.
	ic.Publish(topic, msg)
	// subscribe and we should receive an immediate message if there is a message in the last message map
	ch := ic.Subscribe(topic, id)

	want, exists := ic.lastMessage[topic]
	if !exists {
		t.Errorf("last message topic '%s' did not exists", topic)
	}

	select {
	case got := <-ch:
		if got != *want {
			t.Errorf("want %v: got %v", want, got)
			return
		}
	default:
		t.Errorf("didnt receive the last message on subscribe")
		return
	}
}

func TestMultipleSubscribesWithPublish(t *testing.T) {
	ic := New[string]()

	topic := "test-topic"
	id := "test"

	ch1 := ic.Subscribe(topic, id)
	ic.Publish(topic, "hello1")
	msg1 := <-ch1

	ch1Copy := ic.Subscribe(topic, id)
	msg2 := <-ch1Copy

	ic.Publish(topic, "hello2")
	ch2 := ic.Subscribe(topic, id)

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

	ch1 := ic.Subscribe(topic, id)
	ch1Copy := ic.Subscribe(topic, id)

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
	ic.Subscribe(topic, id)

	consumer, found := ic.channels[topic]
	if !found {
		t.Errorf("channel topic doesnt exist")
	}

	if _, exists := consumer[id]; !exists {
		t.Errorf("consumer channel doesnt exist")
	}

	ic.Unsubscribe(topic, id)

	if _, exists := consumer[id]; exists {
		t.Errorf("consumer channel exists after unsubscribe")
	}

	ic.Unsubscribe(topic, id)

	if _, exists := consumer[id]; exists {
		t.Errorf("consumer channel exists after multiple unsubscribes")
	}
}

func TestUnsubscribeNonExistent(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	notTopic := "not-a-topic"
	id := "test"

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(topic, id)
	ic.Unsubscribe(notTopic, id)

	if _, exists := ic.channels[topic]; !exists {
		t.Errorf("topic '%s' should exist", topic)
	}

	if _, exists := ic.channels[notTopic]; exists {
		t.Errorf("topic '%s' should NOT exist", notTopic)
	}
}

func TestPubSub(t *testing.T) {
	ic := New[bool]()
	topic := "test-topic"
	id := "test"
	want := true

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ch := ic.Subscribe(topic, id)
	defer ic.Unsubscribe(topic, id)

	ic.Publish(topic, want)
	select {
	case msg := <-ch:
		if msg != want {
			t.Errorf("subscriber did not receive correct message: want %v, got %v", want, msg)
		}
		return
	default:
		t.Errorf("subscriber did not receive published message")
	}
}

func TestIntracomClose(t *testing.T) {
	ic := New[bool]()
	topic := "test-topic"
	id := "test"
	want := true

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(topic, id)
	defer ic.Unsubscribe(topic, id)

	if _, exists := ic.channels[topic]; !exists {
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

	if _, exists := ic.channels[topic]; exists {
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
