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

func TestMultipleSubscribes(t *testing.T) {
	ic := New[bool]()

	topic := "test-topic"
	id := "test"

	// subscribe and we should receive an immediate message if there is a message in the last message map
	want := ic.Subscribe(topic, id)
	got := ic.Subscribe(topic, id)

	if want != got {
		t.Errorf("subscribing with the same topic and id resulted in different channel")
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

	want := reflect.TypeOf(&Intracom[bool]{})
	got := reflect.TypeOf(ic)

	if want != got {
		t.Errorf("want %T: got %T", want, t)
	}

}

func TestNewStringTyped(t *testing.T) {
	ic := New[string]()

	want := reflect.TypeOf(&Intracom[string]{})
	got := reflect.TypeOf(ic)

	if want != got {
		t.Errorf("want %T: got %T", want, t)
	}

}

func TestNewIntTyped(t *testing.T) {
	ic := New[int]()

	want := reflect.TypeOf(&Intracom[int]{})
	got := reflect.TypeOf(ic)

	if want != got {
		t.Errorf("want %T: got %T", want, t)
	}

}

func TestNewByteTyped(t *testing.T) {
	ic := New[[]byte]()

	want := reflect.TypeOf(&Intracom[[]byte]{})
	got := reflect.TypeOf(ic)

	if want != got {
		t.Errorf("want %T: got %T", want, t)
	}

}
