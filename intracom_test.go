package intracom

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// import (
// 	"fmt"
// 	"reflect"
// 	"testing"
// 	"time"
// )

// func TestSubscribe(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"
// 	id := "test"

// 	sub := ic.Subscribe(topic, id, 1)
// 	defer ic.Unsubscribe(topic, id)

// 	want := true

// 	channel, topicExists := ic.manager.get(topic)
// 	if !topicExists {
// 		t.Errorf("want %v: got %v", want, topicExists)
// 		return
// 	}

// 	_, chanExists := channel.get(id)

// 	if !chanExists {
// 		t.Errorf("want %v: got %v", want, chanExists)
// 		return
// 	}

// 	size := channel.len()
// 	if size != 1 {
// 		t.Errorf("want %d: got %d", 1, size)
// 	}

// 	timeout := 100 * time.Millisecond

// 	_, ok := sub.NextMsg(timeout)
// 	if ok {
// 		t.Errorf("should not have received a message, nothing has been published")
// 	}

// }

// func TestUnsubscribe(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"
// 	id := "test"
// 	ic.Subscribe(topic, id, 1)

// 	want := true

// 	// ensure the topic exists first
// 	channel, topicExists := ic.manager.get(topic)
// 	if topicExists != want {
// 		t.Errorf("want %v: got %v", want, topicExists)
// 		return
// 	}

// 	_, chanExists := channel.get(id)

// 	if chanExists != want {
// 		t.Errorf("want %v: got %v", want, chanExists)
// 		return
// 	}

// 	size := channel.len()
// 	if size != 1 {
// 		t.Errorf("want %d: got %d", 1, size)
// 	}

// 	ic.Unsubscribe(topic, id)
// 	want = false
// 	channel, topicExists = ic.manager.get(topic)

// 	if topicExists != want {
// 		t.Errorf("want %v: got %v", want, topicExists)
// 		return
// 	}
// }

// func TestLastMessageNoSubscribers(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"

// 	want := true

// 	ic.Publish(topic, want)

// 	channel, exists := ic.manager.get(topic)
// 	if exists {
// 		t.Errorf("channel topic '%s' exists and should not", topic)
// 	}

// 	if channel != nil {
// 		t.Errorf("channel instance '%s' exists and should not", topic)
// 	}
// }

// func TestLastMessageSubscribers(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"
// 	id := "test"
// 	msg := true

// 	// subscribe and we should receive an immediate message if there is a message in the last message map
// 	sub := ic.Subscribe(topic, id, 1)
// 	// publish first so the message is stored in the last message maps topic.
// 	ic.Publish(topic, msg)

// 	channel, exists := ic.manager.get(topic)
// 	if !exists {
// 		t.Errorf("channel topic '%s' does not exist but should", topic)
// 	}

// 	want := channel.message()

// 	timeout := 1 * time.Second

// 	msg, ok := sub.NextMsg(timeout)
// 	if !ok {
// 		t.Errorf("didnt receive the last message on subscribe")
// 	}

// 	if msg != want {
// 		t.Errorf("want %v: got %v", want, msg)
// 	}
// }

// func TestMultipleBufferedSubscribesWithAsyncPublish(t *testing.T) {
// 	ic := New[string]()

// 	topic := "test-topic"
// 	id := "test"
// 	testMsg := "hello1"
// 	sub1 := ic.Subscribe(topic, id, 1)
// 	ic.Publish(topic, testMsg)

// 	msg1, ok := sub1.NextMsg(10 * time.Millisecond)
// 	if !ok {
// 		t.Errorf("did not receive the published message within the timeout duration")
// 	}

// 	msg2, ok := sub1.NextMsg(10 * time.Millisecond)
// 	if ok {
// 		t.Errorf("should not have received any messages")
// 	}

// 	ic.Publish(topic, testMsg)
// 	sub2 := ic.Subscribe(topic, id, 1)

// 	if sub1 != sub2 {
// 		t.Errorf("subscribing with the same topic and id resulted in different consumer")
// 	}

// 	if msg1 != testMsg {
// 		t.Errorf("test msg1 want %s, got %s", msg1, msg2)
// 	}

// 	if msg2 != "" {
// 		t.Errorf("test msg2 want %s, got %s", msg1, msg2)
// 	}
// }

// func TestMultiplePublishersOneSubscriber(t *testing.T) {
// 	ic := New[string]()

// 	topic := "multiple-publish"
// 	id := "subscriber-1"
// 	msgsToPublish := 5

// 	sub := ic.Subscribe(topic, id, msgsToPublish)

// 	for i := 0; i < msgsToPublish; i++ {
// 		go ic.Publish(topic, "hello")
// 	}

// 	timeout := 250 * time.Millisecond

// 	for i := 0; i < msgsToPublish; i++ {
// 		_, ok := sub.NextMsg(timeout)
// 		if !ok {
// 			t.Errorf("timed out before receiving message")
// 		}
// 	}
// }

// func TestMultipleSubscribersOnePublisher(t *testing.T) {
// 	ic := New[map[string]string]()

// 	topic := "broadcaster-of-states"
// 	id1 := "subscriber-1"
// 	id2 := "subscriber-2"
// 	id3 := "subscriber-3"

// 	sub1 := ic.Subscribe(topic, id1, 0)
// 	sub2 := ic.Subscribe(topic, id2, 0)
// 	sub3 := ic.Subscribe(topic, id3, 0)

// 	states := make(map[string]string)
// 	states[id1] = "initial"
// 	states[id2] = "initial"
// 	states[id3] = "initial"

// 	updateToState := "finish"

// 	for i := 0; i < 3; i++ {
// 		states[fmt.Sprintf("subscriber-%d", i+1)] = updateToState
// 		ic.Publish(topic, states)
// 	}

// 	timeout := 100 * time.Millisecond

// 	msg1, ok1 := sub1.NextMsg(timeout)
// 	if !ok1 {
// 		t.Errorf("timed out before receiving message")
// 	}

// 	if msg1[id1] != "finish" {
// 		t.Errorf("did not receive correct finish state")
// 	}

// 	msg2, ok2 := sub2.NextMsg(timeout)
// 	if !ok2 {
// 		t.Errorf("timed out before receiving message")
// 	}

// 	if msg2[id2] != "finish" {
// 		t.Errorf("did not receive correct finish state")
// 	}

// 	msg3, ok3 := sub3.NextMsg(timeout)
// 	if !ok3 {
// 		t.Errorf("timed out before receiving message")
// 	}

// 	if msg3[id3] != "finish" {
// 		t.Errorf("did not receive correct finish state")
// 	}
// }

// func TestMultipleSubscribesWithoutPublish(t *testing.T) {
// 	ic := New[string]()

// 	topic := "test-topic"
// 	id := "test"

// 	sub1 := ic.Subscribe(topic, id, 1)
// 	sub2 := ic.Subscribe(topic, id, 1)

// 	if sub1 != sub2 {
// 		t.Errorf("subscribing with the same topic and id resulted in different consumer")
// 	}

// 	timeout := 100 * time.Millisecond

// 	_, ok1 := sub1.NextMsg(timeout)
// 	if ok1 {
// 		t.Errorf("consumer 1 should not receive a message")
// 	}

// 	_, ok2 := sub2.NextMsg(timeout)
// 	if ok2 {
// 		t.Errorf("consumer 2 should not receive a message")
// 	}
// }

// func TestMultipleUnSubscribes(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"
// 	id := "test"

// 	// subscribe and we should receive an immediate message if there is a message in the last message map
// 	ic.Subscribe(topic, id, 1)

// 	channel, found := ic.manager.get(topic)
// 	if !found {
// 		t.Errorf("channel topic doesnt exist")
// 	}

// 	if _, exists := channel.get(id); !exists {
// 		t.Errorf("consumer '%s' does not exist in channel '%s' doesnt exist", id, topic)
// 	}

// 	ic.Unsubscribe(topic, id)

// 	if _, exists := channel.get(id); exists {
// 		t.Errorf("consumer '%s' exists after unsubscribe from channel '%s'", id, topic)
// 	}

// 	ic.Unsubscribe(topic, id)

// 	if _, exists := channel.get(id); exists {
// 		t.Errorf("consumer channel exists after multiple unsubscribes")
// 		t.Errorf("consumer '%s' still exists after multiple unsubscribes from channel '%s'", id, topic)
// 	}
// }

// func TestUnsubscribeNonExistent(t *testing.T) {
// 	ic := New[bool]()

// 	topic := "test-topic"
// 	notTopic := "not-a-topic"
// 	id := "test"

// 	// subscribe and we should receive an immediate message if there is a message in the last message map
// 	ic.Subscribe(topic, id, 1)
// 	ic.Unsubscribe(notTopic, id)

// 	if _, exists := ic.manager.get(topic); !exists {
// 		t.Errorf("topic '%s' should exist", topic)
// 	}

// 	if _, exists := ic.manager.get(notTopic); exists {
// 		t.Errorf("topic '%s' should NOT exist", notTopic)
// 	}
// }

// func TestPubSubUsingBool(t *testing.T) {
// 	ic := New[bool]()
// 	topic := "test-topic"
// 	id := "test"
// 	want := true

// 	// subscribe and we should receive an immediate message if there is a message in the last message map
// 	sub := ic.Subscribe(topic, id, 0)
// 	// defer ic.Unsubscribe(topic, id)

// 	ic.Publish(topic, want)

// 	timeout := 100 * time.Millisecond

// 	msg, ok := sub.NextMsg(timeout)
// 	if !ok {
// 		t.Errorf("subscriber did not receive published message in time")
// 	}

// 	if msg != want {
// 		t.Errorf("subscriber did not receive correct message: want %v, got %v", want, msg)
// 	}
// }

// func TestIntracomClose(t *testing.T) {
// 	ic := New[bool]()
// 	topic := "test-topic"
// 	id := "test"
// 	want := true

// 	// subscribe and we should receive an immediate message if there is a message in the last message map
// 	ic.Subscribe(topic, id, 1)
// 	defer ic.Unsubscribe(topic, id)

// 	if _, exists := ic.manager.get(topic); !exists {
// 		t.Errorf("topic '%s' should exist", topic)
// 	}

// 	ic.Publish(topic, want)

// 	ic.Close()

// 	if _, exists := ic.manager.get(topic); exists {
// 		t.Errorf("topic '%s' should not exist after close", topic)
// 	}
// }

// // Testing typed instance creations
// func TestNewBoolTyped(t *testing.T) {
// 	ic := New[bool]()

// 	want := reflect.TypeOf(new(Intracom[bool])).String()
// 	got := reflect.TypeOf(ic).String()

// 	if want != got {
// 		t.Errorf("want %s: got %s", want, got)
// 	}

// }

// func TestNewStringTyped(t *testing.T) {
// 	ic := New[string]()

// 	want := reflect.TypeOf(new(Intracom[string])).String()
// 	got := reflect.TypeOf(ic).String()

// 	if want != got {
// 		t.Errorf("want %s: got %s", want, got)
// 	}

// }

// func TestNewIntTyped(t *testing.T) {
// 	ic := New[int]()

// 	want := reflect.TypeOf(new(Intracom[int])).String()
// 	got := reflect.TypeOf(ic).String()

// 	if want != got {
// 		t.Errorf("want %s: got %s", want, got)
// 	}

// }

// func TestNewByteTyped(t *testing.T) {
// 	ic := New[[]byte]()

// 	want := reflect.TypeOf(new(Intracom[[]byte])).String()
// 	got := reflect.TypeOf(ic).String()

// 	if want != got {
// 		t.Errorf("want %s: got %s", want, got)
// 	}

// }

func countMessages[T any](num int, sub chan T, subCh chan int) {
	var total int
	for {
		_, ok := <-sub
		if !ok {
			subCh <- total
			break
		} else {
			total++
		}
	}
}

func BenchmarkIntracom(b *testing.B) {
	runtime.GOMAXPROCS(1)
	runtime.SetMutexProfileFraction(3)
	pubSub := New[string]()

	channel := pubSub.Register("channel1")

	// totalC := make(chan int, 1)
	totalSub1 := make(chan int, 1)
	totalSub2 := make(chan int, 1)
	totalSub3 := make(chan int, 1)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		sub1 := channel.Subscribe("sub1", 10)
		countMessages[string](b.N, sub1, totalSub1)
	}()

	go func() {
		defer wg.Done()
		sub2 := channel.Subscribe("sub2", 10)
		countMessages[string](b.N, sub2, totalSub2)
	}()

	go func() {
		defer wg.Done()
		sub3 := channel.Subscribe("sub3", 10)
		countMessages[string](b.N, sub3, totalSub3)
	}()

	time.Sleep(250 * time.Millisecond)
	b.ResetTimer()

	go func() {
		defer wg.Done()
		// time.Sleep(10 * time.Millisecond)
		// defer pubSub.Unregister("channel1")

		for i := 0; i < b.N; i++ {
			channel.Publish("Test message")
			// time.Sleep(10 * time.Millisecond)
		}

		fmt.Println("closing....")
		channel.Close()

	}()
	fmt.Println("waiting...")
	wg.Wait()

	got1 := <-totalSub1
	if got1 != b.N {
		b.Errorf("expected %d total, got %d", b.N, got1)
	}

	got2 := <-totalSub2
	if got2 != b.N {
		b.Errorf("expected %d total, got %d", b.N, got2)
	}

	got3 := <-totalSub3
	if got3 != b.N {
		b.Errorf("expected %d total, got %d", b.N, got3)
	}

	b.StopTimer()

}
