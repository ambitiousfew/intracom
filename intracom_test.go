package intracom

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestIntracomStart ensures that the closed bool is set to false when
// the intracom instance is started signifying that it is ready to be used.
func TestIntracomStarted(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nil: want nil, got %v", err)
	}

	want := false
	got := ic.closed

	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

// TestIntracomNotStarted ensures that the closed boolean is set to true
// since the intracom instance has not been started yet.
func TestIntracomNotStarted(t *testing.T) {
	ic := New[bool]("test-intracom")

	want := true
	got := ic.closed

	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

// TestRegister ensures that a topic can be registered and a channel is returned
// for publishing messages into the topic. The channel returned will be the same
// channel for the same topic as long as the intracom instance is not closed, it will act as a lookup.
func TestRegister(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}
	defer ic.Close()

	topic := "test-topic"

	publishC, _ := ic.Register(topic)

	if publishC == nil {
		t.Errorf("register returned a nil channel: want non-nil, got %v", publishC)
	}
}

// TestMultipleRegisters ensures that multiple registers can take place against
// the same topic and will effectively return the same channel for the same topic
// as long as the intracom instance is not closed, it will act as a lookup.
func TestMultipleRegisters(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}
	defer ic.Close()

	topic := "test-topic"

	firstRegister, _ := ic.Register(topic)
	secondRegister, _ := ic.Register(topic)

	if firstRegister != secondRegister {
		t.Errorf("register is returning different channels on multiple registers: want %v, got %v", firstRegister, secondRegister)
	}
}

// TestMultipleUnRegisters ensures that multiple unregisters can take place against
// against the same topic. The first unregister will remove stop the broadcaster of
// the topic, the second unregister will be ignored.
func TestMultipleUnRegisters(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}
	defer ic.Close()

	topic := "test-topic"

	publishC, unregister := ic.Register(topic)

	if publishC == nil {
		t.Errorf("register returned a nil channel: want non-nil, got %v", publishC)
	}

	unregister() // the broadcaster for the topic would be shutdown here.
	unregister() // should not panic, the request should be ignored since the topic was removed.
}

// TestSubscribe ensures that a subscriber is able to subscribe to a topic
// whether that topic already exists or not, the topic will be created if it does not exist.
func TestSubscribe(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}
	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	wantC, _ := ic.Subscribe(conf)
	want := true

	// ensure the topic was initialized.
	gotC, got := ic.get(topic, group)

	if want != got {
		t.Errorf("subscriber does not exist: want %v, got %v", want, got)
	}

	if wantC != gotC {
		t.Errorf("subscriber channel does not match: want %v, got %v", wantC, gotC)
	}
}

// TestUnsubscribe ensures that a subscriber is able to individually remove themselves from a topic.
func TestUnsubscribe(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	want := true
	_, got := ic.get(topic, group) // ensure it exists
	if want != got {
		t.Errorf("subscriber does not exist: want %v, got %v", want, got)
	}

	unsubscribe() // perform unsubscribe

	want = false
	_, got = ic.get(topic, group) // ensure it no longer exists
	if want != got {
		t.Errorf("subscriber still exists: want %v, got %v", want, got)
	}
}

// TestMultipleSubscribers ensures that unsubscribe can be called multiple times
// against the same topic without error, the request will effectively be ignored.
func TestMultipleUnSubscribes(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)
	unsubscribe()
	unsubscribe()
}

// TestUnsubscribeAfterClose ensure that a panic: send on a closed channel will happen
// if a subscriber tries to unsubscribe after intracom instance is closed.
// Close() should always be called last, when you are done using intracom.
func TestUnsubscribeAfterClose(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	topic := "test-topic"
	group := "test-subscriber"

	conf := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	ic.Close()    // intracom becomes unusable, sending late unsubscribe no longer panics
	unsubscribe() // sending late request to unsubscribe

}

func TestLateSubscriberDuringSignalCancel(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group1 := "test-subscriber1"
	group2 := "test-subscriber2"

	conf1 := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group1,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	conf2 := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group2,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	var wg sync.WaitGroup
	wg.Add(3)

	doneC := make(chan struct{})

	publishC1, unregister := ic.Register(topic)

	go func() {
		defer wg.Done()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				unregister()
				close(doneC)
				return
			case publishC1 <- true:
			}
		}
	}()

	go func() {
		defer wg.Done()

		ch1, _ := ic.Subscribe(conf1)
		// defer unsubscribe1()
		var isDone bool
		for !isDone {
			select {
			case <-doneC:
				isDone = true
			case <-ch1:
				// w/e
			}
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Second)
		ch2, _ := ic.Subscribe(conf2)

		var isDone bool
		for !isDone {
			select {
			case <-doneC:
				isDone = true
			case <-ch2:
			}
		}
	}()

	wg.Wait()

	want := false
	// consumer one should have been removed by unregister process
	_, got1 := ic.get(topic, group1)
	if want != got1 {
		t.Errorf("subscriber exists: want %v, got %v", want, got1)
	}

	want = true
	// late subscriber after unregister should be added again
	_, got2 := ic.get(topic, group2)
	if want != got2 {
		t.Errorf("subscriber exists: want %v, got %v", want, got2)
	}

}

func TestIntracomCloseWithoutUnsubscribing(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	// subscribe and we should receive an immediate message if there is a message in the last message map
	ic.Subscribe(conf)

	want := true
	_, got := ic.get(topic, group) // true if exists
	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	// intracom instance unusable, sending requests will panic
	ic.Close()

	want = false
	_, got = <-ic.requestC
	if want != got {
		t.Errorf("intracom requests channel open: want %v, got %v", want, got)
	}

}

// Testing typed instance creations
func TestNewStringTyped(t *testing.T) {

	ic := New[string]("test-intracom")

	want := reflect.TypeOf(new(Intracom[string])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewByteTyped(t *testing.T) {

	ic := New[[]byte]("test-intracom")

	want := reflect.TypeOf(new(Intracom[[]byte])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewCustomType(t *testing.T) {
	type customType struct {
		Name string
		Age  int
	}

	ic := New[customType]("test-intracom")

	want := reflect.TypeOf(new(Intracom[customType])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func countMessages[T any](num int, sub <-chan T, subCh chan int) {
	var total int
	for range sub {
		total++
	}
	subCh <- total
}

func BenchmarkIntracom(b *testing.B) {
	ic := New[string]("test-intracom")

	err := ic.Start()
	if err != nil {
		b.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "channel1"

	totalSub1 := make(chan int, 1)
	totalSub2 := make(chan int, 1)
	totalSub3 := make(chan int, 1)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		sub1, unsubscribe := ic.Subscribe(SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "sub1",
			BufferSize:    10,
			BufferPolicy:  DropNone,
		})

		defer unsubscribe()

		countMessages[string](b.N, sub1, totalSub1)
		// fmt.Println("sub1 done")
	}()

	go func() {
		defer wg.Done()
		sub2, unsubscribe := ic.Subscribe(SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "sub2",
			BufferSize:    10,
			BufferPolicy:  DropNone,
		})
		defer unsubscribe()

		countMessages[string](b.N, sub2, totalSub2)
		// fmt.Println("sub2 done")
	}()

	go func() {
		defer wg.Done()

		sub3, unsubscribe := ic.Subscribe(SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "sub3",
			BufferSize:    10,
			BufferPolicy:  DropNone,
		})
		defer unsubscribe()

		countMessages[string](b.N, sub3, totalSub3)
		// fmt.Println("sub3 done")
	}()

	// NOTE: this sleep is necessary to ensure that the subscribers receive all their messages.
	// without a publisher sleep, subscribers may not be subscribed early enough and would miss messages.
	time.Sleep(25 * time.Millisecond)
	b.ResetTimer() // reset benchmark timer once we launch the publisher

	go func() {
		defer wg.Done()

		publishCh, unregister := ic.Register(topic)
		defer unregister() // should be called only after done publishing otherwise it will panic

		for i := 0; i < b.N; i++ {
			publishCh <- "test message"
		}
	}()

	wg.Wait()

	ic.Close() // should be called last

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
