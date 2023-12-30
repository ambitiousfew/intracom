package intracom

import (
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/slog"
)

var debugLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

func TestSubscribe(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}
	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)
	defer unsubscribe()
	want := true

	// ensure the topic was initialized.
	_, got := ic.get(topic, group)
	if !got {
		t.Errorf("subscriber does not exist: want %v, got %v", want, got)
	}
}

func TestUnsubscribe(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	want := true
	_, got := ic.get(topic, group) // true if exists
	if want != got {
		t.Errorf("subscriber does not exist: want %v, got %v", want, got)
	}

	err = unsubscribe()
	if err != nil {
		t.Errorf("want nil, got %s", err)
	}
}

func TestMultipleUnSubscribes(t *testing.T) {
	ic := New[bool]("test-intracom")

	err := ic.Start()
	if err != nil {
		t.Errorf("intracom start error should be nilt: want nil, got %v", err)
	}

	defer ic.Close()

	topic := "test-topic"
	group := "test-subscriber"

	conf := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	err = unsubscribe() // nil if succeeds
	if err != nil {
		t.Errorf("want nil, got %s", err)
	}

	err = unsubscribe() // error if already unsubscribed

	if err == nil {
		t.Errorf("want nil, got %s", err)
	}
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

	conf1 := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group1,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	conf2 := &SubscriberConfig{
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

	conf := &SubscriberConfig{
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
func TestNewBoolTyped(t *testing.T) {
	ic := New[bool]("test-intracom")

	want := reflect.TypeOf(new(Intracom[bool])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewStringTyped(t *testing.T) {

	ic := New[string]("test-intracom")

	want := reflect.TypeOf(new(Intracom[string])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewIntTyped(t *testing.T) {
	ic := New[int]("test-intracom")

	want := reflect.TypeOf(new(Intracom[int])).String()
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
		sub1, unsubscribe := ic.Subscribe(&SubscriberConfig{
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
		sub2, unsubscribe := ic.Subscribe(&SubscriberConfig{
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

		sub3, unsubscribe := ic.Subscribe(&SubscriberConfig{
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
