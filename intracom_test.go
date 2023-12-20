package intracom

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

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

	err := unsubscribe()
	if err != nil {
		t.Errorf("want nil, got %s", err)
	}
}

func TestMultipleUnSubscribes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

	topic := "test-topic"
	group := "test-subscriber"

	conf := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	err := unsubscribe() // nil if succeeds
	if err != nil {
		t.Errorf("want nil, got %s", err)
	}

	err = unsubscribe() // error if already unsubscribed

	if err == nil {
		t.Errorf("want nil, got %s", err)
	}
}

func TestIntracomClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

	topic := "test-topic"
	group := "test-subscriber"

	conf := &SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	// subscribe and we should receive an immediate message if there is a message in the last message map
	_, unsubscribe := ic.Subscribe(conf)
	defer unsubscribe()

	ic.Close()

	want := true
	got := ic.closed.Load()
	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

// Testing typed instance creations
func TestNewBoolTyped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

	want := reflect.TypeOf(new(Intracom[bool])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewStringTyped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ic := New[string](ctx)

	want := reflect.TypeOf(new(Intracom[string])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewIntTyped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ic := New[int](ctx)

	want := reflect.TypeOf(new(Intracom[int])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func TestNewByteTyped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[[]byte](ctx)

	want := reflect.TypeOf(new(Intracom[[]byte])).String()
	got := reflect.TypeOf(ic).String()

	if want != got {
		t.Errorf("want %s: got %s", want, got)
	}

}

func countMessages[T any](ctx context.Context, num int, sub <-chan T, subCh chan int) {
	var total int
	for range sub {
		select {
		case <-ctx.Done():
			subCh <- total
			return
		default:
			total++
		}
	}
	subCh <- total
}

func BenchmarkIntracom(b *testing.B) {
	runtime.GOMAXPROCS(1) // force single core
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	ic := New[string](ctx)

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
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})

		defer unsubscribe()

		countMessages[string](ctx, b.N, sub1, totalSub1)
		// fmt.Println("sub1 done")
	}()

	go func() {
		defer wg.Done()
		sub2, unsubscribe := ic.Subscribe(&SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "sub2",
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})
		defer unsubscribe()

		countMessages[string](ctx, b.N, sub2, totalSub2)
		// fmt.Println("sub2 done")
	}()

	go func() {
		defer wg.Done()

		sub3, unsubscribe := ic.Subscribe(&SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "sub3",
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})
		defer unsubscribe()

		countMessages[string](ctx, b.N, sub3, totalSub3)
		// fmt.Println("sub3 done")
	}()

	// NOTE: this sleep is necessary to ensure that the subscribers receive all their messages.
	// without a publisher sleep, subscribers may not be subscribed early enough and would miss messages.
	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()

		publishCh, unregister := ic.Register(topic)
		defer unregister() // should be called only after done publishing otherwise it will panic
		for i := 0; i < b.N; i++ {
			publishCh <- "test message"
		}
	}()

	b.ResetTimer() // reset benchmark timer once we launch the publisher

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
