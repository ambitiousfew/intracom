package intracom

import (
	"context"
	"fmt"
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

	conf := &ConsumerConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)
	defer unsubscribe()

	want := true

	// ensure the topic was initialized.
	channel, exists := ic.get(topic)
	if !exists {
		t.Errorf("topic channel does not exist: want %v, got %v", want, exists)
	}
	// ensure the consumer group was created.
	_, got := channel.get(group)

	fmt.Println("---------------")
	if got != want {
		t.Errorf("want %v, got %v", want, got)
	}

}

func TestUnsubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

	topic := "test-topic"
	group := "test-subscriber"

	conf := &ConsumerConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	want := true
	// ensure the topic still exists
	channel, exists := ic.get(topic)
	if !exists {
		t.Errorf("topic channel does not exist: want %v, got %v", want, exists)
	}

	_, got := channel.get(group)
	if want != got {
		t.Errorf("consumer group does not exist: want %v, got %v", want, got)
	}

	unsubscribe()
	// TODO: How is it still finding it?
	want = false
	// ensure the consumer group no longer exists.
	if _, got := channel.get(group); want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestMultipleUnSubscribes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ic := New[bool](ctx)

	topic := "test-topic"
	group := "test-subscriber"

	conf := &ConsumerConfig{
		Topic:         topic,
		ConsumerGroup: group,
		BufferSize:    1,
		BufferPolicy:  DropNone,
	}

	_, unsubscribe := ic.Subscribe(conf)

	want := true
	got := unsubscribe() // true when exists
	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	want = false
	got = unsubscribe() // false when does not exist

	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

// TODO: test the intracom closer
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
	runtime.GOMAXPROCS(1)
	// runtime.SetMutexProfileFraction(3)
	ctx, cancel := context.WithCancel(context.Background())
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ic := New[string](ctx)

	topic := "channel1"

	totalSub1 := make(chan int, 1)
	totalSub2 := make(chan int, 1)
	totalSub3 := make(chan int, 1)

	var wg sync.WaitGroup
	wg.Add(4)

	publishCh, unregister := ic.Register(topic)

	go func() {
		defer wg.Done()
		sub1, _ := ic.Subscribe(&ConsumerConfig{
			Topic:         topic,
			ConsumerGroup: "sub1",
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})

		countMessages[string](ctx, b.N, sub1, totalSub1)
		// unsubscribe()
	}()

	go func() {
		defer wg.Done()
		sub2, _ := ic.Subscribe(&ConsumerConfig{
			Topic:         topic,
			ConsumerGroup: "sub2",
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})

		countMessages[string](ctx, b.N, sub2, totalSub2)
	}()

	go func() {
		defer wg.Done()

		sub3, _ := ic.Subscribe(&ConsumerConfig{
			Topic:         topic,
			ConsumerGroup: "sub3",
			BufferSize:    1,
			BufferPolicy:  DropNone,
		})

		countMessages[string](ctx, b.N, sub3, totalSub3)
	}()

	// TODO: without a delay here we end up in a deadlock. Why?
	time.Sleep(50 * time.Millisecond)
	b.ResetTimer()

	go func() {
		defer wg.Done()
		defer unregister()
		for i := 0; i < b.N; i++ {
			publishCh <- "test message"
		}
	}()

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
