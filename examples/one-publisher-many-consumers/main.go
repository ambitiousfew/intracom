package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ambitiousfew/intracom"
)

func startPublisher(publishC chan<- string) {
	// time.Sleep(2 * time.Second)
	// publish a message 10 times.
	for i := 0; i < 10; i++ {
		publishC <- fmt.Sprintf("message %d", i)
	}
}

func main() {
	// a parent context that can be cancelled to end all underlying channels inside Intracom
	// cleanup performed, Intracom instance is no longer usable after cancel
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ic := intracom.New[string](ctx)

	topic := "events"

	// use .Register to register a topic you wish to publish to.
	// you will receive a send-only channel for your producers
	// and a bound-callable function that when called will unregister this topic.
	publishCh, unregister := ic.Register(topic)

	// For this example we are launching 3 routines
	// 2 routines that will subscribe to the same topic with different consumer names.
	// 1 routine that will publish to the same topic.
	// Publishes will be "broadcasted" to all subscribers of the same topic.
	// Late subscribers will only receive messages from the time at which they connect.
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { // consumer1 subscribing using intracom instance.
		defer wg.Done()
		// consumer 1
		ch, _ := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer1",
			BufferSize:    10,
			BufferPolicy:  intracom.DropNone,
		})

		for message := range ch {
			fmt.Println("consumer1 message received: ", message)
		}
	}()

	go func() { // consumer2 subscribing using intracom instance.
		defer wg.Done()
		// consumer 2
		ch, _ := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer2",
			BufferSize:    10,
			BufferPolicy:  intracom.DropNone,
		})

		for message := range ch {
			fmt.Println("consumer2 message received: ", message)
		}
	}()

	go func() { // publisher of 10 messages, 0 through 9
		defer wg.Done()
		defer unregister() // nothing more will be produced, unregister topic.
		startPublisher(publishCh)
	}()

	fmt.Println("waiting for metric events consumer")
	// consume from a receive-only channel that publisher pushes into.
	wg.Wait()
	fmt.Println("done.")
}
