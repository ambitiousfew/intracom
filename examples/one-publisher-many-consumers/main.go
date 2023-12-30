// Example of one publisher and many consumers.
//
// This example demonstrates two concepts at the same time: Fan-Out and Broadcasting
//
// - Fan Out - 2 goroutines are subscribed to the same topic (events) using the same consumer name (consumer1).
//  this makes them part of the same consumer group, effectively creating a worker pool against the channel.
//  this allows you to "fan-out" the messages to multiple workers without repeating the same message to them all.
//
// - Broadcasting - 1 goroutine is subscribed to the same topic (events) with a different consumer name (consumer2).
//  this makes it part of a different consumer group of the same topic, because of this it will receive the same
//  messages as the other goroutines, but it will not share the workload with them.
//  this allows you to send the same message to a separate consumer to process differently than the worker pool.

package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ambitiousfew/intracom"
)

func main() {
	ic := intracom.New[string]("one-to-many-example")

	err := ic.Start()
	if err != nil {
		log.Fatal(err)
	}

	topic := "events"

	// register topic, receive a publishing channel and unregister function bound to the topic registered.
	publishCh, unregister := ic.Register(topic)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		// subscribe to the topic with consumer1 name - worker 1
		ch, _ := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer1",
			BufferSize:    1,
			BufferPolicy:  intracom.DropNone,
		})

		// consume from a receive-only channel that publisher broadcasts into.
		for message := range ch {
			log.Println("consumer1 worker1 - message received: ", message)
		}
	}()

	go func() {
		// subscribe to the same topic with consumer1 - worker 2
		defer wg.Done()

		ch, _ := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer1",
			BufferSize:    1,
			BufferPolicy:  intracom.DropNone,
		})

		// because this routine subscribed using the same consumer group (consumer1)
		// same as the previous routine, both routines share the workload.
		//
		// This is useful if you have a fast publisher or many publishers and you want to consume faster
		for message := range ch {
			log.Println("consumer1 worker2 - message received: ", message)
		}
	}()

	go func() {
		// subscribe to the same topic with consumer2
		defer wg.Done()

		ch, _ := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer2",
			BufferSize:    1,
			BufferPolicy:  intracom.DropNone,
		})

		// consume from a receive-only channel that publisher broadcasts into.
		// beacause this consumer has a different consumer group, it will receive the same messages as consumer1.
		for message := range ch {
			log.Println("consumer2 worker  - message received: ", message)
		}
	}()

	go func() {
		// start a routine that will publish to the topic.
		defer wg.Done()
		defer unregister() // unregister the topic when done (closes all subscribers to the topic)

		// publish 10 messages to the topic.
		for i := 0; i < 10; i++ {
			publishCh <- fmt.Sprintf("message %d", i)
			time.Sleep(250 * time.Millisecond)
		}
	}()

	log.Println("waiting for metric events consumer")
	// consume from a receive-only channel that publisher pushes into.
	wg.Wait()
	log.Println("done.")
}
