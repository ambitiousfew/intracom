package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ambitiousfew/intracom"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ic := intracom.New[string](ctx)

	topic := "metric-events"

	var wg sync.WaitGroup
	wg.Add(2)
	// Register topic, receive a publishing channel

	publishCh, unregister := ic.Register(topic)
	publisherCount := 5

	go func() {
		defer wg.Done()
		defer unregister()

		// spawn routine that will launch 5 publishers
		var pwg sync.WaitGroup
		pwg.Add(publisherCount)
		for i := 0; i < publisherCount; i++ { // spawn 5 publishers
			i := i
			go func() {
				defer pwg.Done()
				publishCh <- fmt.Sprintf("Hello from publisher %d", i+1)
			}()
		}
		pwg.Wait()

	}()

	// launch consumer that prints messages
	go func() {
		defer wg.Done()
		// Create a consumer
		consumer, unsubscribe := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer1",
			BufferSize:    100,
			BufferPolicy:  intracom.DropNone,
		})

		defer unsubscribe()

		for msg := range consumer {
			fmt.Println(msg)
		}
	}()

	// wait for all routines to finish
	wg.Wait()
	fmt.Println("done")
}
