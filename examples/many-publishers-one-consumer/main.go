// Example of many publishers and one consumer.
//
// This example demonstrates one concept: Fan-In
//
// - 1 parent goroutine is launched to a given topic (events), it registers the topic and receives a publishing channel.
//   this routine will launch 5 child routines that will each block waiting to send 1 message to the publishing channel, once sent, their work is done.
//   the parent routine waits for all publishers to complete their work before unregistering the topic
//
// - 1 routine is launched to subscribe to the same topic (events), it subscribes to the topic with a consumer group name (consumer1).
//  it iterates against the subscriber channel until the publisher has signaled it is done by unregistering the topic which will close the
//  subscriber channel causing the loop to exit.

package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/ambitiousfew/intracom"
)

func main() {
	ic := intracom.New[string]()

	err := ic.Start()
	if err != nil {
		log.Fatal(err)
	}

	topic := "events"

	var wg sync.WaitGroup
	wg.Add(2)
	// Register topic, receive a publishing channel

	publishCh, unregister := ic.Register(topic)
	publisherCount := 5

	go func() {
		// start a routine that will launch 5 publishers that send 1 message each.
		// publish order is up to the go scheduler here, so we can't guarantee the order
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

	go func() {
		// launch a routuine that subscribes to the same topic
		defer wg.Done()
		// Create a consumer
		consumer, unsubscribe := ic.Subscribe(&intracom.SubscriberConfig{
			Topic:         topic,
			ConsumerGroup: "consumer1",
			BufferSize:    1,
			BufferPolicy:  intracom.DropNone,
		})

		defer unsubscribe()

		// consume messages until the publisher signals it is done by unregistering the topic.
		for msg := range consumer {
			log.Println(msg)
		}
	}()

	wg.Wait() // wait for all routines to finish
	log.Println("done")

}
