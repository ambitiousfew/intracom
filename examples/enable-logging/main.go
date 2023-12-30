// Example of logger
//
// This example demonstrates how to enable logging on the intracom instance.
// Intracom uses slog for structured logging.

package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ambitiousfew/intracom"
	"golang.org/x/exp/slog"
)

func main() {
	myHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	logger := slog.New(myHandler)

	ic := intracom.New[string]()
	defer ic.Close()

	ic.SetLogHandler(myHandler) // must happen before .Start()

	err := ic.Start()
	if err != nil {
		log.Fatal(err)
	}

	topic := "events"

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		// start a routine that will publish to the topic.
		defer wg.Done()
		publishCh, unregister := ic.Register(topic)
		defer unregister() // unregister the topic when done (closes all subscribers to the topic)

		// publish 10 messages to the topic.
		for i := 0; i < 10; i++ {
			publishCh <- fmt.Sprintf("message %d", i)

		}
	}()

	time.Sleep(1 * time.Second) // delay subscriber start on purpose.

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
			logger.Info("consumer1 worker1 - message received: ", message)
		}
	}()

	logger.Info("waiting for metric events consumer")
	// consume from a receive-only channel that publisher pushes into.
	wg.Wait()
	logger.Info("done.")
}
