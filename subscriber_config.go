package intracom

import "time"

type BufferPolicy int

const (
	DropNone BufferPolicy = iota
	DropOldest
	DropOldestAfterTimeout
	DropNewest
	DropNewestAfterTimeout
)

type SubscriberConfig struct {
	Topic         string
	ConsumerGroup string
	BufferSize    int
	BufferPolicy  BufferPolicy
	DropTimeout   time.Duration
}
