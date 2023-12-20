package intracom

type BufferPolicy int

const (
	DropNone BufferPolicy = iota
	DropOldest
	DropOldestWithTimeout
	DropNewest
	DropNewestTimeout
)

type SubscriberConfig struct {
	Topic         string
	ConsumerGroup string
	BufferSize    int
	BufferPolicy  BufferPolicy
}
