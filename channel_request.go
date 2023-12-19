package intracom

// channelLookupRequest represents a request to lookup a channel by its consumer.
type channelLookupRequest[T any] struct {
	consumer  string
	responseC chan channelLookupResponse[T]
}

// channelLookupResponse represents the response to a channel lookup request.
type channelLookupResponse[T any] struct {
	ch    chan T
	found bool
}

// channelSubscribeRequest represents a request to subscribe to a channel.
type channelSubscribeRequest[T any] struct {
	conf      ConsumerConfig
	ch        chan T
	responseC chan channelSubscribeResponse[T]
}

// channelSubscribeResponse represents the response to a channel subscribe request.
type channelSubscribeResponse[T any] struct {
	ch     chan T
	exists bool
}

// channelUnsubscribeRequest represents a request to unsubscribe from a channel.
type channelUnsubscribeRequest[T any] struct {
	consumer  string
	responseC chan bool
}

// channelUnsubscribeResponse represents the response to a channel unsubscribe request.
type channelUnsubscribeResponse[T any] struct {
	ch      chan T
	success bool
}

// channelCloseRequest represents a signal request to close a channel.
type channelCloseRequest struct {
	responseC chan struct{}
}
