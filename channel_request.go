package intracom

// channelLookupRequest represents a request to lookup a channel by its ID.
type channelLookupRequest[T any] struct {
	id        string
	responseC chan channelLookupResponse[T]
}

// channelLookupResponse represents the response to a channel lookup request.
type channelLookupResponse[T any] struct {
	ch    chan T
	found bool
}

// channelSubscribeRequest represents a request to subscribe to a channel.
type channelSubscribeRequest[T any] struct {
	id        string
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
	id        string
	responseC chan error
}

// channelUnsubscribeResponse represents the response to a channel unsubscribe request.
type channelUnsubscribeResponse[T any] struct {
	ch      chan T
	success bool
}

// channelCloseRequest represents a signal request to close a channel.
type channelCloseRequest struct{}
