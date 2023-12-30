package intracom

// closeRequest represents a request to close the requests broker.
type closeRequest struct {
	responseC chan struct{}
}

// intracomLookupRequest represents a request to lookup a value of type T in a topic.
type lookupRequest[T any] struct {
	topic     string
	consumer  string
	responseC chan lookupResponse[T]
}

// intracomLookupResponse represents the response to a lookup request.
type lookupResponse[T any] struct {
	ch    chan T
	found bool
}

// intracomSubscribeRequest represents a request to subscribe to a topic and receive values of type T.
type subscribeRequest[T any] struct {
	conf      SubscriberConfig
	responseC chan subscribeResponse[T]
}

// intracomSubscribeResponse represents the response to a subscribe request.
type subscribeResponse[T any] struct {
	ch      chan T
	created bool
}

// intracomUnsubscribeRequest represents a request to unsubscribe from a topic.
type unsubscribeRequest[T any] struct {
	topic     string
	consumer  string
	responseC chan struct{}
}

// registerRequest represents a request to register a topic with the requests broker.
type registerRequest[T any] struct {
	topic     string
	responseC chan chan T
}

// intracomUnregisterRequest represents a request to unregister a topic.
type unregisterRequest struct {
	topic     string
	responseC chan struct{}
}
