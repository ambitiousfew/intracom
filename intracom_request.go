package intracom

// intracomLookupRequest represents a request to lookup a value of type T in a topic.
type intracomLookupRequest[T any] struct {
	topic     string
	consumer  string
	responseC chan intracomLookupResponse[T]
}

// intracomLookupResponse represents the response to a lookup request.
type intracomLookupResponse[T any] struct {
	ch    chan T
	found bool
}

// intracomSubscribeRequest represents a request to subscribe to a topic and receive values of type T.
type intracomSubscribeRequest[T any] struct {
	conf      *ConsumerConfig
	topic     string
	consumer  string
	ch        chan T
	responseC chan intracomSubscribeResponse[T]
}

// intracomSubscribeResponse represents the response to a subscribe request.
type intracomSubscribeResponse[T any] struct {
	ch      chan T
	success bool
}

// intracomUnsubscribeRequest represents a request to unsubscribe from a topic.
type intracomUnsubscribeRequest[T any] struct {
	topic     string
	consumer  string
	responseC chan bool
}

// intracomRegisterRequest represents a request to register a topic with a publish channel of type T.
type intracomRegisterRequest[T any] struct {
	topicID   string
	responseC chan intracomRegisterResponse[T]
}

// intracomRegisterResponse represents the response to a register request.
type intracomRegisterResponse[T any] struct {
	publishC chan T
	found    bool
}

// intracomUnregisterRequest represents a request to unregister a topic.
type intracomUnregisterRequest struct {
	topicID   string
	responseC chan bool
}
