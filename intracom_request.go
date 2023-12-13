package intracom

// registerRequest for the creation of a channel topic
type registerRequest[T any] struct {
	topicID string
	channel *channel[T]
}

// channelRequest for the requested retrieval of a channel
type channelRequest[T any] struct {
	topicID string
	ch      chan channelResponse[T]
}

// channelResponse for the retrieval response of a channel
type channelResponse[T any] struct {
	channel *channel[T]
	found   bool
}

// unregisterRequest for the removal of a channel topic
type unregisterRequest struct {
	topicID string
}
