package intracom

type publishRequest[T any] struct {
	consumerID string
	message    T
}

type addRequest[T any] struct {
	consumerID string
	ch         chan T
}

type subscriberRequest[T any] struct {
	consumerID string
	ch         chan subscriberResponse[T]
}

type subscriberResponse[T any] struct {
	ch          chan T
	found       bool
	lastMessage *T
}

type removeRequest struct {
	consumerID string
}
