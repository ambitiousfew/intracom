package intracom

import "testing"

func TestNewSubscriberDefaults(t *testing.T) {
	topic := "test"
	consumerGroup := "test"
	bufferSize := 1

	s := newSubscriber[int](SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		BufferSize:    bufferSize,
		BufferPolicy:  DropNone,
	})

	if s.topic != topic {
		t.Errorf("topic mismatch: expected %s, got %s", topic, s.topic)
	}

	if s.consumerGroup != consumerGroup {
		t.Errorf("consumerGroup mismatch: expected %s, got %s", consumerGroup, s.consumerGroup)
	}

	if s.bufferSize != bufferSize {
		t.Errorf("bufferSize mismatch: expected %d, got %d", bufferSize, s.bufferSize)
	}

	if s.bufferPolicy != DropNone {
		t.Errorf("bufferPolicy mismatch: expected %d, got %d", DropNone, s.bufferPolicy)
	}

	if s.dropTimeout != 0 {
		t.Errorf("dropTimeout mismatch: expected %d, got %d", 0, s.dropTimeout)
	}

	if s.timer != nil {
		t.Errorf("timer mismatch: expected nil, got %v", s.timer)
	}

	if s.ch == nil {
		t.Errorf("ch mismatch: expected not nil, got nil")
	}

	if s.stopC == nil {
		t.Errorf("stopC mismatch: expected not nil, got nil")
	}

	if s.closed {
		t.Errorf("closed mismatch: expected false, got true")
	}

}

func TestSubscriberSend(t *testing.T) {
	messagesToSend := 3

	topic := "test"
	consumerGroup := "test"
	bufferSize := messagesToSend

	s := newSubscriber[int](SubscriberConfig{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		BufferSize:    bufferSize,
		BufferPolicy:  DropNone,
	})

	doneC := make(chan struct{})
	go func() {
		for i := 0; i < messagesToSend; i++ {
			s.send(i)
		}
		s.close()
		close(doneC)
	}()

	var total int
	for range s.ch {
		total++
	}

	<-doneC

	if total != messagesToSend {
		t.Errorf("total mismatch: expected %d, got %d", messagesToSend, total)
	}
}
