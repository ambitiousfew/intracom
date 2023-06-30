package intracom

import (
	"testing"
)

func TestnewManager(t *testing.T) {
	manager := newManager[string]()

	if manager == nil {
		t.Errorf("manager should not be nil")
	}

	if len(manager.channels) != 0 {
		t.Errorf("expected empty consumers map, got %d", len(manager.channels))
	}

	if manager.mu == nil {
		t.Errorf("expected mutex to be non-nil, got %v", manager.mu)
	}
}

func TestIntraManagerGetNonExisting(t *testing.T) {
	manager := newManager[string]()

	id := "test-topic"

	want := false

	_, got := manager.get(id)

	if got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestIntraManagerGetExisting(t *testing.T) {
	manager := newManager[string]()

	// TODO:
	id := "test-topic"

	want := true

	_, got := manager.get(id)

	if got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestIntraManagerAdd(t *testing.T) {
	manager := newManager[string]()
	channel := newChannel[string]()

	topic := "test-topic"

	manager.add(topic, channel)

	want := true

	_, got := manager.get(topic)

	if !got {
		t.Errorf("want %v, got %v", want, got)
	}

	size := manager.len()
	if manager.len() != 1 {
		t.Errorf("want %d, got %d", 1, size)
	}
}

func TestIntraManagerCloseWhileNotEmpty(t *testing.T) {
	manager := newManager[string]()
	channel := newChannel[string]()

	topic := "test-topic"
	manager.add(topic, channel)

	beforeLength := len(manager.channels)
	if beforeLength != 1 {
		t.Errorf("want %d, got %d", 1, beforeLength)
	}

	manager.close()

	afterLength := len(manager.channels)
	if afterLength != 0 {
		t.Errorf("want %d, got %d", 0, afterLength)
	}
}

func TestIntraManagerCloseWhileEmpty(t *testing.T) {
	manager := newManager[string]()

	want := 0

	beforeLength := len(manager.channels)
	if beforeLength != 0 {
		t.Errorf("want %d, got %d", want, beforeLength)
	}

	manager.close()

	afterLength := len(manager.channels)
	if afterLength != 0 {
		t.Errorf("want %d, got %d", want, afterLength)
	}
}
