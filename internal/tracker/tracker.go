package tracker

import (
	"sync"

	"redops/internal/kafka"
)

// RequestTracker хранит ожидающие ответы агента по correlation_id.
type RequestTracker struct {
	mu    sync.Mutex
	waits map[string]chan kafka.AgentResponse
}

func New() *RequestTracker {
	return &RequestTracker{
		waits: make(map[string]chan kafka.AgentResponse),
	}
}

func (t *RequestTracker) Register(correlationID string) <-chan kafka.AgentResponse {
	ch := make(chan kafka.AgentResponse, 1)

	t.mu.Lock()
	t.waits[correlationID] = ch
	t.mu.Unlock()

	return ch
}

func (t *RequestTracker) Deliver(correlationID string, resp kafka.AgentResponse) bool {
	t.mu.Lock()
	ch, ok := t.waits[correlationID]
	t.mu.Unlock()
	if !ok {
		return false
	}

	select {
	case ch <- resp:
		return true
	default:
		return false
	}
}

func (t *RequestTracker) Unregister(correlationID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch, ok := t.waits[correlationID]
	if !ok {
		return
	}
	delete(t.waits, correlationID)
	close(ch)
}

func (t *RequestTracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.waits)
}
