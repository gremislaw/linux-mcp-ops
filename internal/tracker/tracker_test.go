package tracker

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
)

func TestRegisterDeliverUnregister(t *testing.T) {
	tr := New()
	correlationID := uuid.NewString()

	ch := tr.Register(correlationID)

	resp := kafka.AgentResponse{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Status:        "success",
		Timestamp:     time.Now().UTC(),
		Payload:       map[string]any{"ok": true},
	}

	if !tr.Deliver(correlationID, resp) {
		t.Fatal("expected deliver to succeed")
	}

	select {
	case got := <-ch:
		if got.Status != "success" {
			t.Fatalf("unexpected status: %s", got.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for response")
	}

	tr.Unregister(correlationID)
	if tr.Pending() != 0 {
		t.Fatalf("expected 0 pending, got %d", tr.Pending())
	}
}

func TestDeliverUnknownCorrelationID(t *testing.T) {
	tr := New()
	ok := tr.Deliver(uuid.NewString(), kafka.AgentResponse{Status: "success"})
	if ok {
		t.Fatal("expected deliver to unknown id to fail")
	}
}
