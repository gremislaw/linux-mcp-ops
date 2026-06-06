package tracker

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
	"redops/internal/models"
)

func TestRegisterDeliverUnregister(t *testing.T) {
	tr := New()
	correlationID := uuid.NewString()
	ch := tr.Register(correlationID)

	resp := kafka.AgentResponse{
		RequestID:     uuid.NewString(),
		CorrelationID: correlationID,
		Status:        models.AgentStatusSuccess,
		Result:        map[string]any{"ok": true},
	}

	if !tr.Deliver(correlationID, resp) {
		t.Fatal("expected deliver")
	}

	select {
	case got := <-ch:
		if got.Status != models.AgentStatusSuccess {
			t.Fatalf("unexpected status: %s", got.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	tr.Unregister(correlationID)
}
