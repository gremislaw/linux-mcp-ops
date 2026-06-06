package agent

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"redops/internal/kafka"
	"redops/internal/models"
)

type recordingSender struct {
	last kafka.AgentRequest
}

func (s *recordingSender) Send(_ context.Context, _ string, value any) (int32, int64, error) {
	s.last = value.(kafka.AgentRequest)
	return 0, 1, nil
}

func TestBuildRequestDefaults(t *testing.T) {
	req, err := BuildRequest(DispatchInput{
		CorrelationID: uuid.NewString(),
		Intent:        models.IntentDiagnoseAuth,
		Payload:       map[string]any{"host": "prod-01"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.Mode != models.AgentModeDryRun {
		t.Fatalf("expected dry-run, got %s", req.Mode)
	}
	if req.Intent != models.IntentDiagnoseAuth {
		t.Fatalf("unexpected intent: %s", req.Intent)
	}
}

func TestDispatcherSend(t *testing.T) {
	sender := &recordingSender{}
	d := NewDispatcher(sender, models.AgentModeDryRun)

	correlationID := uuid.NewString()
	sent, err := d.Dispatch(context.Background(), DispatchInput{
		CorrelationID: correlationID,
		Intent:        models.IntentDiagnoseAuth,
		Payload:       map[string]any{"symptom": "SSH failed"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if sender.last.CorrelationID != correlationID {
		t.Fatalf("correlation_id mismatch")
	}
	if sender.last.RequestID != sent.RequestID {
		t.Fatalf("request_id mismatch")
	}
	if sender.last.Mode != models.AgentModeDryRun {
		t.Fatalf("expected dry-run")
	}
}
