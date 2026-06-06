package agent

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
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
		RequestID:     uuid.NewString(),
		CorrelationID: uuid.NewString(),
		Tool:          "diagnose_auth",
		Arguments:     map[string]any{"host": "prod-01"},
	}, DefaultMode)
	if err != nil {
		t.Fatal(err)
	}
	if req.Mode != "dry-run" {
		t.Fatalf("expected dry-run, got %s", req.Mode)
	}
	if req.Tool != "diagnose_auth" {
		t.Fatalf("unexpected tool: %s", req.Tool)
	}
	if req.Arguments["host"] != "prod-01" {
		t.Fatalf("unexpected arguments: %#v", req.Arguments)
	}
}

func TestDispatcherSend(t *testing.T) {
	sender := &recordingSender{}
	d := NewDispatcher(sender, DefaultMode)

	requestID := uuid.NewString()
	correlationID := uuid.NewString()

	sent, err := d.Dispatch(context.Background(), DispatchInput{
		RequestID:     requestID,
		CorrelationID: correlationID,
		Tool:          "diagnose_auth",
		Arguments:     map[string]any{"symptom": "SSH failed"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if sender.last.CorrelationID != correlationID {
		t.Fatalf("correlation_id mismatch: %s", sender.last.CorrelationID)
	}
	if sender.last.RequestID != requestID {
		t.Fatalf("request_id mismatch: %s", sender.last.RequestID)
	}
	if sent.ID != sender.last.ID {
		t.Fatalf("returned request id mismatch")
	}
	_ = time.Now()
}
