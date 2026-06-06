package router

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
)

type mockLLM struct {
	called bool
}

func (m *mockLLM) Analyze(_ context.Context, req kafka.OrchestratorRequest) (Result, error) {
	m.called = true
	return Result{
		Prompt: "mock prompt for " + req.Text,
		Plan:   map[string]any{"action": "mock"},
	}, nil
}

type recordingAgent struct {
	sent []kafka.AgentRequest
}

func (p *recordingAgent) Send(_ context.Context, _ string, value any) (int32, int64, error) {
	req := value.(kafka.AgentRequest)
	p.sent = append(p.sent, req)
	return 1, 42, nil
}

func TestRequiresAction(t *testing.T) {
	if !RequiresAction("execute") {
		t.Fatal("execute should require action")
	}
	if RequiresAction("noop") {
		t.Fatal("noop should not require action")
	}
}

func TestHandlePassiveIntent(t *testing.T) {
	llm := &mockLLM{}
	agent := &recordingAgent{}
	h := NewHandler(llm, agent)

	req := kafka.OrchestratorRequest{
		ID:            uuid.NewString(),
		CorrelationID: uuid.NewString(),
		Timestamp:     time.Now().UTC(),
		Intent:        "noop",
		Text:          "hello",
	}

	if err := h.Handle(context.Background(), req); err != nil {
		t.Fatalf("handle passive: %v", err)
	}
	if llm.called {
		t.Fatal("llm should not be called for passive intent")
	}
	if len(agent.sent) != 0 {
		t.Fatal("agent should not receive passive intents")
	}
}

func TestHandleActionIntent(t *testing.T) {
	llm := &mockLLM{}
	agent := &recordingAgent{}
	h := NewHandler(llm, agent)

	req := kafka.OrchestratorRequest{
		ID:            uuid.NewString(),
		CorrelationID: uuid.NewString(),
		Timestamp:     time.Now().UTC(),
		Intent:        "execute",
		Text:          "restart nginx",
	}

	if err := h.Handle(context.Background(), req); err != nil {
		t.Fatalf("handle action: %v", err)
	}
	if !llm.called {
		t.Fatal("llm should be called for action intent")
	}
	if len(agent.sent) != 1 {
		t.Fatalf("expected 1 agent request, got %d", len(agent.sent))
	}
	if agent.sent[0].OrchestratorRequestID != req.ID {
		t.Fatalf("orchestrator_request_id mismatch: %s", agent.sent[0].OrchestratorRequestID)
	}
}
