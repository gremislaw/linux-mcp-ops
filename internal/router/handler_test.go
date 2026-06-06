package router

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
	"redops/internal/tracker"
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

type recordingOrchestrator struct {
	sent []kafka.OrchestratorResponse
}

func (p *recordingOrchestrator) Send(_ context.Context, _ string, value any) (int32, int64, error) {
	resp := value.(kafka.OrchestratorResponse)
	p.sent = append(p.sent, resp)
	return 2, 99, nil
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
	h := NewHandler(HandlerConfig{
		LLM:   &mockLLM{},
		Agent: &recordingAgent{},
	})

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
}

func TestHandleActionIntentWithAgentResponse(t *testing.T) {
	tr := tracker.New()
	agent := &recordingAgent{}
	orch := &recordingOrchestrator{}

	h := NewHandler(HandlerConfig{
		LLM:          &mockLLM{},
		Agent:        agent,
		Orchestrator: orch,
		Tracker:      tr,
		WaitTimeout:  2 * time.Second,
	})

	req := kafka.OrchestratorRequest{
		ID:            uuid.NewString(),
		CorrelationID: uuid.NewString(),
		Timestamp:     time.Now().UTC(),
		Intent:        "execute",
		Text:          "restart nginx",
		ChatID:        1001,
	}

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(context.Background(), req)
	}()

	time.Sleep(50 * time.Millisecond)
	if len(agent.sent) != 1 {
		t.Fatalf("expected agent request, got %d", len(agent.sent))
	}

	agentReqID := agent.sent[0].ID
	tr.Deliver(req.CorrelationID, kafka.AgentResponse{
		ID:             uuid.NewString(),
		CorrelationID:  req.CorrelationID,
		AgentRequestID: agentReqID,
		Timestamp:      time.Now().UTC(),
		Status:         "success",
		Payload:        map[string]any{"result": "nginx restarted"},
	})

	if err := <-done; err != nil {
		t.Fatalf("handle action: %v", err)
	}
	if len(orch.sent) != 1 {
		t.Fatalf("expected orchestrator response, got %d", len(orch.sent))
	}
	if orch.sent[0].Status != "success" {
		t.Fatalf("unexpected status: %s", orch.sent[0].Status)
	}
}

func TestHandleActionIntentTimeout(t *testing.T) {
	tr := tracker.New()
	orch := &recordingOrchestrator{}

	h := NewHandler(HandlerConfig{
		LLM:          &mockLLM{},
		Agent:        &recordingAgent{},
		Orchestrator: orch,
		Tracker:      tr,
		WaitTimeout:  100 * time.Millisecond,
	})

	req := kafka.OrchestratorRequest{
		ID:            uuid.NewString(),
		CorrelationID: uuid.NewString(),
		Timestamp:     time.Now().UTC(),
		Intent:        "execute",
		Text:          "restart nginx",
		ChatID:        1001,
	}

	if err := h.Handle(context.Background(), req); err != nil {
		t.Fatalf("handle timeout: %v", err)
	}
	if len(orch.sent) != 1 {
		t.Fatalf("expected timeout response, got %d", len(orch.sent))
	}
	if orch.sent[0].Status != "timeout" {
		t.Fatalf("unexpected status: %s", orch.sent[0].Status)
	}
}
