package router

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/agent"
	"redops/internal/kafka"
	"redops/internal/llm"
	"redops/internal/tracker"
)

type mockLLM struct {
	called bool
}

func (m *mockLLM) Analyze(_ context.Context, req kafka.OrchestratorRequest) (Result, error) {
	m.called = true
	return Result{
		ToolCalls: []llm.ToolCall{
			{
				Name:      "diagnose_auth",
				Arguments: map[string]any{"host": "prod-01", "symptom": req.Text},
			},
		},
	}, nil
}

type recordingSender struct {
	sent []kafka.AgentRequest
}

func (p *recordingSender) Send(_ context.Context, _ string, value any) (int32, int64, error) {
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
	sender := &recordingSender{}
	h := NewHandler(HandlerConfig{
		LLM:   &mockLLM{},
		Agent: agent.NewDispatcher(sender, agent.DefaultMode),
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
	sender := &recordingSender{}
	orch := &recordingOrchestrator{}

	h := NewHandler(HandlerConfig{
		LLM:          &mockLLM{},
		Agent:        agent.NewDispatcher(sender, agent.DefaultMode),
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
	if len(sender.sent) != 1 {
		t.Fatalf("expected agent request, got %d", len(sender.sent))
	}
	if sender.sent[0].Mode != "dry-run" {
		t.Fatalf("expected dry-run mode, got %s", sender.sent[0].Mode)
	}
	if sender.sent[0].CorrelationID != req.CorrelationID {
		t.Fatalf("correlation_id mismatch")
	}
	if sender.sent[0].RequestID != req.ID {
		t.Fatalf("request_id mismatch")
	}

	agentReqID := sender.sent[0].ID
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
		Agent:        agent.NewDispatcher(&recordingSender{}, agent.DefaultMode),
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
