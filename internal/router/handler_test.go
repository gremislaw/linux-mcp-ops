package router

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"redops/internal/agent"
	"redops/internal/kafka"
	"redops/internal/llm"
	"redops/internal/models"
	"redops/internal/tracker"
)

type mockLLM struct{}

func (mockLLM) Analyze(_ context.Context, _ string) (Result, error) {
	return Result{
		ToolCalls: []llm.ToolCall{{
			Name:      models.IntentDiagnoseAuth,
			Arguments: map[string]any{"host": "prod-01"},
		}},
	}, nil
}

type recordingAgent struct {
	sent []kafka.AgentRequest
}

func (p *recordingAgent) Send(_ context.Context, _ string, value any) (int32, int64, error) {
	p.sent = append(p.sent, value.(kafka.AgentRequest))
	return 1, 42, nil
}

type recordingBot struct {
	sent []kafka.OrchestratorResponse
}

func (p *recordingBot) Send(_ context.Context, _ string, value any) (int32, int64, error) {
	p.sent = append(p.sent, value.(kafka.OrchestratorResponse))
	return 2, 99, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestHandleWithAgentResponse(t *testing.T) {
	tr := tracker.New()
	agentSender := &recordingAgent{}
	bot := &recordingBot{}

	h := NewHandler(HandlerConfig{
		LLM:         mockLLM{},
		Agent:       agent.NewDispatcher(agentSender, models.AgentModeDryRun),
		Bot:         bot,
		Tracker:     tr,
		WaitTimeout: 2 * time.Second,
		Logger:      testLogger(),
	})

	req := kafka.OrchestratorRequest{
		RequestID:     uuid.NewString(),
		CorrelationID: uuid.NewString(),
		ChatID:        1001,
		UserText:      "не заходит по SSH",
		Timestamp:     time.Now().UTC(),
	}

	done := make(chan error, 1)
	go func() { done <- h.Handle(context.Background(), req) }()

	time.Sleep(50 * time.Millisecond)
	agentReqID := agentSender.sent[0].RequestID
	tr.Deliver(req.CorrelationID, kafka.AgentResponse{
		RequestID:     agentReqID,
		CorrelationID: req.CorrelationID,
		Status:        models.AgentStatusSuccess,
		Result:        map[string]any{"message": "SSH ключ добавлен"},
	})

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(bot.sent) != 1 {
		t.Fatalf("expected bot response, got %d", len(bot.sent))
	}
	if bot.sent[0].Text == "" {
		t.Fatal("expected text")
	}
	if strings.Contains(bot.sent[0].Text, "map[") {
		t.Fatalf("go dump in text: %q", bot.sent[0].Text)
	}
}

func TestHandleTimeout(t *testing.T) {
	tr := tracker.New()
	bot := &recordingBot{}

	h := NewHandler(HandlerConfig{
		LLM:         mockLLM{},
		Agent:       agent.NewDispatcher(&recordingAgent{}, models.AgentModeDryRun),
		Bot:         bot,
		Tracker:     tr,
		WaitTimeout: 100 * time.Millisecond,
		Logger:      testLogger(),
	})

	req := kafka.OrchestratorRequest{
		RequestID:     uuid.NewString(),
		CorrelationID: uuid.NewString(),
		ChatID:        1001,
		UserText:      "restart nginx",
		Timestamp:     time.Now().UTC(),
	}

	if err := h.Handle(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if bot.sent[0].Text != models.MsgAgentTimeout {
		t.Fatalf("unexpected text: %q", bot.sent[0].Text)
	}
}

func TestHandleLLMFailure(t *testing.T) {
	bot := &recordingBot{}
	h := NewHandler(HandlerConfig{
		LLM:    failingLLM{},
		Agent:  agent.NewDispatcher(&recordingAgent{}, models.AgentModeDryRun),
		Bot:    bot,
		Logger: testLogger(),
	})

	req := kafka.OrchestratorRequest{
		RequestID:     uuid.NewString(),
		CorrelationID: uuid.NewString(),
		ChatID:        1001,
		UserText:      "привет",
		Timestamp:     time.Now().UTC(),
	}

	if err := h.Handle(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if bot.sent[0].Text != models.MsgUnrecognizedRequest {
		t.Fatalf("unexpected text: %q", bot.sent[0].Text)
	}
}

type failingLLM struct{}

func (failingLLM) Analyze(context.Context, string) (Result, error) {
	return Result{}, nil
}
