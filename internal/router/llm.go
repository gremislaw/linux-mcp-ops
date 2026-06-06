package router

import (
	"context"
	"fmt"

	"redops/internal/kafka"
)

type Result struct {
	Prompt string
	Plan   map[string]any
}

type Client interface {
	Analyze(ctx context.Context, req kafka.OrchestratorRequest) (Result, error)
}

// StubClient — заглушка LLM для MVP; заменяется реальным клиентом на следующем этапе.
type StubClient struct{}

func (StubClient) Analyze(_ context.Context, req kafka.OrchestratorRequest) (Result, error) {
	return Result{
		Prompt: fmt.Sprintf("Process user request with intent=%s: %s", req.Intent, req.Text),
		Plan: map[string]any{
			"steps": []string{
				"parse_intent",
				"select_tools",
				"execute",
			},
			"intent": req.Intent,
			"source": "stub-llm",
		},
	}, nil
}
