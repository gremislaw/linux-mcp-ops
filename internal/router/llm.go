package router

import (
	"context"
	"fmt"

	"redops/internal/kafka"
	"redops/internal/llm"
)

type Result struct {
	ToolCalls []llm.ToolCall
}

type Client interface {
	Analyze(ctx context.Context, req kafka.OrchestratorRequest) (Result, error)
}

// StubClient — заглушка LLM с tool call для тестов без Ollama.
type StubClient struct{}

func (StubClient) Analyze(_ context.Context, req kafka.OrchestratorRequest) (Result, error) {
	return Result{
		ToolCalls: []llm.ToolCall{
			{
				Name: "diagnose_auth",
				Arguments: map[string]any{
					"host":    "prod-01",
					"symptom": req.Text,
				},
			},
		},
	}, nil
}

// OllamaClient вызывает локальный/K8s Ollama и возвращает tool_calls.
type OllamaClient struct{}

func (OllamaClient) Analyze(ctx context.Context, req kafka.OrchestratorRequest) (Result, error) {
	resp, err := llm.CallOllama(ctx, req.Text)
	if err != nil {
		return Result{}, fmt.Errorf("ollama: %w", err)
	}
	if len(resp.ToolCalls) == 0 {
		return Result{}, fmt.Errorf("ollama returned no tool_calls")
	}
	return Result{ToolCalls: resp.ToolCalls}, nil
}
