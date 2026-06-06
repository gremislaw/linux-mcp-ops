package router

import (
	"context"
	"fmt"

	"redops/internal/llm"
)

type Result struct {
	ToolCalls []llm.ToolCall
}

type Client interface {
	Analyze(ctx context.Context, userText string) (Result, error)
}

type StubClient struct{}

func (StubClient) Analyze(_ context.Context, userText string) (Result, error) {
	return Result{
		ToolCalls: []llm.ToolCall{
			{
				Name: "diagnose_auth",
				Arguments: map[string]any{
					"host":    "prod-01",
					"symptom": userText,
				},
			},
		},
	}, nil
}

type OllamaClient struct {
	Cfg llm.Config
}

func NewOllamaClient(cfg llm.Config) OllamaClient {
	return OllamaClient{Cfg: cfg}
}

func (c OllamaClient) Analyze(ctx context.Context, userText string) (Result, error) {
	resp, err := llm.CallOllama(ctx, c.Cfg, userText)
	if err != nil {
		return Result{}, fmt.Errorf("ollama: %w", err)
	}
	if len(resp.ToolCalls) == 0 {
		return Result{}, fmt.Errorf("no tool_calls")
	}
	return Result{ToolCalls: resp.ToolCalls}, nil
}
