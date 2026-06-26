package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Config struct {
	BaseURL string
	Model   string
	Timeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.BaseURL == "" {
		c.BaseURL = "http://ollama:11434"
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.Model == "" {
		c.Model = "qwen2.5:7b"
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	return c
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Tools    []ollamaTool  `json:"tools"`
	Stream   bool          `json:"stream"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Message responseMessage `json:"message"`
}

type responseMessage struct {
	Role      string             `json:"role"`
	Content   string             `json:"content"`
	ToolCalls []responseToolCall `json:"tool_calls"`
}

type responseToolCall struct {
	Type     string `json:"type"`
	Function struct {
		Index     int             `json:"index"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	} `json:"function"`
}

func CallOllama(ctx context.Context, cfg Config, userText string) (*LLMResponse, error) {
	cfg = cfg.withDefaults()

	toolsList, err := agentTools()
	if err != nil {
		return nil, err
	}

	reqBody := chatRequest{
		Model: cfg.Model,
		Messages: []chatMessage{
			{
				Role: "system",
				Content: "You are a Linux operations assistant for RedOS. " +
					"Classify user intent and call diagnose_auth or setup_workstation tool. " +
					"Never answer in plain text when a tool applies.",
			},
			{Role: "user", Content: userText},
		},
		Tools:  toolsList,
		Stream: false,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := cfg.BaseURL + "/api/chat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama status %d: %s", resp.StatusCode, string(body))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return parseLLMResponse(chatResp.Message)
}

func parseLLMResponse(msg responseMessage) (*LLMResponse, error) {
	result := &LLMResponse{
		Content:   msg.Content,
		ToolCalls: make([]ToolCall, 0, len(msg.ToolCalls)),
	}

	for _, tc := range msg.ToolCalls {
		args, err := parseToolArguments(tc.Function.Arguments)
		if err != nil {
			return nil, fmt.Errorf("parse tool arguments for %s: %w", tc.Function.Name, err)
		}
		result.ToolCalls = append(result.ToolCalls, ToolCall{
			Name:      tc.Function.Name,
			Arguments: args,
		})
	}

	return result, nil
}

func parseToolArguments(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj, nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("unsupported arguments format: %s", string(raw))
	}
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return nil, fmt.Errorf("parse arguments string: %w", err)
	}
	return obj, nil
}
