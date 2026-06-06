package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultOllamaBaseURL = "http://ollama.ollama.svc.cluster.local:11434"
	defaultOllamaModel   = "llama3.2"
)

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

func ollamaBaseURL() string {
	if v := os.Getenv("OLLAMA_BASE_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return defaultOllamaBaseURL
}

func ollamaModel() string {
	if v := os.Getenv("OLLAMA_MODEL"); v != "" {
		return v
	}
	return defaultOllamaModel
}

func CallOllama(ctx context.Context, userText string) (*LLMResponse, error) {
	toolsList, err := agentTools()
	if err != nil {
		return nil, err
	}

	reqBody := chatRequest{
		Model: ollamaModel(),
		Messages: []chatMessage{
			{
				Role: "system",
				Content: "You are a Linux operations assistant. " +
					"When the user describes infrastructure or access problems, " +
					"call the appropriate tool instead of answering in plain text.",
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

	url := ollamaBaseURL() + "/api/chat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
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
