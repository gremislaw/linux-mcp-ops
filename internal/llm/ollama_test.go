package llm

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseToolArgumentsObject(t *testing.T) {
	args, err := parseToolArguments(json.RawMessage(`{"host":"prod-01"}`))
	if err != nil {
		t.Fatal(err)
	}
	if args["host"] != "prod-01" {
		t.Fatalf("unexpected host: %v", args["host"])
	}
}

func TestParseLLMResponse(t *testing.T) {
	raw := responseMessage{
		ToolCalls: []responseToolCall{{
			Type: "function",
			Function: struct {
				Index     int             `json:"index"`
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}{
				Name:      "diagnose_auth",
				Arguments: json.RawMessage(`{"host":"prod-01"}`),
			},
		}},
	}

	resp, err := parseLLMResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ToolCalls[0].Name != "diagnose_auth" {
		t.Fatalf("unexpected tool: %s", resp.ToolCalls[0].Name)
	}
}

func TestCallOllamaIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	t.Setenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")

	ctx := t.Context()
	resp, err := CallOllama(ctx, Config{BaseURL: "http://127.0.0.1:11434", Model: "llama3.2", Timeout: 30 * time.Second}, "Пользователь не заходит по SSH")
	if err != nil {
		t.Skipf("ollama unavailable: %v", err)
	}

	found := false
	for _, tc := range resp.ToolCalls {
		if tc.Name == "diagnose_auth" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected diagnose_auth, got %#v", resp.ToolCalls)
	}
}
