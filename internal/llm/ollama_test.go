package llm

import (
	"encoding/json"
	"testing"
)

func TestParseToolArgumentsObject(t *testing.T) {
	args, err := parseToolArguments(json.RawMessage(`{"host":"prod-01","username":"admin"}`))
	if err != nil {
		t.Fatal(err)
	}
	if args["host"] != "prod-01" {
		t.Fatalf("unexpected host: %v", args["host"])
	}
}

func TestParseToolArgumentsString(t *testing.T) {
	args, err := parseToolArguments(json.RawMessage(`"{\"host\":\"prod-01\"}"`))
	if err != nil {
		t.Fatal(err)
	}
	if args["host"] != "prod-01" {
		t.Fatalf("unexpected host: %v", args["host"])
	}
}

func TestParseLLMResponse(t *testing.T) {
	raw := responseMessage{
		Content: "",
		ToolCalls: []responseToolCall{
			{
				Type: "function",
				Function: struct {
					Index     int             `json:"index"`
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				}{
					Name:      "diagnose_auth",
					Arguments: json.RawMessage(`{"host":"prod-01","symptom":"permission denied"}`),
				},
			},
		},
	}

	resp, err := parseLLMResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].Name != "diagnose_auth" {
		t.Fatalf("unexpected tool name: %s", resp.ToolCalls[0].Name)
	}
	if resp.ToolCalls[0].Arguments["host"] != "prod-01" {
		t.Fatalf("unexpected host: %v", resp.ToolCalls[0].Arguments["host"])
	}
}

func TestAgentTools(t *testing.T) {
	toolsList, err := agentTools()
	if err != nil {
		t.Fatal(err)
	}
	if len(toolsList) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(toolsList))
	}

	names := map[string]bool{}
	for _, tool := range toolsList {
		names[tool.Function.Name] = true
		if tool.Type != "function" {
			t.Fatalf("unexpected tool type: %s", tool.Type)
		}
		if len(tool.Function.Parameters) == 0 {
			t.Fatalf("empty parameters for %s", tool.Function.Name)
		}
	}

	if !names["diagnose_auth"] || !names["setup_workstation"] {
		t.Fatalf("missing expected tools: %#v", names)
	}
}

func TestCallOllamaIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Setenv("OLLAMA_BASE_URL", "http://localhost:11434")

	ctx := t.Context()
	resp, err := CallOllama(ctx, "Пользователь не заходит по SSH")
	if err != nil {
		t.Skipf("ollama unavailable: %v", err)
	}

	if len(resp.ToolCalls) == 0 {
		t.Fatalf("expected tool_calls, got content=%q", resp.Content)
	}

	found := false
	for _, tc := range resp.ToolCalls {
		if tc.Name == "diagnose_auth" {
			found = true
			if tc.Arguments["host"] == nil && tc.Arguments["symptom"] == nil {
				t.Fatalf("diagnose_auth arguments are empty: %#v", tc.Arguments)
			}
		}
	}
	if !found {
		t.Fatalf("expected diagnose_auth tool call, got: %#v", resp.ToolCalls)
	}
}
