package botfmt

import (
	"strings"
	"testing"
	"time"

	"redops/internal/kafka"
)

func TestFormatSuccessResult(t *testing.T) {
	text := FormatAgentResponse(kafka.AgentResponse{
		Status:  "success",
		Payload: map[string]any{"result": "nginx restarted on prod-01"},
	})
	if !strings.HasPrefix(text, "✅") {
		t.Fatalf("unexpected prefix: %q", text)
	}
	if !strings.Contains(text, "nginx restarted on prod-01") {
		t.Fatalf("unexpected text: %q", text)
	}
	if strings.Contains(text, "map[") {
		t.Fatalf("looks like go dump: %q", text)
	}
}

func TestFormatSuccessLogs(t *testing.T) {
	text := FormatAgentResponse(kafka.AgentResponse{
		Status: "success",
		Payload: map[string]any{
			"logs": []any{"step 1: ok", "step 2: ok"},
		},
	})
	if !strings.Contains(text, "step 1: ok") || !strings.Contains(text, "step 2: ok") {
		t.Fatalf("unexpected text: %q", text)
	}
}

func TestFormatValidationErrors(t *testing.T) {
	text := FormatAgentResponse(kafka.AgentResponse{
		Status: "success",
		Payload: map[string]any{
			"validation_errors": []any{
				map[string]any{"field": "host", "message": "required"},
			},
		},
	})
	if !strings.Contains(text, "host: required") {
		t.Fatalf("unexpected text: %q", text)
	}
}

func TestFormatError(t *testing.T) {
	text := FormatAgentResponse(kafka.AgentResponse{
		Status: "error",
		Error:  &kafka.ErrorDetail{Code: "VALIDATION_FAILED", Message: "invalid host"},
	})
	if !strings.Contains(text, "VALIDATION_FAILED") || !strings.Contains(text, "invalid host") {
		t.Fatalf("unexpected text: %q", text)
	}
}

func TestFormatTimeout(t *testing.T) {
	text := FormatTimeout(60 * time.Second)
	if !strings.Contains(text, "1m0s") {
		t.Fatalf("unexpected text: %q", text)
	}
}
