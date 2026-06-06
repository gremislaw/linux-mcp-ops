package botfmt

import (
	"strings"
	"testing"

	"redops/internal/kafka"
	"redops/internal/models"
)

func strPtr(s string) *string { return &s }

func TestFormatSuccessResult(t *testing.T) {
	text := FormatAgentResponse(kafka.AgentResponse{
		Status: models.AgentStatusSuccess,
		Result: map[string]any{"message": "nginx restarted on prod-01"},
		Error:  nil,
	})
	if !strings.HasPrefix(text, "[OK]") {
		t.Fatalf("expected [OK] prefix: %q", text)
	}
	if !strings.Contains(text, "nginx restarted on prod-01") {
		t.Fatalf("unexpected: %q", text)
	}
}

func TestFormatError(t *testing.T) {
	errMsg := "invalid host"
	text := FormatAgentResponse(kafka.AgentResponse{
		Status: models.AgentStatusError,
		Error:  &errMsg,
	})
	if !strings.HasPrefix(text, "[ERROR]") {
		t.Fatalf("expected [ERROR] prefix: %q", text)
	}
	if !strings.Contains(text, "invalid host") {
		t.Fatalf("unexpected: %q", text)
	}
}

func TestMaskSecrets(t *testing.T) {
	text := MaskSecrets("domain_admin_password=secret123")
	if strings.Contains(text, "secret123") {
		t.Fatalf("secret not masked: %q", text)
	}
}

func TestFormatTimeout(t *testing.T) {
	if FormatTimeout() != models.MsgAgentTimeout {
		t.Fatal("timeout message mismatch")
	}
}

func TestFormatNoEmoji(t *testing.T) {
	errMsg := "connection refused"
	cases := []string{
		FormatAgentResponse(kafka.AgentResponse{
			Status: models.AgentStatusSuccess,
			Result: map[string]any{"message": "done"},
		}),
		FormatAgentResponse(kafka.AgentResponse{
			Status: models.AgentStatusError,
			Error:  &errMsg,
		}),
		FormatAgentResponse(kafka.AgentResponse{
			Status: models.AgentStatusValidationError,
			Error:  &errMsg,
		}),
		FormatTimeout(),
		FormatUnrecognized(),
	}
	for _, text := range cases {
		if strings.ContainsAny(text, "✅❌⚠️⏱🔹📦🧪") {
			t.Fatalf("emoji found in output: %q", text)
		}
	}
}
