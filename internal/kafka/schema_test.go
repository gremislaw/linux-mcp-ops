package kafka

import "testing"

func TestValidateOrchestratorRequest(t *testing.T) {
	valid := []byte(`{
		"request_id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"chat_id": 1001,
		"user_text": "не заходит по SSH",
		"timestamp": "2026-06-06T12:00:00Z"
	}`)
	if err := ValidateOrchestratorRequest(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}

func TestValidateAgentRequest(t *testing.T) {
	valid := []byte(`{
		"request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"intent": "diagnose_auth",
		"payload": {"host": "prod-01"},
		"mode": "dry-run"
	}`)
	if err := ValidateAgentRequest(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}

func TestValidateAgentResponse(t *testing.T) {
	valid := []byte(`{
		"request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"status": "success",
		"result": {"message": "ok"},
		"error": null
	}`)
	if err := ValidateAgentResponse(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}

func TestValidateOrchestratorResponse(t *testing.T) {
	valid := []byte(`{
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"chat_id": 1001,
		"text": "[OK] nginx restarted",
		"timestamp": "2026-06-06T12:00:00Z"
	}`)
	if err := ValidateOrchestratorResponse(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}
