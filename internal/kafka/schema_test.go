package kafka

import "testing"

func TestValidateTelegramRequest(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"timestamp": "2026-06-06T12:00:00Z",
		"chat_id": 12345,
		"user_id": 67890,
		"text": "hello"
	}`)

	if err := ValidateTelegramRequest(valid); err != nil {
		t.Fatalf("expected valid request: %v", err)
	}
}

func TestValidateOrchestratorRequest(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"timestamp": "2026-06-06T12:00:00Z",
		"intent": "execute",
		"text": "restart nginx"
	}`)

	if err := ValidateOrchestratorRequest(valid); err != nil {
		t.Fatalf("expected valid orchestrator request: %v", err)
	}
}

func TestValidateOrchestratorResponse(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"orchestrator_request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"timestamp": "2026-06-06T12:00:00Z",
		"status": "success",
		"chat_id": 1001,
		"text": "✅ nginx restarted on prod-01",
		"payload": {"result": "nginx restarted on prod-01"}
	}`)

	if err := ValidateOrchestratorResponse(valid); err != nil {
		t.Fatalf("expected valid orchestrator response: %v", err)
	}
}

func TestValidateAgentRequest(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"timestamp": "2026-06-06T12:00:00Z",
		"tool": "diagnose_auth",
		"arguments": {"host": "prod-01"},
		"mode": "dry-run"
	}`)

	if err := ValidateAgentRequest(valid); err != nil {
		t.Fatalf("expected valid agent request: %v", err)
	}
}

func TestValidateWorkerResponse(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"timestamp": "2026-06-06T12:00:00Z",
		"status": "success",
		"payload": {"reply": "ok"}
	}`)

	if err := ValidateWorkerResponse(valid); err != nil {
		t.Fatalf("expected valid response: %v", err)
	}
}

func TestValidateWorkerDLQ(t *testing.T) {
	valid := []byte(`{
		"id": "550e8400-e29b-41d4-a716-446655440000",
		"correlation_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"original_topic": "tg.requests",
		"failed_at": "2026-06-06T12:00:00Z",
		"retry_count": 3,
		"error": {"code": "PROCESSING_FAILED", "message": "boom"},
		"message": {"text": "hello"}
	}`)

	if err := ValidateWorkerDLQ(valid); err != nil {
		t.Fatalf("expected valid dlq message: %v", err)
	}
}
