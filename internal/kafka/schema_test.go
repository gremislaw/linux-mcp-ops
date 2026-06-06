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

	invalid := []byte(`{"id": "not-uuid", "text": ""}`)
	if err := ValidateTelegramRequest(invalid); err == nil {
		t.Fatal("expected validation error")
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
