package kafka

import "time"

type TelegramRequest struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	Timestamp     time.Time      `json:"timestamp"`
	ChatID        int64          `json:"chat_id"`
	UserID        int64          `json:"user_id"`
	Text          string         `json:"text"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type WorkerResponse struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	RequestID     string         `json:"request_id"`
	Timestamp     time.Time      `json:"timestamp"`
	Status        string         `json:"status"`
	Payload       map[string]any `json:"payload,omitempty"`
	Error         *ErrorDetail   `json:"error,omitempty"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type WorkerDLQMessage struct {
	ID                string         `json:"id"`
	CorrelationID     string         `json:"correlation_id"`
	OriginalTopic     string         `json:"original_topic"`
	OriginalPartition int32          `json:"original_partition,omitempty"`
	OriginalOffset    int64          `json:"original_offset,omitempty"`
	FailedAt          time.Time      `json:"failed_at"`
	RetryCount        int            `json:"retry_count"`
	Error             ErrorDetail    `json:"error"`
	Message           map[string]any `json:"message"`
}

type OrchestratorRequest struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	Timestamp     time.Time      `json:"timestamp"`
	Intent        string         `json:"intent"`
	Text          string         `json:"text"`
	ChatID        int64          `json:"chat_id,omitempty"`
	UserID        int64          `json:"user_id,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type AgentRequest struct {
	ID                    string         `json:"id"`
	CorrelationID         string         `json:"correlation_id"`
	OrchestratorRequestID string         `json:"orchestrator_request_id"`
	Timestamp             time.Time      `json:"timestamp"`
	Intent                string         `json:"intent"`
	Prompt                string         `json:"prompt"`
	Plan                  map[string]any `json:"plan"`
}
