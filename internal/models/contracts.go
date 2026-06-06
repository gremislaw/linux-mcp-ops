package models

import "time"

// OrchestratorRequest — вход от Telegram-бота (orchestrator.requests).
type OrchestratorRequest struct {
	RequestID     string    `json:"request_id"`
	CorrelationID string    `json:"correlation_id"`
	ChatID        int64     `json:"chat_id"`
	UserText      string    `json:"user_text"`
	Timestamp     time.Time `json:"timestamp"`
}

// AgentRequest — задача для Linux Agent (agent.requests).
type AgentRequest struct {
	RequestID     string         `json:"request_id"`
	CorrelationID string         `json:"correlation_id"`
	Intent        string         `json:"intent"`
	Payload       map[string]any `json:"payload"`
	Mode          string         `json:"mode"`
}

// AgentResponse — ответ от Linux Agent (agent.responses).
type AgentResponse struct {
	RequestID     string         `json:"request_id"`
	CorrelationID string         `json:"correlation_id"`
	Status        string         `json:"status"`
	Result        map[string]any `json:"result"`
	Error         *string        `json:"error"`
}

// OrchestratorResponse — ответ боту (orchestrator.responses).
type OrchestratorResponse struct {
	CorrelationID string    `json:"correlation_id"`
	ChatID        int64     `json:"chat_id"`
	Text          string    `json:"text"`
	Timestamp     time.Time `json:"timestamp"`
}

// OrchestratorDLQMessage — poison pill / необработанные ошибки.
type OrchestratorDLQMessage struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	RequestID     string         `json:"request_id,omitempty"`
	FailedAt      time.Time      `json:"failed_at"`
	ErrorCode     string         `json:"error_code"`
	ErrorMessage  string         `json:"error_message"`
	OriginalTopic string         `json:"original_topic"`
	Message       map[string]any `json:"message"`
}

const (
	AgentModeDryRun = "dry-run"
	AgentModeExec   = "exec"

	IntentDiagnoseAuth     = "diagnose_auth"
	IntentSetupWorkstation = "setup_workstation"

	AgentStatusSuccess         = "success"
	AgentStatusError           = "error"
	AgentStatusValidationError = "validation_error"

	MsgUnrecognizedRequest = "Не удалось распознать запрос. Пожалуйста, используйте команды /diagnose или /setup"
	MsgAgentTimeout        = "Превышено время ожидания ответа от системы"
)
