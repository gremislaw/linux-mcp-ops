package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"

	"redops/internal/kafka"
)

type ResponseHandler struct {
	tracker *RequestTracker
	log     *slog.Logger
}

func NewResponseHandler(tracker *RequestTracker, log *slog.Logger) *ResponseHandler {
	if log == nil {
		log = slog.Default()
	}
	return &ResponseHandler{tracker: tracker, log: log}
}

func (h *ResponseHandler) HandleMessage(_ context.Context, msg *sarama.ConsumerMessage) error {
	var resp kafka.AgentResponse
	if err := json.Unmarshal(msg.Value, &resp); err != nil {
		return fmt.Errorf("parse agent response: %w", err)
	}

	if resp.CorrelationID == "" {
		return fmt.Errorf("agent response missing correlation_id")
	}

	if !h.tracker.Deliver(resp.CorrelationID, resp) {
		h.log.Warn("no waiter for agent response", "correlation_id", resp.CorrelationID)
		return nil
	}

	h.log.Info("delivered agent response",
		"correlation_id", resp.CorrelationID,
		"request_id", resp.RequestID,
		"status", resp.Status,
	)
	return nil
}
