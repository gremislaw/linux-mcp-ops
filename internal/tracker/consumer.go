package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"

	"redops/internal/kafka"
)

type ResponseHandler struct {
	tracker *RequestTracker
	log     *log.Logger
}

func NewResponseHandler(tracker *RequestTracker) *ResponseHandler {
	return &ResponseHandler{
		tracker: tracker,
		log:     log.New(os.Stderr, "[agent-response] ", log.LstdFlags),
	}
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
		h.log.Printf("no waiter for correlation_id=%s", resp.CorrelationID)
		return nil
	}

	h.log.Printf("delivered agent response correlation_id=%s status=%s", resp.CorrelationID, resp.Status)
	return nil
}
