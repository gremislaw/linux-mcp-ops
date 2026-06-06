package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"redops/internal/kafka"
)

type AgentSender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type Handler struct {
	llm   Client
	agent AgentSender
	log   *log.Logger
}

func NewHandler(llm Client, agent AgentSender) *Handler {
	if llm == nil {
		llm = StubClient{}
	}
	return &Handler{
		llm:   llm,
		agent: agent,
		log:   log.New(os.Stderr, "[router] ", log.LstdFlags),
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var req kafka.OrchestratorRequest
	if err := json.Unmarshal(msg.Value, &req); err != nil {
		return fmt.Errorf("parse orchestrator request: %w", err)
	}

	return h.Handle(ctx, req)
}

func (h *Handler) Handle(ctx context.Context, req kafka.OrchestratorRequest) error {
	h.log.Printf("received orchestrator request id=%s intent=%s text=%q", req.ID, req.Intent, req.Text)

	if !RequiresAction(req.Intent) {
		h.log.Printf("passive intent=%s, no action required", req.Intent)
		return nil
	}

	result, err := h.llm.Analyze(ctx, req)
	if err != nil {
		return fmt.Errorf("llm analyze: %w", err)
	}

	agentReq := kafka.AgentRequest{
		ID:                    uuid.NewString(),
		CorrelationID:         req.CorrelationID,
		OrchestratorRequestID: req.ID,
		Timestamp:             time.Now().UTC(),
		Intent:                req.Intent,
		Prompt:                result.Prompt,
		Plan:                  result.Plan,
	}

	partition, offset, err := h.agent.Send(ctx, req.CorrelationID, agentReq)
	if err != nil {
		return fmt.Errorf("send agent request: %w", err)
	}

	h.log.Printf(
		"routed to %s partition=%d offset=%d orchestrator_id=%s agent_id=%s",
		kafka.TopicAgentRequests, partition, offset, req.ID, agentReq.ID,
	)
	return nil
}
