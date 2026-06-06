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
	"redops/internal/tracker"
)

type AgentSender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type Handler struct {
	llm           Client
	agent         AgentSender
	orchestrator  AgentSender
	tracker       *tracker.RequestTracker
	waitTimeout   time.Duration
	log           *log.Logger
}

type HandlerConfig struct {
	LLM          Client
	Agent        AgentSender
	Orchestrator AgentSender
	Tracker      *tracker.RequestTracker
	WaitTimeout  time.Duration
}

func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.LLM == nil {
		cfg.LLM = StubClient{}
	}
	if cfg.Tracker == nil {
		cfg.Tracker = tracker.New()
	}
	if cfg.WaitTimeout <= 0 {
		cfg.WaitTimeout = kafka.DefaultWaitTimeout
	}

	return &Handler{
		llm:          cfg.LLM,
		agent:        cfg.Agent,
		orchestrator: cfg.Orchestrator,
		tracker:      cfg.Tracker,
		waitTimeout:  cfg.WaitTimeout,
		log:          log.New(os.Stderr, "[router] ", log.LstdFlags),
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var req kafka.OrchestratorRequest
	if err := json.Unmarshal(msg.Value, &req); err != nil {
		return fmt.Errorf("parse orchestrator request: %w", err)
	}

	if !RequiresAction(req.Intent) {
		return h.handlePassive(ctx, req)
	}

	go h.processRequest(context.WithoutCancel(ctx), req)
	return nil
}

func (h *Handler) Handle(ctx context.Context, req kafka.OrchestratorRequest) error {
	if !RequiresAction(req.Intent) {
		return h.handlePassive(ctx, req)
	}
	return h.processRequest(ctx, req)
}

func (h *Handler) handlePassive(ctx context.Context, req kafka.OrchestratorRequest) error {
	h.log.Printf("passive intent=%s id=%s, no action required", req.Intent, req.ID)
	return nil
}

func (h *Handler) processRequest(ctx context.Context, req kafka.OrchestratorRequest) error {
	h.log.Printf("processing orchestrator request id=%s intent=%s text=%q", req.ID, req.Intent, req.Text)

	waitCh := h.tracker.Register(req.CorrelationID)
	defer h.tracker.Unregister(req.CorrelationID)

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

	timer := time.NewTimer(h.waitTimeout)
	defer timer.Stop()

	select {
	case agentResp, ok := <-waitCh:
		if !ok {
			return fmt.Errorf("wait channel closed for correlation_id=%s", req.CorrelationID)
		}
		return h.sendOrchestratorResponse(ctx, req, agentResp)
	case <-timer.C:
		return h.sendTimeoutResponse(ctx, req)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) sendOrchestratorResponse(ctx context.Context, req kafka.OrchestratorRequest, agentResp kafka.AgentResponse) error {
	resp := kafka.OrchestratorResponse{
		ID:                    uuid.NewString(),
		CorrelationID:         req.CorrelationID,
		OrchestratorRequestID: req.ID,
		Timestamp:             time.Now().UTC(),
		Status:                agentResp.Status,
		ChatID:                req.ChatID,
	}

	if agentResp.Status == "success" {
		resp.Payload = agentResp.Payload
	} else if agentResp.Error != nil {
		resp.Error = agentResp.Error
	} else {
		resp.Error = &kafka.ErrorDetail{Code: "AGENT_ERROR", Message: "agent returned error status without details"}
	}

	partition, offset, err := h.orchestrator.Send(ctx, req.CorrelationID, resp)
	if err != nil {
		return fmt.Errorf("send orchestrator response: %w", err)
	}

	h.log.Printf(
		"sent to %s partition=%d offset=%d correlation_id=%s status=%s",
		kafka.TopicOrchestratorResponses, partition, offset, req.CorrelationID, resp.Status,
	)
	return nil
}

func (h *Handler) sendTimeoutResponse(ctx context.Context, req kafka.OrchestratorRequest) error {
	resp := kafka.OrchestratorResponse{
		ID:                    uuid.NewString(),
		CorrelationID:         req.CorrelationID,
		OrchestratorRequestID: req.ID,
		Timestamp:             time.Now().UTC(),
		Status:                "timeout",
		ChatID:                req.ChatID,
		Error: &kafka.ErrorDetail{
			Code:    "AGENT_TIMEOUT",
			Message: fmt.Sprintf("agent did not respond within %s", h.waitTimeout),
		},
	}

	partition, offset, err := h.orchestrator.Send(ctx, req.CorrelationID, resp)
	if err != nil {
		return fmt.Errorf("send timeout response: %w", err)
	}

	h.log.Printf(
		"timeout sent to %s partition=%d offset=%d correlation_id=%s",
		kafka.TopicOrchestratorResponses, partition, offset, req.CorrelationID,
	)
	return nil
}
