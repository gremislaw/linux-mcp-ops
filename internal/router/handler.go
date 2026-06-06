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

	"redops/internal/agent"
	"redops/internal/botfmt"
	"redops/internal/kafka"
	"redops/internal/tracker"
)

type Handler struct {
	llm          Client
	agent        *agent.Dispatcher
	orchestrator AgentSender
	tracker      *tracker.RequestTracker
	waitTimeout  time.Duration
	log          *log.Logger
}

type AgentSender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type HandlerConfig struct {
	LLM          Client
	Agent        *agent.Dispatcher
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

func (h *Handler) handlePassive(_ context.Context, req kafka.OrchestratorRequest) error {
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
	if len(result.ToolCalls) == 0 {
		return fmt.Errorf("llm returned no tool_calls for request %s", req.ID)
	}

	toolCall := result.ToolCalls[0]

	agentReq, err := h.agent.Dispatch(ctx, agent.DispatchInput{
		RequestID:     req.ID,
		CorrelationID: req.CorrelationID,
		Tool:          toolCall.Name,
		Arguments:     toolCall.Arguments,
	})
	if err != nil {
		return fmt.Errorf("dispatch agent request: %w", err)
	}

	h.log.Printf(
		"dispatched to %s tool=%s mode=%s request_id=%s correlation_id=%s agent_id=%s",
		kafka.TopicAgentRequests, agentReq.Tool, agentReq.Mode, agentReq.RequestID, agentReq.CorrelationID, agentReq.ID,
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
		resp.Text = botfmt.FormatAgentResponse(agentResp)
	} else if agentResp.Error != nil {
		resp.Error = agentResp.Error
		resp.Text = botfmt.FormatAgentResponse(agentResp)
	} else {
		resp.Error = &kafka.ErrorDetail{Code: "AGENT_ERROR", Message: "agent returned error status without details"}
		resp.Text = botfmt.FormatAgentResponse(kafka.AgentResponse{Status: "error", Error: resp.Error})
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
		Text: botfmt.FormatTimeout(h.waitTimeout),
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
