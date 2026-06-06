package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"redops/internal/agent"
	"redops/internal/botfmt"
	"redops/internal/kafka"
	"redops/internal/tracker"
)

type BotSender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type Handler struct {
	llm         Client
	agent       *agent.Dispatcher
	bot         BotSender
	tracker     *tracker.RequestTracker
	waitTimeout time.Duration
	log         *slog.Logger
}

type HandlerConfig struct {
	LLM          Client
	Agent        *agent.Dispatcher
	Bot          BotSender
	Tracker      *tracker.RequestTracker
	WaitTimeout  time.Duration
	Logger       *slog.Logger
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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Handler{
		llm:         cfg.LLM,
		agent:       cfg.Agent,
		bot:         cfg.Bot,
		tracker:     cfg.Tracker,
		waitTimeout: cfg.WaitTimeout,
		log:         cfg.Logger,
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var req kafka.OrchestratorRequest
	if err := json.Unmarshal(msg.Value, &req); err != nil {
		return fmt.Errorf("parse orchestrator request: %w", err)
	}

	go h.processRequest(context.WithoutCancel(ctx), req)
	return nil
}

func (h *Handler) Handle(ctx context.Context, req kafka.OrchestratorRequest) error {
	return h.processRequest(ctx, req)
}

func (h *Handler) processRequest(ctx context.Context, req kafka.OrchestratorRequest) error {
	log := h.log.With(
		"request_id", req.RequestID,
		"correlation_id", req.CorrelationID,
		"chat_id", req.ChatID,
	)

	log.Info("received orchestrator request", "user_text", req.UserText)

	result, err := h.llm.Analyze(ctx, req.UserText)
	if err != nil || len(result.ToolCalls) == 0 {
		log.Warn("llm did not return tool_calls", "error", err)
		return h.sendBotMessage(ctx, req, botfmt.FormatUnrecognized())
	}

	toolCall := result.ToolCalls[0]
	log.Info("llm tool call", "intent", toolCall.Name, "payload", toolCall.Arguments)

	waitCh := h.tracker.Register(req.CorrelationID)
	defer h.tracker.Unregister(req.CorrelationID)

	agentReq, err := h.agent.Dispatch(ctx, agent.DispatchInput{
		CorrelationID: req.CorrelationID,
		Intent:        toolCall.Name,
		Payload:       toolCall.Arguments,
	})
	if err != nil {
		return fmt.Errorf("dispatch agent request: %w", err)
	}

	log.Info("dispatched agent request",
		"agent_request_id", agentReq.RequestID,
		"intent", agentReq.Intent,
		"mode", agentReq.Mode,
	)

	timer := time.NewTimer(h.waitTimeout)
	defer timer.Stop()

	select {
	case agentResp, ok := <-waitCh:
		if !ok {
			return fmt.Errorf("wait channel closed for correlation_id=%s", req.CorrelationID)
		}
		log.Info("received agent response", "status", agentResp.Status)
		return h.sendBotMessage(ctx, req, botfmt.FormatAgentResponse(agentResp))
	case <-timer.C:
		log.Warn("agent response timeout", "timeout", h.waitTimeout)
		return h.sendBotMessage(ctx, req, botfmt.FormatTimeout())
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) sendBotMessage(ctx context.Context, req kafka.OrchestratorRequest, text string) error {
	resp := kafka.OrchestratorResponse{
		CorrelationID: req.CorrelationID,
		ChatID:        req.ChatID,
		Text:          botfmt.MaskSecrets(text),
		Timestamp:     time.Now().UTC(),
	}

	partition, offset, err := h.bot.Send(ctx, req.CorrelationID, resp)
	if err != nil {
		return fmt.Errorf("send orchestrator response: %w", err)
	}

	h.log.Info("sent bot response",
		"request_id", req.RequestID,
		"correlation_id", req.CorrelationID,
		"partition", partition,
		"offset", offset,
	)
	return nil
}
