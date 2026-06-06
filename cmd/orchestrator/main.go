package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"redops/internal/agent"
	"redops/internal/config"
	"redops/internal/kafka"
	"redops/internal/llm"
	"redops/internal/router"
	"redops/internal/tracker"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("orchestrator stopped", "error", err)
		os.Exit(1)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	<-shutdownCtx.Done()
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	dlq, err := kafka.NewOrchestratorDLQProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer dlq.Close()

	agentProducer, err := kafka.NewAgentRequestProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer agentProducer.Close()

	botProducer, err := kafka.NewOrchestratorResponseProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer botProducer.Close()

	requestTracker := tracker.New()
	responseHandler := tracker.NewResponseHandler(requestTracker, log)

	var llmClient router.Client = router.StubClient{}
	if cfg.UseOllama {
		llmClient = router.NewOllamaClient(llm.Config{
			BaseURL: cfg.OllamaBaseURL,
			Model:   cfg.OllamaModel,
			Timeout: cfg.OllamaTimeout,
		})
	}

	handler := router.NewHandler(router.HandlerConfig{
		LLM:         llmClient,
		Agent:       agent.NewDispatcher(agentProducer, ""),
		Bot:         botProducer,
		Tracker:     requestTracker,
		WaitTimeout: cfg.AgentWaitTimeout,
		Logger:      log,
	})

	requestConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    kafka.TopicOrchestratorRequests,
		GroupID:  cfg.ConsumerGroup,
		DLQ:      dlq,
		MaxRetry: 3,
		Validate: kafka.ValidateOrchestratorRequest,
		Handler:  handler.HandleMessage,
	})
	if err != nil {
		return err
	}
	defer requestConsumer.Close()

	responseConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    kafka.TopicAgentResponses,
		GroupID:  cfg.ResponseGroup,
		DLQ:      dlq,
		MaxRetry: 3,
		Validate: kafka.ValidateAgentResponse,
		Handler:  responseHandler.HandleMessage,
	})
	if err != nil {
		return err
	}
	defer responseConsumer.Close()

	log.Info("orchestrator started",
		"brokers", cfg.KafkaBrokers,
		"ollama", cfg.UseOllama,
		"model", cfg.OllamaModel,
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return requestConsumer.Run(ctx) })
	g.Go(func() error { return responseConsumer.Run(ctx) })

	return g.Wait()
}
