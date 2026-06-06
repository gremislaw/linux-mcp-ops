package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"redops/internal/kafka"
	"redops/internal/router"
	"redops/internal/tracker"
)

func main() {
	log.SetOutput(os.Stderr)

	brokers := flag.String("brokers", kafka.DefaultBootstrap, "Kafka bootstrap servers")
	mode := flag.String("mode", "produce-orchestrator", "Mode: produce-orchestrator or consume")
	intent := flag.String("intent", "execute", "Intent for produce-orchestrator mode")
	correlationID := flag.String("correlation-id", "", "Optional fixed correlation_id for produce mode")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch *mode {
	case "produce", "produce-orchestrator":
		if err := runProducer(ctx, *brokers, *intent, *correlationID); err != nil {
			log.Fatalf("produce failed: %v", err)
		}
	case "consume":
		if err := runOrchestrator(ctx, *brokers); err != nil {
			log.Fatalf("consume failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runProducer(ctx context.Context, brokers, intent, fixedCorrelationID string) error {
	producer, err := kafka.NewOrchestratorRequestProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	correlationID := fixedCorrelationID
	if correlationID == "" {
		correlationID = uuid.NewString()
	}

	req := kafka.OrchestratorRequest{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC(),
		Intent:        intent,
		Text:          "restart nginx on prod-01",
		ChatID:        1001,
		UserID:        2002,
	}

	partition, offset, err := producer.Send(ctx, correlationID, req)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "sent to %s partition=%d offset=%d correlation_id=%s intent=%s\n",
		kafka.TopicOrchestratorRequests, partition, offset, correlationID, intent)
	fmt.Println(correlationID)
	return nil
}

func runOrchestrator(ctx context.Context, brokers string) error {
	brokerList := []string{brokers}

	dlq, err := kafka.NewDLQProducer(brokerList)
	if err != nil {
		return err
	}
	defer dlq.Close()

	agentProducer, err := kafka.NewAgentRequestProducer(brokerList)
	if err != nil {
		return err
	}
	defer agentProducer.Close()

	orchestratorProducer, err := kafka.NewOrchestratorResponseProducer(brokerList)
	if err != nil {
		return err
	}
	defer orchestratorProducer.Close()

	requestTracker := tracker.New()
	responseHandler := tracker.NewResponseHandler(requestTracker)

	handler := router.NewHandler(router.HandlerConfig{
		LLM:          router.StubClient{},
		Agent:        agentProducer,
		Orchestrator: orchestratorProducer,
		Tracker:      requestTracker,
	})

	requestConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  brokerList,
		Topic:    kafka.TopicOrchestratorRequests,
		GroupID:  "redops-orchestrator",
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
		Brokers:  brokerList,
		Topic:    kafka.TopicAgentResponses,
		GroupID:  "redops-orchestrator-responses",
		MaxRetry: 3,
		Validate: kafka.ValidateAgentResponse,
		Handler:  responseHandler.HandleMessage,
	})
	if err != nil {
		return err
	}
	defer responseConsumer.Close()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return requestConsumer.Run(ctx) })
	g.Go(func() error { return responseConsumer.Run(ctx) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
