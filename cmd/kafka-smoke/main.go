package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
	"redops/internal/router"
)

func main() {
	log.SetOutput(os.Stderr)

	brokers := flag.String("brokers", kafka.DefaultBootstrap, "Kafka bootstrap servers")
	mode := flag.String("mode", "produce-orchestrator", "Mode: produce-orchestrator or consume")
	intent := flag.String("intent", "execute", "Intent for produce-orchestrator mode")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch *mode {
	case "produce", "produce-orchestrator":
		if err := runProducer(ctx, *brokers, *intent); err != nil {
			log.Fatalf("produce failed: %v", err)
		}
	case "consume":
		if err := runConsumer(ctx, *brokers); err != nil {
			log.Fatalf("consume failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runProducer(ctx context.Context, brokers, intent string) error {
	producer, err := kafka.NewOrchestratorRequestProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	correlationID := uuid.NewString()
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
	return nil
}

func runConsumer(ctx context.Context, brokers string) error {
	dlq, err := kafka.NewDLQProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer dlq.Close()

	agentProducer, err := kafka.NewAgentRequestProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer agentProducer.Close()

	handler := router.NewHandler(router.StubClient{}, agentProducer)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  []string{brokers},
		Topic:    kafka.TopicOrchestratorRequests,
		GroupID:  "redops-router",
		DLQ:      dlq,
		MaxRetry: 3,
		Validate: kafka.ValidateOrchestratorRequest,
		Handler:  handler.HandleMessage,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx)
}
