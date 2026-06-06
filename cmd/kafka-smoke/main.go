package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"redops/internal/kafka"
)

func main() {
	log.SetOutput(os.Stderr)

	brokers := flag.String("brokers", kafka.DefaultBootstrap, "Kafka bootstrap servers")
	mode := flag.String("mode", "produce", "Mode: produce or consume")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch *mode {
	case "produce":
		if err := runProducer(ctx, *brokers); err != nil {
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

func runProducer(ctx context.Context, brokers string) error {
	producer, err := kafka.NewTelegramRequestProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	correlationID := uuid.NewString()
	req := kafka.TelegramRequest{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC(),
		ChatID:        1001,
		UserID:        2002,
		Text:          "smoke test message",
	}

	partition, offset, err := producer.Send(ctx, correlationID, req)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "sent to %s partition=%d offset=%d correlation_id=%s\n",
		kafka.TopicTGRequests, partition, offset, correlationID)
	return nil
}

func runConsumer(ctx context.Context, brokers string) error {
	dlq, err := kafka.NewDLQProducer([]string{brokers})
	if err != nil {
		return err
	}
	defer dlq.Close()

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  []string{brokers},
		Topic:    kafka.TopicTGRequests,
		GroupID:  "redops-smoke",
		DLQ:      dlq,
		MaxRetry: 3,
		Validate: kafka.ValidateTelegramRequest,
		Handler: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			var req kafka.TelegramRequest
			if err := json.Unmarshal(msg.Value, &req); err != nil {
				return err
			}
			log.Printf("received request id=%s text=%q", req.ID, req.Text)
			return nil
		},
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx)
}
