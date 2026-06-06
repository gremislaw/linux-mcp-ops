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
)

func main() {
	log.SetOutput(os.Stderr)

	brokers := flag.String("brokers", kafka.DefaultBootstrap, "Kafka bootstrap servers")
	correlationID := flag.String("correlation-id", "", "Optional correlation_id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer, err := kafka.NewOrchestratorRequestProducer([]string{*brokers})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	corr := *correlationID
	if corr == "" {
		corr = uuid.NewString()
	}

	req := kafka.OrchestratorRequest{
		RequestID:     uuid.NewString(),
		CorrelationID: corr,
		ChatID:        1001,
		UserText:      "Пользователь не заходит по SSH",
		Timestamp:     time.Now().UTC(),
	}

	partition, offset, err := producer.Send(ctx, corr, req)
	if err != nil {
		log.Fatalf("send: %v", err)
	}

	fmt.Fprintf(os.Stderr, "sent partition=%d offset=%d correlation_id=%s\n", partition, offset, corr)
	fmt.Println(corr)
}
