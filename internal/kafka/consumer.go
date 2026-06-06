package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

type Consumer struct {
	group    sarama.ConsumerGroup
	topic    string
	handler  MessageHandler
	dlq      *Producer
	maxRetry int
	logger   *log.Logger
}

type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Handler  MessageHandler
	DLQ      *Producer
	MaxRetry int
	Validate func([]byte) error
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{DefaultBootstrap}
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("group id is required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("handler is required")
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 3
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Version = sarama.V3_6_0_0

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	handler := cfg.Handler
	if cfg.Validate != nil {
		validate := cfg.Validate
		inner := handler
		handler = func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			if err := validate(msg.Value); err != nil {
				return fmt.Errorf("validate message: %w", err)
			}
			return inner(ctx, msg)
		}
	}

	return &Consumer{
		group:    group,
		topic:    cfg.Topic,
		handler:  handler,
		dlq:      cfg.DLQ,
		maxRetry: cfg.MaxRetry,
		logger:   log.New(os.Stderr, "[kafka-consumer] ", log.LstdFlags),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	handler := &consumerGroupHandler{
		consumer: c,
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := c.group.Consume(ctx, []string{c.topic}, handler); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

type consumerGroupHandler struct {
	consumer *Consumer
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.processMessage(session, msg); err != nil {
			h.consumer.logger.Printf("fatal processing error: %v", err)
			return err
		}
	}
	return nil
}

func (h *consumerGroupHandler) processMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	ctx := session.Context()
	var lastErr error

	for attempt := 1; attempt <= h.consumer.maxRetry; attempt++ {
		lastErr = h.consumer.handler(ctx, msg)
		if lastErr == nil {
			session.MarkMessage(msg, "")
			session.Commit()
			return nil
		}

		h.consumer.logger.Printf(
			"retry %d/%d topic=%s partition=%d offset=%d: %v",
			attempt, h.consumer.maxRetry, msg.Topic, msg.Partition, msg.Offset, lastErr,
		)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}

	if err := h.sendToDLQ(ctx, msg, lastErr); err != nil {
		return fmt.Errorf("send to dlq: %w", err)
	}

	session.MarkMessage(msg, "")
	session.Commit()
	return nil
}

func (h *consumerGroupHandler) sendToDLQ(ctx context.Context, msg *sarama.ConsumerMessage, procErr error) error {
	if h.consumer.dlq == nil {
		return fmt.Errorf("dlq producer not configured: %w", procErr)
	}

	var original map[string]any
	if err := json.Unmarshal(msg.Value, &original); err != nil {
		original = map[string]any{"raw": string(msg.Value)}
	}

	correlationID, _ := original["correlation_id"].(string)
	if correlationID == "" {
		correlationID = uuid.NewString()
	}

	dlqMsg := WorkerDLQMessage{
		ID:                uuid.NewString(),
		CorrelationID:     correlationID,
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		FailedAt:          time.Now().UTC(),
		RetryCount:        h.consumer.maxRetry,
		Error: ErrorDetail{
			Code:    "PROCESSING_FAILED",
			Message: procErr.Error(),
		},
		Message: original,
	}

	_, _, err := h.consumer.dlq.Send(ctx, correlationID, dlqMsg)
	return err
}
