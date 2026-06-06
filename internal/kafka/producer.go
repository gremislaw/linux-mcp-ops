package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	topic    string
	producer sarama.SyncProducer
	validate func([]byte) error
}

type ProducerConfig struct {
	Brokers  []string
	Topic    string
	Validate func([]byte) error
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{DefaultBootstrap}
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 250 * time.Millisecond
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V3_6_0_0

	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	return &Producer{
		topic:    cfg.Topic,
		producer: producer,
		validate: cfg.Validate,
	}, nil
}

func (p *Producer) Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal message: %w", err)
	}

	if p.validate != nil {
		if err := p.validate(data); err != nil {
			return 0, 0, fmt.Errorf("validate message: %w", err)
		}
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	partition, offset, err = p.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("send message: %w", err)
	}

	return partition, offset, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func NewTelegramRequestProducer(brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		Brokers:  brokers,
		Topic:    TopicTGRequests,
		Validate: ValidateTelegramRequest,
	})
}

func NewWorkerResponseProducer(brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		Brokers:  brokers,
		Topic:    TopicWorkerResponses,
		Validate: ValidateWorkerResponse,
	})
}

func NewDLQProducer(brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		Brokers:  brokers,
		Topic:    TopicWorkerDLQ,
		Validate: ValidateWorkerDLQ,
	})
}

func NewOrchestratorRequestProducer(brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		Brokers:  brokers,
		Topic:    TopicOrchestratorRequests,
		Validate: ValidateOrchestratorRequest,
	})
}

func NewAgentRequestProducer(brokers []string) (*Producer, error) {
	return NewProducer(ProducerConfig{
		Brokers:  brokers,
		Topic:    TopicAgentRequests,
		Validate: ValidateAgentRequest,
	})
}
