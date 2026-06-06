package agent

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"redops/internal/kafka"
	"redops/internal/models"
)

type Sender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type DispatchInput struct {
	CorrelationID string
	Intent        string
	Payload       map[string]any
	Mode          string
}

type Dispatcher struct {
	producer Sender
	mode     string
}

func NewDispatcher(producer Sender, mode string) *Dispatcher {
	if mode == "" {
		mode = models.AgentModeDryRun
	}
	return &Dispatcher{producer: producer, mode: mode}
}

func BuildRequest(in DispatchInput) (kafka.AgentRequest, error) {
	if in.CorrelationID == "" {
		return kafka.AgentRequest{}, fmt.Errorf("correlation_id is required")
	}
	if in.Intent == "" {
		return kafka.AgentRequest{}, fmt.Errorf("intent is required")
	}
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}

	mode := in.Mode
	if mode == "" {
		mode = models.AgentModeDryRun
	}

	return kafka.AgentRequest{
		RequestID:     uuid.NewString(),
		CorrelationID: in.CorrelationID,
		Intent:        in.Intent,
		Payload:       in.Payload,
		Mode:          mode,
	}, nil
}

func (d *Dispatcher) Dispatch(ctx context.Context, in DispatchInput) (kafka.AgentRequest, error) {
	req, err := BuildRequest(DispatchInput{
		CorrelationID: in.CorrelationID,
		Intent:        in.Intent,
		Payload:       in.Payload,
		Mode:          d.mode,
	})
	if err != nil {
		return kafka.AgentRequest{}, err
	}

	if _, _, err := d.producer.Send(ctx, in.CorrelationID, req); err != nil {
		return kafka.AgentRequest{}, fmt.Errorf("send agent request: %w", err)
	}

	return req, nil
}
