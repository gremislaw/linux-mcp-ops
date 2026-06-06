package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"redops/internal/kafka"
)

const DefaultMode = "dry-run"

type Sender interface {
	Send(ctx context.Context, key string, value any) (partition int32, offset int64, err error)
}

type DispatchInput struct {
	RequestID     string
	CorrelationID string
	Tool          string
	Arguments     map[string]any
	Mode          string
}

type Dispatcher struct {
	producer Sender
	mode     string
}

func NewDispatcher(producer Sender, mode string) *Dispatcher {
	if mode == "" {
		mode = DefaultMode
	}
	return &Dispatcher{producer: producer, mode: mode}
}

func BuildRequest(in DispatchInput, defaultMode string) (kafka.AgentRequest, error) {
	if in.RequestID == "" {
		return kafka.AgentRequest{}, fmt.Errorf("request_id is required")
	}
	if in.CorrelationID == "" {
		return kafka.AgentRequest{}, fmt.Errorf("correlation_id is required")
	}
	if in.Tool == "" {
		return kafka.AgentRequest{}, fmt.Errorf("tool is required")
	}
	if in.Arguments == nil {
		in.Arguments = map[string]any{}
	}

	mode := in.Mode
	if mode == "" {
		mode = defaultMode
	}
	if mode == "" {
		mode = DefaultMode
	}

	return kafka.AgentRequest{
		ID:            uuid.NewString(),
		RequestID:     in.RequestID,
		CorrelationID: in.CorrelationID,
		Timestamp:     time.Now().UTC(),
		Tool:          in.Tool,
		Arguments:     in.Arguments,
		Mode:          mode,
	}, nil
}

func (d *Dispatcher) Dispatch(ctx context.Context, in DispatchInput) (kafka.AgentRequest, error) {
	req, err := BuildRequest(in, d.mode)
	if err != nil {
		return kafka.AgentRequest{}, err
	}

	if _, _, err := d.producer.Send(ctx, in.CorrelationID, req); err != nil {
		return kafka.AgentRequest{}, fmt.Errorf("send agent request: %w", err)
	}

	return req, nil
}
