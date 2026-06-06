package kafka

import "time"

const (
	TopicOrchestratorRequests  = "orchestrator.requests"
	TopicOrchestratorResponses = "orchestrator.responses"
	TopicOrchestratorDLQ       = "orchestrator.dlq"
	TopicAgentRequests         = "agent.requests"
	TopicAgentResponses        = "agent.responses"
)

const (
	DefaultBootstrap   = "localhost:9092"
	DefaultWaitTimeout = 60 * time.Second
)
