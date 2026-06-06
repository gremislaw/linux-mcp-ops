package kafka

import "time"

const (
	TopicTGRequests            = "tg.requests"
	TopicOrchestratorRequests  = "orchestrator.requests"
	TopicOrchestratorResponses = "orchestrator.responses"
	TopicAgentRequests         = "agent.requests"
	TopicAgentResponses        = "agent.responses"
	TopicWorkerResponses       = "worker.responses"
	TopicWorkerDLQ             = "worker.dlq"
)

const (
	DefaultBootstrap  = "localhost:9092"
	DefaultWaitTimeout = 60 * time.Second
)
