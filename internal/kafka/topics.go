package kafka

const (
	TopicTGRequests           = "tg.requests"
	TopicOrchestratorRequests = "orchestrator.requests"
	TopicAgentRequests        = "agent.requests"
	TopicWorkerResponses      = "worker.responses"
	TopicWorkerDLQ            = "worker.dlq"
)

const DefaultBootstrap = "localhost:9092"
