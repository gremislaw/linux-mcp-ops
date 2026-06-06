package router

// ActionIntents — intent-ы, требующие цепочку LLM → agent.requests.
var ActionIntents = map[string]struct{}{
	"execute":   {},
	"tool_call": {},
	"query":     {},
}

func RequiresAction(intent string) bool {
	_, ok := ActionIntents[intent]
	return ok
}
