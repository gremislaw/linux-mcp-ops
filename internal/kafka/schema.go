package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"redops/schemas"
)

var (
	orchestratorRequestSchema  *jsonschema.Schema
	orchestratorResponseSchema *jsonschema.Schema
	orchestratorDLQSchema      *jsonschema.Schema
	agentRequestSchema         *jsonschema.Schema
	agentResponseSchema        *jsonschema.Schema
)

func init() {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	entries := map[string]**jsonschema.Schema{
		"orchestrator.requests":  &orchestratorRequestSchema,
		"orchestrator.responses": &orchestratorResponseSchema,
		"orchestrator.dlq":       &orchestratorDLQSchema,
		"agent.requests":         &agentRequestSchema,
		"agent.responses":        &agentResponseSchema,
	}

	for topic, target := range entries {
		data, err := schemas.FS.ReadFile(fmt.Sprintf("%s.schema.json", topic))
		if err != nil {
			panic(fmt.Errorf("read schema %s: %w", topic, err))
		}
		if err := compiler.AddResource(topic, bytes.NewReader(data)); err != nil {
			panic(fmt.Errorf("add schema resource %s: %w", topic, err))
		}
		schema, err := compiler.Compile(topic)
		if err != nil {
			panic(fmt.Errorf("compile schema %s: %w", topic, err))
		}
		*target = schema
	}
}

func ValidateOrchestratorRequest(data []byte) error {
	return validate(orchestratorRequestSchema, data)
}

func ValidateOrchestratorResponse(data []byte) error {
	return validate(orchestratorResponseSchema, data)
}

func ValidateOrchestratorDLQ(data []byte) error {
	return validate(orchestratorDLQSchema, data)
}

func ValidateAgentRequest(data []byte) error {
	return validate(agentRequestSchema, data)
}

func ValidateAgentResponse(data []byte) error {
	return validate(agentResponseSchema, data)
}

func validate(schema *jsonschema.Schema, data []byte) error {
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	if err := schema.Validate(doc); err != nil {
		return fmt.Errorf("schema validation: %w", err)
	}
	return nil
}
