package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"redops/schemas"
)

var (
	telegramRequestSchema       *jsonschema.Schema
	orchestratorRequestSchema   *jsonschema.Schema
	orchestratorResponseSchema  *jsonschema.Schema
	agentRequestSchema          *jsonschema.Schema
	agentResponseSchema         *jsonschema.Schema
	workerResponseSchema        *jsonschema.Schema
	workerDLQSchema             *jsonschema.Schema
)

func init() {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	entries := map[string]**jsonschema.Schema{
		"tg.requests":              &telegramRequestSchema,
		"orchestrator.requests":      &orchestratorRequestSchema,
		"orchestrator.responses":     &orchestratorResponseSchema,
		"agent.requests":             &agentRequestSchema,
		"agent.responses":            &agentResponseSchema,
		"worker.responses":           &workerResponseSchema,
		"worker.dlq":                 &workerDLQSchema,
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

func ValidateTelegramRequest(data []byte) error {
	return validate(telegramRequestSchema, data)
}

func ValidateOrchestratorRequest(data []byte) error {
	return validate(orchestratorRequestSchema, data)
}

func ValidateAgentRequest(data []byte) error {
	return validate(agentRequestSchema, data)
}

func ValidateAgentResponse(data []byte) error {
	return validate(agentResponseSchema, data)
}

func ValidateOrchestratorResponse(data []byte) error {
	return validate(orchestratorResponseSchema, data)
}

func ValidateWorkerResponse(data []byte) error {
	return validate(workerResponseSchema, data)
}

func ValidateWorkerDLQ(data []byte) error {
	return validate(workerDLQSchema, data)
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
