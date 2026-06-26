package llm

import (
	"encoding/json"
	"fmt"

	"redops/schemas/tools"
)

type ollamaTool struct {
	Type     string         `json:"type"`
	Function ollamaFunction `json:"function"`
}

type ollamaFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

func agentTools() ([]ollamaTool, error) {
	definitions := []string{"diagnose_auth", "setup_workstation"}
	result := make([]ollamaTool, 0, len(definitions))

	for _, name := range definitions {
		raw, err := tools.FS.ReadFile(name + ".schema.json")
		if err != nil {
			return nil, fmt.Errorf("read tool schema %s: %w", name, err)
		}

		var schema struct {
			Description string         `json:"description"`
			Properties  map[string]any `json:"properties"`
			Required    []string       `json:"required"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			return nil, fmt.Errorf("parse tool schema %s: %w", name, err)
		}

		parameters, err := json.Marshal(map[string]any{
			"type":                 "object",
			"properties":           schema.Properties,
			"required":             schema.Required,
			"additionalProperties": false,
		})
		if err != nil {
			return nil, err
		}

		result = append(result, ollamaTool{
			Type: "function",
			Function: ollamaFunction{
				Name:        name,
				Description: schema.Description,
				Parameters:  parameters,
			},
		})
	}

	return result, nil
}
