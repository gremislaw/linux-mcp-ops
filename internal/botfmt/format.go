package botfmt

import (
	"fmt"
	"strings"
	"time"

	"redops/internal/kafka"
)

func FormatAgentResponse(resp kafka.AgentResponse) string {
	switch resp.Status {
	case "success":
		return formatSuccess(resp.Payload)
	case "error":
		return formatError(resp.Error)
	default:
		return fmt.Sprintf("Неизвестный статус ответа агента: %s", resp.Status)
	}
}

func FormatTimeout(wait time.Duration) string {
	return fmt.Sprintf("⏱ Агент не ответил в течение %s. Попробуйте позже.", wait)
}

func formatSuccess(payload map[string]any) string {
	if len(payload) == 0 {
		return "✅ Задача выполнена."
	}

	if text := stringField(payload, "result", "message", "summary", "output"); text != "" {
		return "✅ " + text
	}

	if logs, ok := payload["logs"]; ok {
		if formatted := formatLogs(logs); formatted != "" {
			return formatted
		}
	}

	if validation, ok := payload["validation_errors"]; ok {
		if formatted := formatValidationErrors(validation); formatted != "" {
			return formatted
		}
	}

	if details := summarizePayload(payload); details != "" {
		return "✅ " + details
	}

	return "✅ Задача выполнена."
}

func formatError(err *kafka.ErrorDetail) string {
	if err == nil {
		return "❌ Произошла ошибка при выполнении задачи."
	}
	return fmt.Sprintf("❌ [%s] %s", err.Code, err.Message)
}

func formatLogs(raw any) string {
	lines := make([]string, 0)

	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				lines = append(lines, s)
			}
		}
	case []string:
		for _, s := range v {
			if strings.TrimSpace(s) != "" {
				lines = append(lines, s)
			}
		}
	}

	if len(lines) == 0 {
		return ""
	}
	if len(lines) == 1 {
		return "✅ " + lines[0]
	}
	return "✅ Выполнение завершено:\n" + strings.Join(lines, "\n")
}

func formatValidationErrors(raw any) string {
	lines := make([]string, 0)

	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			switch entry := item.(type) {
			case string:
				lines = append(lines, entry)
			case map[string]any:
				field := stringField(entry, "field", "path", "name")
				msg := stringField(entry, "message", "error", "detail")
				if field != "" && msg != "" {
					lines = append(lines, fmt.Sprintf("%s: %s", field, msg))
				} else if msg != "" {
					lines = append(lines, msg)
				}
			}
		}
	case map[string]any:
		for field, value := range v {
			lines = append(lines, fmt.Sprintf("%s: %v", field, value))
		}
	}

	if len(lines) == 0 {
		return ""
	}
	return "⚠️ Ошибки валидации:\n" + strings.Join(lines, "\n")
}

func summarizePayload(payload map[string]any) string {
	skip := map[string]bool{
		"source": true, "raw": true, "debug": true,
	}

	parts := make([]string, 0, 3)
	for key, value := range payload {
		if skip[key] {
			continue
		}
		switch v := value.(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				parts = append(parts, fmt.Sprintf("%s: %s", key, v))
			}
		case float64, bool, int, int64:
			parts = append(parts, fmt.Sprintf("%s: %v", key, v))
		}
		if len(parts) >= 3 {
			break
		}
	}

	return strings.Join(parts, "; ")
}

func stringField(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
