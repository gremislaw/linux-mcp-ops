package botfmt

import (
	"fmt"
	"regexp"
	"strings"

	"redops/internal/kafka"
	"redops/internal/models"
)

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|secret|token|api_key)\s*[:=]\s*\S+`),
	regexp.MustCompile(`(?i)domain_admin_password\s*[:=]\s*\S+`),
}

func FormatAgentResponse(resp kafka.AgentResponse) string {
	switch resp.Status {
	case models.AgentStatusSuccess:
		return formatSuccess(resp.Result)
	case models.AgentStatusValidationError:
		if resp.Error != nil && *resp.Error != "" {
			return "⚠️ Ошибка валидации: " + MaskSecrets(*resp.Error)
		}
		return "⚠️ Ошибка валидации параметров."
	case models.AgentStatusError:
		if resp.Error != nil && *resp.Error != "" {
			return "❌ " + MaskSecrets(*resp.Error)
		}
		return "❌ Произошла ошибка при выполнении задачи."
	default:
		return fmt.Sprintf("Неизвестный статус ответа агента: %s", resp.Status)
	}
}

func FormatTimeout() string {
	return models.MsgAgentTimeout
}

func FormatUnrecognized() string {
	return models.MsgUnrecognizedRequest
}

func MaskSecrets(text string) string {
	out := text
	for _, pattern := range secretPatterns {
		out = pattern.ReplaceAllString(out, "[REDACTED]")
	}
	return out
}

func formatSuccess(result map[string]any) string {
	if len(result) == 0 {
		return "✅ Задача выполнена."
	}

	if text := stringField(result, "message", "summary", "output", "result"); text != "" {
		return "✅ " + MaskSecrets(text)
	}

	if logs, ok := result["logs"]; ok {
		if formatted := formatLogs(logs); formatted != "" {
			return MaskSecrets(formatted)
		}
	}

	if details := summarizeResult(result); details != "" {
		return "✅ " + MaskSecrets(details)
	}

	return "✅ Задача выполнена."
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
		lines = append(lines, v...)
	}

	if len(lines) == 0 {
		return ""
	}
	if len(lines) == 1 {
		return "✅ " + lines[0]
	}
	return "✅ Выполнение завершено:\n" + strings.Join(lines, "\n")
}

func summarizeResult(result map[string]any) string {
	parts := make([]string, 0, 3)
	for key, value := range result {
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
