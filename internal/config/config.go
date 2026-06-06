package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers      []string
	OllamaBaseURL     string
	OllamaModel       string
	OllamaTimeout     time.Duration
	AgentWaitTimeout  time.Duration
	ShutdownTimeout   time.Duration
	UseOllama         bool
	ConsumerGroup     string
	ResponseGroup     string
}

func Load() Config {
	return Config{
		KafkaBrokers:     splitEnv("KAFKA_BROKERS", "localhost:9092"),
		OllamaBaseURL:    strings.TrimRight(getEnv("OLLAMA_BASE_URL", "http://ollama:11434"), "/"),
		OllamaModel:      getEnv("OLLAMA_MODEL", "qwen2.5:7b"),
		OllamaTimeout:    durationEnv("OLLAMA_TIMEOUT", 10*time.Second),
		AgentWaitTimeout: durationEnv("AGENT_WAIT_TIMEOUT", 60*time.Second),
		ShutdownTimeout:  durationEnv("SHUTDOWN_TIMEOUT", 5*time.Second),
		UseOllama:        os.Getenv("USE_OLLAMA") == "1",
		ConsumerGroup:    getEnv("ORCHESTRATOR_GROUP", "redops-orchestrator"),
		ResponseGroup:    getEnv("ORCHESTRATOR_RESPONSE_GROUP", "redops-orchestrator-responses"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	raw := getEnv(key, fallback)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	if seconds, err := strconv.Atoi(raw); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	return fallback
}
