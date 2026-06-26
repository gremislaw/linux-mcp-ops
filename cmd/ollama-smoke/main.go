package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"redops/internal/llm"
)

func main() {
	log.SetOutput(os.Stderr)

	text := flag.String("text", "Пользователь не заходит по SSH", "User message")
	baseURL := flag.String("ollama", "http://127.0.0.1:11434", "Ollama base URL")
	model := flag.String("model", "qwen2.5:7b", "Ollama model")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	resp, err := llm.CallOllama(ctx, llm.Config{
		BaseURL: *baseURL,
		Model:   *model,
	}, *text)
	if err != nil {
		log.Fatalf("call ollama: %v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		log.Fatalf("encode: %v", err)
	}
}
