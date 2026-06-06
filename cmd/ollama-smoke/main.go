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

	text := flag.String("text", "Пользователь не заходит по SSH", "User message for Ollama")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	resp, err := llm.CallOllama(ctx, *text)
	if err != nil {
		log.Fatalf("call ollama: %v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		log.Fatalf("encode response: %v", err)
	}
}
