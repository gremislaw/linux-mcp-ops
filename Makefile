.PHONY: build test e2e kafka-up kafka-down

build:
	go build -o bin/orchestrator ./cmd/orchestrator
	go build -o bin/kafka-smoke ./cmd/kafka-smoke
	go build -o bin/ollama-smoke ./cmd/ollama-smoke

test:
	go test ./...

e2e:
	./scripts/e2e_agent_cycle.sh

kafka-up:
	docker-compose up -d

kafka-down:
	docker-compose down -v

run:
	./bin/orchestrator
