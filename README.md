# redops-orchestrator

AI Orchestrator для автоматизации типовых задач на RedOS (Группа Московская Биржа).

## Архитектура

```
Telegram Bot → orchestrator.requests → AI Orchestrator → agent.requests
                                              ↑                ↓
                                   orchestrator.responses ← agent.responses
```

Оркестратор **не выполняет** команды в Linux — только маршрутизирует задачи агенту.

## Контракты Kafka (согласованы с Bot + Agent)

| Топик | Направление | Схема |
|-------|-------------|-------|
| `orchestrator.requests` | Bot → Orchestrator | `request_id`, `correlation_id`, `chat_id`, `user_text`, `timestamp` |
| `agent.requests` | Orchestrator → Agent | `request_id`, `correlation_id`, `intent`, `payload`, `mode` |
| `agent.responses` | Agent → Orchestrator | `request_id`, `correlation_id`, `status`, `result`, `error` |
| `orchestrator.responses` | Orchestrator → Bot | `correlation_id`, `chat_id`, `text`, `timestamp` |
| `orchestrator.dlq` | ошибки | poison pills, validation failures |

JSON Schema: [`schemas/`](schemas/)

## Запуск

```bash
docker-compose up -d          # Kafka + топики
make build
./bin/orchestrator            # основной сервис
```

### Переменные окружения

| Переменная | Default | Описание |
|------------|---------|----------|
| `KAFKA_BROKERS` | `localhost:9092` | Брокеры Kafka |
| `USE_OLLAMA` | `0` | `1` — реальный Ollama вместо stub |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Адрес Ollama (K8s) |
| `OLLAMA_MODEL` | `qwen2.5:7b` | Модель |
| `OLLAMA_TIMEOUT` | `10s` | Таймаут HTTP к Ollama |
| `AGENT_WAIT_TIMEOUT` | `60s` | Ожидание ответа агента |

## Тестирование

```bash
make test
make e2e                      # полный цикл с fake agent

# Отправить запрос от бота
./bin/kafka-smoke

# Эмуляция ответа агента через 2 сек
./scripts/fake_agent_response.sh <correlation_id> 2

# Ollama tool calling
./bin/ollama-smoke -text "Пользователь не заходит по SSH"
```

## Структура

```
cmd/orchestrator/     — точка входа
internal/config/      — конфигурация
internal/models/      — контракты сообщений
internal/kafka/       — producer/consumer/DLQ
internal/llm/         — Ollama tool calling
internal/tracker/     — async request tracker
internal/router/      — бизнес-логика
internal/agent/       — диспетчеризация в agent.requests
internal/botfmt/      — форматирование ответа боту
schemas/              — JSON Schema
```
