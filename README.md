# redops-orchestrator

AI Orchestrator для автоматизации типовых задач на RedOS (Группа Московская Биржа).

Оркестратор принимает запросы от Telegram-бота через Kafka, определяет намерение пользователя (Ollama tool calling), отправляет задачу Linux Agent и возвращает человекочитаемый ответ боту.

## Архитектура

```
Telegram Bot → orchestrator.requests → AI Orchestrator → agent.requests
                                              ↑                ↓
                                   orchestrator.responses ← agent.responses
```

Оркестратор **не выполняет** команды в Linux — только маршрутизирует задачи агенту и форматирует ответ.

## Контракты Kafka (согласованы с Bot + Agent)

| Топик | Направление | Ключевые поля |
|-------|-------------|---------------|
| `orchestrator.requests` | Bot → Orchestrator | `request_id`, `correlation_id`, `chat_id`, `user_text`, `timestamp` |
| `agent.requests` | Orchestrator → Agent | `request_id`, `correlation_id`, `intent`, `payload`, `mode` |
| `agent.responses` | Agent → Orchestrator | `request_id`, `correlation_id`, `status`, `result`, `error` |
| `orchestrator.responses` | Orchestrator → Bot | `correlation_id`, `chat_id`, `text`, `timestamp` |
| `orchestrator.dlq` | ошибки | poison pills, validation failures |

JSON Schema: [`schemas/`](schemas/)

### Intent и mode

| Intent | Описание |
|--------|----------|
| `diagnose_auth` | Диагностика проблем входа (SSH, AD, Kerberos) |
| `setup_workstation` | Подготовка рабочей станции |

| Mode | Описание |
|------|----------|
| `dry-run` | План без выполнения (по умолчанию) |
| `exec` | Реальное выполнение (зарезервировано для подтверждения ботом) |

## Запуск

```bash
docker-compose up -d          # Kafka KRaft + init топиков
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
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown |

### Локальная разработка с Ollama

```bash
USE_OLLAMA=1 OLLAMA_BASE_URL=http://127.0.0.1:11434 ./bin/orchestrator
./bin/ollama-smoke -text "Пользователь не заходит по SSH"
```

## Тестирование

```bash
make test                     # unit-тесты всех пакетов
make e2e                      # полный цикл с fake agent

# Отправить тестовый запрос от бота
./bin/kafka-smoke

# Эмуляция ответа агента через N секунд
./scripts/fake_agent_response.sh <correlation_id> 2
```

E2E проверяет цепочку: `orchestrator.requests` → `agent.requests` (dry-run) → fake `agent.responses` → `orchestrator.responses` с полем `text`.

## Структура проекта

```
cmd/orchestrator/     — точка входа сервиса
cmd/kafka-smoke/      — отправка тестового запроса в Kafka
cmd/ollama-smoke/     — проверка Ollama tool calling
internal/config/      — конфигурация из env
internal/models/      — контракты сообщений
internal/kafka/       — producer, consumer, schema validation, DLQ
internal/llm/         — Ollama /api/chat с tool calling
internal/tracker/     — async ожидание agent.responses по correlation_id
internal/router/      — бизнес-логика обработки запроса
internal/agent/       — диспетчеризация в agent.requests
internal/botfmt/      — форматирование ответа боту, маскирование секретов
schemas/              — JSON Schema контрактов
scripts/              — init топиков, fake agent, E2E
```

## Компромиссы и недочёты

Текущая реализация — MVP для интеграции с Bot и Linux Agent. Осознанные ограничения:

| Область | Компромисс |
|---------|------------|
| Request tracker | In-memory `map[correlation_id]chan`. При рестарте оркестратора ожидающие запросы теряются. Нет горизонтального масштабирования. |
| LLM по умолчанию | `USE_OLLAMA=0` — stub, всегда возвращает `diagnose_auth`. Для prod нужен Ollama или другой провайдер. |
| Режим `exec` | Отправляется только `dry-run`. Подтверждение пользователя через бота и переключение в `exec` не реализованы. |
| Persistence | Нет БД, нет audit log. Состояние только в Kafka и памяти процесса. |
| DLQ | Poison messages пропускаются при недоступном DLQ-producer; offset commit всё равно выполняется. |
| Schema validation | Валидация на стороне consumer; нет Schema Registry / Avro. |
| Безопасность | Маскирование секретов — regex по ключевым словам, не полноценный DLP. |
| Observability | Только `slog`. Нет метрик (Prometheus), трейсинга, health/readiness endpoints. |
| Single instance | Один consumer group member; несколько реплик потребуют sticky routing или внешний tracker (Redis). |
| Таймауты | Фиксированный `AGENT_WAIT_TIMEOUT`; нет retry/backoff для agent.requests. |

## Будущие планы

1. **Redis request tracker** — shared state для нескольких реплик оркестратора, переживание рестартов.
2. **Подтверждение `exec`** — двухшаговый сценарий: dry-run → кнопка в боте → повторный запрос с `mode=exec`.
3. **Metrics и health** — `/healthz`, `/readyz`, Prometheus (latency LLM, agent wait, DLQ rate).
4. **K8s manifests** — Deployment, ConfigMap, Secret, liveness/readiness probes.
5. **Schema Registry** — централизованные схемы, совместимость версий контрактов.
6. **Retry и idempotency** — dedup по `request_id`, повторная отправка в agent при transient errors.
7. **Расширение intents** — новые tool definitions в Ollama по мере появления сценариев Agent.
8. **Integration tests** — CI с Testcontainers (Kafka) и mock Ollama.

## Лицензия

Внутренний проект Группы Московская Биржа.
