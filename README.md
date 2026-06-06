# linux-mcp-ops

MCP-сервер для автоматизации типовых задач с Linux.

## Kafka-инфраструктура (задача 1)

### Запуск кластера

```bash
docker-compose up -d
```

Kafka (KRaft, без Zookeeper) поднимается на `localhost:9092`. Топики создаются автоматически через сервис `kafka-init`:

| Топик | Назначение | Retention |
|-------|------------|-----------|
| `tg.requests` | Входящие запросы от Telegram-бота | 7 дней |
| `worker.responses` | Ответы воркеров | 7 дней |
| `worker.dlq` | Dead Letter Queue | 7 дней |

### Контракты сообщений

JSON Schema: [`schemas/`](schemas/)

- `tg.requests.schema.json` — запрос пользователя
- `worker.responses.schema.json` — ответ воркера
- `worker.dlq.schema.json` — сообщение в DLQ после исчерпания retry

OpenAPI-обёртка: `schemas/openapi.yaml`

### Go producer / consumer

Пакет [`internal/kafka`](internal/kafka/):

- **Producer**: `acks=all`, идемпотентный режим, валидация по JSON Schema перед отправкой
- **Consumer**: `auto.offset.reset=latest`, ручной commit, retry (3 попытки) → DLQ

```bash
go build -o bin/kafka-smoke ./cmd/kafka-smoke

# Терминал 1 — консьюмер
./bin/kafka-smoke -mode consume

# Терминал 2 — продюсер
./bin/kafka-smoke -mode produce
```

### Тестирование

```bash
# Проверка топиков (репликация, retention)
make kafka-topics

# Console producer / consumer
make kafka-produce
make kafka-consume

# Проверка retry и DLQ: остановить брокер во время consume,
# затем поднять снова — после 3 retry сообщение попадёт в worker.dlq
docker-compose stop kafka
# ... отправить сообщение ...
docker-compose start kafka
```

### Остановка

```bash
docker-compose down -v
```
