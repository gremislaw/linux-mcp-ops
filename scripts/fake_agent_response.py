#!/usr/bin/env python3
"""Отправляет фейковый ответ агента в agent.responses через delay секунд."""

import json
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    print("Install: pip install kafka-python", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <correlation_id> [delay_seconds=5] [bootstrap=localhost:9092]", file=sys.stderr)
        sys.exit(1)

    correlation_id = sys.argv[1]
    delay = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
    bootstrap = sys.argv[3] if len(sys.argv) > 3 else "localhost:9092"

    print(f"Waiting {delay}s before sending fake agent response for correlation_id={correlation_id}", file=sys.stderr)
    time.sleep(delay)

    message = {
        "id": str(uuid.uuid4()),
        "correlation_id": correlation_id,
        "agent_request_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "success",
        "payload": {
            "result": "nginx restarted on prod-01",
            "source": "fake-agent-script",
        },
    }

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    future = producer.send("agent.responses", key=correlation_id, value=message)
    future.get(timeout=10)
    producer.close()

    print(json.dumps(message, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
