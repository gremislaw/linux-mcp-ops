#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <correlation_id> [delay_seconds=5] [bootstrap=localhost:9092]" >&2
  exit 1
fi

CORRELATION_ID="$1"
DELAY="${2:-5}"
BOOTSTRAP="${3:-localhost:9092}"

echo "Waiting ${DELAY}s before sending fake agent response for correlation_id=${CORRELATION_ID}" >&2
sleep "$DELAY"

MSG=$(cat <<EOF
{
  "id": "$(uuidgen | tr '[:upper:]' '[:lower:]')",
  "correlation_id": "${CORRELATION_ID}",
  "agent_request_id": "$(uuidgen | tr '[:upper:]' '[:lower:]')",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "status": "success",
  "payload": {
    "result": "nginx restarted on prod-01",
    "source": "fake-agent-script"
  }
}
EOF
)

if docker ps --format '{{.Names}}' | grep -qx redops-kafka; then
  echo "$MSG" | docker exec -i redops-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic agent.responses
else
  echo "$MSG" | kcat -P -b "$BOOTSTRAP" -t agent.responses -k "$CORRELATION_ID"
fi

echo "$MSG"
