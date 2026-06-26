#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <correlation_id> [delay_seconds=2] [request_id=auto]" >&2
  exit 1
fi

CORRELATION_ID="$1"
DELAY="${2:-2}"
REQUEST_ID="${3:-$(uuidgen | tr '[:upper:]' '[:lower:]')}"

echo "Waiting ${DELAY}s before fake agent.responses for correlation_id=${CORRELATION_ID}" >&2
sleep "$DELAY"

MSG=$(printf '{"request_id":"%s","correlation_id":"%s","status":"success","result":{"message":"nginx restarted on prod-01"},"error":null}' \
  "$REQUEST_ID" "$CORRELATION_ID")

if docker ps --format '{{.Names}}' | grep -qx redops-kafka; then
  echo "$MSG" | docker exec -i redops-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic agent.responses
else
  echo "$MSG" | kcat -P -b localhost:9092 -t agent.responses -k "$CORRELATION_ID"
fi

echo "$MSG"
