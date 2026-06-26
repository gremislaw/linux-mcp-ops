#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="/opt/kafka/bin"
RETENTION_MS=604800000

topics=(
  "orchestrator.requests"
  "orchestrator.responses"
  "orchestrator.dlq"
  "agent.requests"
  "agent.responses"
)

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

for topic in "${topics[@]}"; do
  if "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${topic}"; then
    echo "Topic ${topic} already exists, skipping."
    continue
  fi

  echo "Creating topic ${topic}..."
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms="${RETENTION_MS}" \
    --config min.insync.replicas=1
done

echo "Topic configuration:"
for topic in "${topics[@]}"; do
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}"
done

echo "Kafka topics initialized."
