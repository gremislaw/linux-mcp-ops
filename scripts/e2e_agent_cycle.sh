#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BROKERS="${BROKERS:-localhost:9092}"
DELAY="${DELAY:-2}"
BIN="$ROOT/bin/kafka-smoke"

if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT" && go build -o bin/kafka-smoke ./cmd/kafka-smoke)
fi

echo "==> Starting orchestrator (background)"
"$BIN" -mode consume -brokers "$BROKERS" > /tmp/e2e-orchestrator.log 2>&1 &
ORCH_PID=$!
trap 'kill $ORCH_PID 2>/dev/null || true' EXIT
sleep 2

echo "==> Sending orchestrator.requests"
CORR=$("$BIN" -mode produce-orchestrator -brokers "$BROKERS" 2>/dev/null | tail -1)
echo "correlation_id=$CORR"

echo "==> Waiting for agent.requests (up to 15s)"
FOUND=""
for _ in $(seq 1 15); do
  MSG=$(docker exec redops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic agent.requests \
    --from-beginning \
    --timeout-ms 1000 \
    --property print.key=true 2>/dev/null | grep "$CORR" | tail -1 || true)
  if [[ -n "$MSG" ]]; then
    FOUND="$MSG"
    break
  fi
  sleep 1
done

if [[ -z "$FOUND" ]]; then
  echo "FAIL: no message in agent.requests for correlation_id=$CORR" >&2
  cat /tmp/e2e-orchestrator.log >&2
  exit 1
fi

echo "OK agent.requests: $FOUND"
echo "$FOUND" | grep -q '"mode":"dry-run"' || { echo "FAIL: expected mode dry-run in agent.requests"; exit 1; }
echo "$FOUND" | grep -q '"tool":"diagnose_auth"' || echo "WARN: diagnose_auth tool not found (ollama may pick another tool)"
echo "$FOUND" | grep -q '"request_id"' || { echo "FAIL: request_id missing"; exit 1; }
echo "$FOUND" | grep -q "$CORR" || { echo "FAIL: correlation_id mismatch"; exit 1; }

echo "==> Sending fake agent.responses in ${DELAY}s"
"$ROOT/scripts/fake_agent_response.sh" "$CORR" "$DELAY" "$BROKERS" >/dev/null

echo "==> Waiting for orchestrator.responses"
FINAL=""
for _ in $(seq 1 15); do
  FINAL=$(docker exec redops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic orchestrator.responses \
    --from-beginning \
    --timeout-ms 1000 2>/dev/null | grep "$CORR" | tail -1 || true)
  if [[ -n "$FINAL" ]]; then
    break
  fi
  sleep 1
done

if [[ -z "$FINAL" ]]; then
  echo "FAIL: no orchestrator.responses for correlation_id=$CORR" >&2
  cat /tmp/e2e-orchestrator.log >&2
  exit 1
fi

echo "OK orchestrator.responses: $FINAL"
echo "$FINAL" | grep -q '"status":"success"' || { echo "FAIL: expected success status"; exit 1; }
echo "==> E2E cycle passed"
