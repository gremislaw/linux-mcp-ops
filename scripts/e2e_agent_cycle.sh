#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BROKERS="${BROKERS:-localhost:9092}"
DELAY="${DELAY:-2}"
BIN="$ROOT/bin/orchestrator"
SMOKE="$ROOT/bin/kafka-smoke"

if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT" && go build -o bin/orchestrator ./cmd/orchestrator)
fi
if [[ ! -x "$SMOKE" ]]; then
  (cd "$ROOT" && go build -o bin/kafka-smoke ./cmd/kafka-smoke)
fi

echo "==> Starting orchestrator"
USE_OLLAMA="${USE_OLLAMA:-0}" KAFKA_BROKERS="$BROKERS" "$BIN" > /tmp/e2e-orchestrator.log 2>&1 &
ORCH_PID=$!
trap 'kill $ORCH_PID 2>/dev/null || true' EXIT
sleep 2

echo "==> Sending orchestrator.requests"
CORR=$("$SMOKE" -brokers "$BROKERS" 2>/dev/null | tail -1)
echo "correlation_id=$CORR"

echo "==> Waiting for agent.requests"
FOUND=""
for _ in $(seq 1 15); do
  FOUND=$(docker exec redops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic agent.requests --from-beginning \
    --timeout-ms 1000 --property print.key=true 2>/dev/null | grep "$CORR" | tail -1 || true)
  [[ -n "$FOUND" ]] && break
  sleep 1
done
[[ -n "$FOUND" ]] || { echo "FAIL: no agent.requests"; cat /tmp/e2e-orchestrator.log; exit 1; }

echo "OK agent.requests: $FOUND"
echo "$FOUND" | grep -q '"mode":"dry-run"' || { echo "FAIL: dry-run missing"; exit 1; }
echo "$FOUND" | grep -q '"intent":"diagnose_auth"' || echo "WARN: intent may differ with Ollama"

echo "==> Fake agent.responses in ${DELAY}s"
"$ROOT/scripts/fake_agent_response.sh" "$CORR" "$DELAY" >/dev/null

echo "==> Waiting for orchestrator.responses"
FINAL=""
for _ in $(seq 1 15); do
  FINAL=$(docker exec redops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic orchestrator.responses --from-beginning \
    --timeout-ms 1000 2>/dev/null | grep "$CORR" | tail -1 || true)
  [[ -n "$FINAL" ]] && break
  sleep 1
done
[[ -n "$FINAL" ]] || { echo "FAIL: no orchestrator.responses"; cat /tmp/e2e-orchestrator.log; exit 1; }

echo "OK orchestrator.responses: $FINAL"
echo "$FINAL" | grep -q '"text":' || { echo "FAIL: text missing"; exit 1; }
echo "$FINAL" | grep -q 'map\[' && { echo "FAIL: Go dump in text"; exit 1; }
echo "==> E2E passed"
