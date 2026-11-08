#!/usr/bin/env bash
# Helper functions for the fault-injection scripts. Source this file, do not
# execute it:  source "$(dirname "$0")/lib.sh"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/.run"
LAB_ROOT="$(cd "$ROOT/../.." && pwd)"
COMPOSE_FILE="$LAB_ROOT/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

ORDER_URL="http://localhost:3000"
PAYMENT_URL="http://localhost:3001"
NOTIFICATION_URL="http://localhost:3002"

mkdir -p "$RUN"

json_field() {
  node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{const j=JSON.parse(d);console.log(j['$1'] ?? '')})"
}

post_order() {
  local amount="${1:-125000}"
  local customer="${2:-cust-fault}"
  curl -sf -X POST "$ORDER_URL/orders" \
    -H 'content-type: application/json' \
    -H "x-correlation-id: fault-$(date +%s)" \
    -d "{\"customerId\":\"$customer\",\"amount\":$amount,\"currency\":\"IDR\"}"
}

sql() {
  "${COMPOSE[@]}" exec -T postgres psql -U app -d shop -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

assert_eq() {
  local actual="$1" expected="$2" label="$3"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL: $label (dapat '$actual', harusnya '$expected')"
    exit 1
  fi
  echo "OK: $label = $expected"
}

wait_sql() {
  local query="$1" expected="$2" label="$3" tries="${4:-60}"
  local actual=""
  for _ in $(seq 1 "$tries"); do
    actual=$(sql "$query")
    if [ "$actual" = "$expected" ]; then
      echo "OK: $label = $expected"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $label (dapat '$actual', harusnya '$expected')"
  exit 1
}

wait_http() {
  # A consumer group that lost a member needs up to one session timeout before
  # the replacement can join, so allow 60 seconds by default.
  local url="$1" name="$2" tries="${3:-120}"
  for _ in $(seq 1 "$tries"); do
    if curl -sf "$url" >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "FAIL: $name tidak siap di $url"
  exit 1
}

wait_status() {
  local order_id="$1" expected="$2" tries="${3:-60}"
  local status=""
  for _ in $(seq 1 "$tries"); do
    status=$(curl -sf "$ORDER_URL/orders/$order_id" | json_field status || true)
    if [ "$status" = "$expected" ]; then
      echo "OK: order $order_id berstatus $expected"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: order $order_id berakhir '$status', harusnya '$expected'"
  return 1
}

wait_log() {
  local name="$1" pattern="$2" tries="${3:-60}"
  for _ in $(seq 1 "$tries"); do
    if grep -q "$pattern" "$RUN/$name.log" 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "FAIL: $name log tidak memuat '$pattern'"
  exit 1
}

wait_kafka() {
  local tries="${1:-60}"
  for _ in $(seq 1 "$tries"); do
    if "${COMPOSE[@]}" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "FAIL: cluster Kafka tidak siap"
  exit 1
}

start_proc() {
  local name="$1"
  shift
  # The subshell execs the process, so $! is the node pid and stop_proc can
  # kill it. Backgrounding an AND-list instead gives the subshell pid, leaving
  # node orphaned after stop.
  (
    cd "$ROOT" || exit 1
    exec nohup "$@" >"$RUN/$name.log" 2>&1
  ) &
  echo $! >"$RUN/$name.pid"
  sleep 0.3
  echo "$name pid $(cat "$RUN/$name.pid")"
}

stop_proc() {
  local name="$1"
  if [ ! -f "$RUN/$name.pid" ]; then
    return 0
  fi
  local pid
  pid=$(cat "$RUN/$name.pid")
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 30); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.3
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$RUN/$name.pid"
}
