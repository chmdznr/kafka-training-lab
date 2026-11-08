#!/usr/bin/env bash
# Lab 01 - prove the leader moves and messages stay readable when a broker dies.
set -euo pipefail

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$LAB_ROOT"

KAFKA_BIN=/opt/kafka/bin
CANONICAL_TOPIC=order.created.v1
TOPIC=lab01.orders.v1
STATE_TOPIC=order.state.v1
EXEC_BROKER=kafka-1
BOOTSTRAP=kafka-1:29092
RUN_ID="lab01-$(date +%s)-$RANDOM"

kafka() {
  docker compose exec -T "$EXEC_BROKER" "$KAFKA_BIN/$1" --bootstrap-server "$BOOTSTRAP" "${@:2}"
}

describe_topic() {
  kafka kafka-topics.sh --describe --topic "$TOPIC" 2>/dev/null || true
}

leader_of() {
  describe_topic | grep -E "Partition: $1[^0-9]" | sed -n 's/.*Leader: \([0-9][0-9]*\).*/\1/p' | head -1
}

isr_size() {
  local isr
  isr=$(describe_topic | grep -E "Partition: $1[^0-9]" | sed -n 's/.*Isr: \([0-9,]*\).*/\1/p' | head -1)
  [ -z "$isr" ] && { echo 0; return; }
  echo "$isr" | awk -F, '{print NF}'
}

total_for() {
  local topic=$1
  docker compose exec -T "$EXEC_BROKER" "$KAFKA_BIN/kafka-get-offsets.sh" \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic" --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

topic_total() {
  total_for "$TOPIC"
}

read_all_messages() {
  local total
  total=$(topic_total)
  docker compose exec -T "$EXEC_BROKER" "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --from-beginning \
    --max-messages "$total" --timeout-ms 30000 2>/dev/null || true
}

produce_lines() {
  docker compose exec -T "$EXEC_BROKER" "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    --reader-property parse.key=true --reader-property key.separator=: >/dev/null
}

echo "== 1. Pastikan cluster siap"
docker compose up -d >/dev/null
for _ in $(seq 1 60); do
  if docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-broker-api-versions.sh" \
      --bootstrap-server kafka-1:29092 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-metadata-quorum.sh" \
  --bootstrap-server kafka-1:29092 describe --status | sed -n '1,7p'

echo
echo "== 2. Buat topic kanonik $CANONICAL_TOPIC (kosong) dan sandbox $TOPIC"
kafka kafka-topics.sh --create --if-not-exists --topic "$CANONICAL_TOPIC" \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka kafka-topics.sh --describe --topic "$TOPIC"
CANON_TOTAL=$(total_for "$CANONICAL_TOPIC")
echo "INFO  $CANONICAL_TOPIC berisi $CANON_TOTAL record (topic kanonik untuk event kontrak)"

echo
echo "== 3. Produce 6 pesan dengan key"
{
  for seq in 1 2; do
    for key in 1 2 3; do
      printf 'order-%s:{"runId":"%s","orderId":"order-%s","seq":%s}\n' \
        "$key" "$RUN_ID" "$key" "$seq"
    done
  done
} | produce_lines

echo
echo "== 4. Baca pesan dari awal"
CONSUMED=$(read_all_messages | grep -c "$RUN_ID" || true)
[ "$CONSUMED" -ge 6 ] && echo "PASS  $CONSUMED pesan milik run ini terbaca" \
  || { echo "FAIL  hanya $CONSUMED pesan milik run ini terbaca"; exit 1; }

echo
echo "== 5. Fault injection: matikan leader partisi 0"
OLD_LEADER=$(leader_of 0)
[ -n "$OLD_LEADER" ] || { echo "FAIL  leader partisi 0 tidak terbaca"; exit 1; }
STOPPED="kafka-$OLD_LEADER"
if [ "$STOPPED" = kafka-1 ]; then EXEC_BROKER=kafka-2; else EXEC_BROKER=kafka-1; fi
BOOTSTRAP="$EXEC_BROKER:29092"
echo "Leader partisi 0: $STOPPED. Perintah berikutnya lewat $EXEC_BROKER ($BOOTSTRAP)."

docker compose stop "$STOPPED" >/dev/null
echo "Menunggu leader berpindah..."
NEW_LEADER=""
for _ in $(seq 1 60); do
  NEW_LEADER=$(leader_of 0)
  if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$OLD_LEADER" ]; then break; fi
  sleep 1
done
[ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$OLD_LEADER" ] \
  || { echo "FAIL  leader tidak berpindah"; docker compose start "$STOPPED" >/dev/null; exit 1; }
echo "PASS  leader partisi 0: $OLD_LEADER -> $NEW_LEADER"

for _ in $(seq 1 30); do
  [ "$(isr_size 0)" = 2 ] && break
  sleep 1
done
echo "PASS  Isr partisi 0 setelah broker mati: $(isr_size 0) broker"

CONSUMED_AFTER=$(read_all_messages | grep -c "$RUN_ID" || true)
[ "$CONSUMED_AFTER" -ge 6 ] && echo "PASS  semua pesan tetap terbaca ($CONSUMED_AFTER/6)" \
  || { echo "FAIL  pesan hilang ($CONSUMED_AFTER/6)"; docker compose start "$STOPPED" >/dev/null; exit 1; }

echo
echo "== 6. Nyalakan broker dan tunggu ISR pulih"
docker compose start "$STOPPED" >/dev/null
for _ in $(seq 1 90); do
  [ "$(isr_size 0)" = 3 ] && break
  sleep 1
done
ISR=$(isr_size 0)
[ "$ISR" = 3 ] && echo "PASS  Isr partisi 0 pulih: 3 broker" \
  || { echo "FAIL  Isr partisi 0 = $ISR"; exit 1; }

echo
echo "== 7. Buat topic compacted $STATE_TOPIC"
kafka kafka-topics.sh --create --if-not-exists --topic "$STATE_TOPIC" \
  --partitions 3 --replication-factor 3 --config cleanup.policy=compact >/dev/null 2>&1 || true
kafka kafka-topics.sh --alter --topic "$STATE_TOPIC" --add-config cleanup.policy=compact >/dev/null 2>&1 || true
CONFIG=$(kafka kafka-topics.sh --describe --topic "$STATE_TOPIC" | grep -o 'cleanup.policy=[a-z,]*' | head -1)
[ "$CONFIG" = "cleanup.policy=compact" ] && echo "PASS  $STATE_TOPIC $CONFIG" \
  || { echo "FAIL  cleanup.policy tidak terpasang ($CONFIG)"; exit 1; }

echo
CANON_TOTAL=$(total_for "$CANONICAL_TOPIC")
if [ "$CANON_TOTAL" -eq 0 ]; then
  echo "PASS  $CANONICAL_TOPIC tetap kosong; lalu lintas uji hanya di $TOPIC"
else
  echo "INFO  $CANONICAL_TOPIC berisi $CANON_TOTAL record dari lab kontrak, bukan dari Lab 01"
fi

echo
echo "PASS lab01: leader berpindah, pesan tetap terbaca, ISR pulih, topic compacted dibuat"
