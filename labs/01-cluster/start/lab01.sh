#!/usr/bin/env bash
# Lab 01 — titik awal. Lengkapi bagian LATIHAN, lalu jalankan skrip ini.
set -euo pipefail

# Git Bash on Windows rewrites arguments like /opt/kafka/bin/... into Windows
# paths before docker sees them. Harmless everywhere else.
export MSYS_NO_PATHCONV=1

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$LAB_ROOT"

KAFKA_BIN=/opt/kafka/bin
CANONICAL_TOPIC=order.created.v1
TOPIC=lab01.orders.v1
STATE_TOPIC=order.state.v1
BOOTSTRAP=kafka-1:29092

latihan() {
  printf '\nLATIHAN: %s\n' "$1" >&2
  exit 1
}

echo "== 1. Periksa quorum KRaft"
docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-metadata-quorum.sh" \
  --bootstrap-server "$BOOTSTRAP" describe --status

echo
echo "== 2. Buat topic kanonik $CANONICAL_TOPIC (kosong) dan sandbox $TOPIC"
# LATIHAN: buat dua topic, masing-masing 3 partisi, replication factor 3,
# min.insync.replicas=2. Jangan menulis pesan uji ke $CANONICAL_TOPIC.
# docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
#   --create --topic "$CANONICAL_TOPIC" ... && ... --topic "$TOPIC" ...
latihan "lengkapi pembuatan $CANONICAL_TOPIC (kosong) dan $TOPIC (sandbox uji)"

echo
echo "== 3. Produce pesan dengan key ke $TOPIC"
# LATIHAN: kirim pesan dengan key ke $TOPIC (bukan $CANONICAL_TOPIC).
# Gunakan --reader-property parse.key=true dan --reader-property key.separator=:.
# docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-console-producer.sh" ...
latihan "lengkapi perintah produce dengan key (reader-property parse.key=true dan key.separator=:)"
echo
echo "== 4. Lihat leader dan ISR $TOPIC"
# LATIHAN: tampilkan --describe untuk $TOPIC (sandbox), catat leader partisi 0.
# docker compose exec -T kafka-1 "$KAFKA_BIN/kafka-topics.sh" ...
latihan "lengkapi perintah --describe dan catat leader partisi 0"
echo
echo "== 5. Fault injection pada $TOPIC"
# LATIHAN: matikan broker yang menjadi leader partisi 0 di $TOPIC, lalu describe lagi
# sampai leader berpindah dan Isr tinggal 2. Baca ulang semua pesan.
# docker compose stop kafka-N
# docker compose start kafka-N
latihan "lengkapi fault injection: matikan leader partisi 0, amati leader dan ISR baru, lalu nyalakan lagi"
echo
echo "== 6. Topic compacted"
# LATIHAN: buat $STATE_TOPIC dengan --config cleanup.policy=compact.
latihan "lengkapi pembuatan topic compacted $STATE_TOPIC"