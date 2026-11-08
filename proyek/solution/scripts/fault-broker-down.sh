#!/usr/bin/env bash
# Stops all brokers, accepts an order anyway, then starts the brokers again.
# The order is durable in the outbox and the event is published afterwards.
source "$(dirname "$0")/lib.sh"

wait_http "$ORDER_URL/health" order-service
wait_kafka

echo "1) matikan ketiga broker"
"${COMPOSE[@]}" stop kafka-1 kafka-2 kafka-3 >/dev/null

echo "2) terima order saat Kafka mati"
order=$(post_order 175000 broker-test)
order_id=$(printf '%s' "$order" | json_field orderId)
echo "   orderId=$order_id"

echo "3) order tersimpan, event belum terkirim"
wait_sql "select count(*) from proyek_order_outbox where key='$order_id' and published_at is null" "1" "outbox belum terkirim"
assert_eq "$(curl -sf "$ORDER_URL/orders/$order_id" | json_field status)" "PENDING" "status order saat Kafka mati"

echo "4) nyalakan broker lagi dan tunggu cluster siap"
"${COMPOSE[@]}" start kafka-1 kafka-2 kafka-3 >/dev/null
wait_kafka 90

echo "5) poller mengirim event, pembayaran selesai"
wait_status "$order_id" PAID 120
wait_sql "select count(*) from proyek_order_outbox where key='$order_id' and published_at is not null" "1" "outbox terkirim"
wait_sql "select count(*) from proyek_payments where order_id='$order_id'" "1" "baris payments"
wait_sql "select count(*) from proyek_notifications where order_id='$order_id'" "1" "baris notifications"
echo "selesai: tidak ada order tanpa event dan tidak ada event tanpa order"
