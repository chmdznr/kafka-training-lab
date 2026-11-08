#!/usr/bin/env bash
# Sends a message that can never be processed; it must end in the DLT without
# blocking the main topic.
source "$(dirname "$0")/lib.sh"

wait_http "$ORDER_URL/health" order-service
cd "$ROOT"

json=$(node scripts/send-poison.js)
order_id=$(printf '%s' "$json" | json_field orderId)
event_id=$(printf '%s' "$json" | json_field eventId)
echo "orderId=$order_id"
echo "eventId=$event_id (amount negatif, tidak mungkin valid)"

# DLT is written after the retry topic retries twice (~3 detik).
node scripts/dlt-check.js "$event_id"

wait_sql "select count(*) from proyek_payments where order_id='$order_id'" "0" "baris payments untuk pesan rusak"
echo "selesai: pesan rusak ada di DLT dan tidak membuat pembayaran"
