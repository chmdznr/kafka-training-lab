#!/usr/bin/env bash
# Sends the same eventId twice and shows a single payment and notification.
source "$(dirname "$0")/lib.sh"

wait_http "$ORDER_URL/health" order-service
cd "$ROOT"

json=$(node scripts/send-duplicate.js)
order_id=$(printf '%s' "$json" | json_field orderId)
event_id=$(printf '%s' "$json" | json_field eventId)
echo "orderId=$order_id"
echo "eventId=$event_id (dikirim dua kali)"

wait_sql "select count(*) from proyek_payments where order_id='$order_id'" "1" "baris payments"
wait_sql "select count(*) from proyek_processed_events where event_id='$event_id'" "1" "baris proyek_processed_events"
wait_sql "select count(*) from proyek_notifications where order_id='$order_id'" "1" "baris notifications"
echo "selesai: duplikat diproses sekali"
