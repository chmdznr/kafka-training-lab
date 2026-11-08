#!/usr/bin/env bash
# Kills payment-service with SIGKILL while a message is being processed, then
# shows that the order still ends PAID with exactly one payment.
source "$(dirname "$0")/lib.sh"

wait_http "$ORDER_URL/health" order-service
wait_http "$PAYMENT_URL/health" payment-service

echo "1) payment dijalankan ulang dengan handler lambat (3 detik)"
stop_proc payment
start_proc payment env PROCESSING_DELAY_MS=3000 node dist/payment/main.js
wait_http "$PAYMENT_URL/health" payment-service

echo "2) buat order, lalu bunuh payment saat pesan sedang diproses"
order=$(post_order 250000 crash-test)
order_id=$(printf '%s' "$order" | json_field orderId)
echo "   orderId=$order_id"
sleep 1.2
pid=$(cat "$RUN/payment.pid")
kill -9 "$pid"
rm -f "$RUN/payment.pid"
echo "   payment pid $pid dibunuh dengan SIGKILL"

echo "3) nyalakan payment lagi tanpa delay"
sleep 0.5
start_proc payment node dist/payment/main.js
wait_http "$PAYMENT_URL/health" payment-service
wait_status "$order_id" PAID 60

echo "4) verifikasi efeknya tepat sekali"
wait_sql "select count(*) from proyek_payments where order_id='$order_id'" "1" "baris payments"
wait_sql "select count(*) from proyek_notifications where order_id='$order_id'" "1" "baris notifications"
wait_sql "select count(*) from proyek_processed_events where event_id=(select event_id from proyek_payments where order_id='$order_id' limit 1)" "1" "baris proyek_processed_events"
echo "selesai: crash tidak menggandakan pembayaran dan order tetap PAID"
