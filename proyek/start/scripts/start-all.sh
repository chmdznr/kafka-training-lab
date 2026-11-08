#!/usr/bin/env bash
# Starts the four processes in the background, logs and pids under .run/.
source "$(dirname "$0")/lib.sh"

for name in order payment retry notification; do
  if [ -f "$RUN/$name.pid" ]; then
    echo "FAIL: $name masih berjalan (pid $(cat "$RUN/$name.pid")). Jalankan scripts/stop-all.sh dulu."
    exit 1
  fi
done

cd "$ROOT"
if [ ! -d dist ]; then
  npm run build
fi
npm run create-topics

start_proc payment env PROCESSING_DELAY_MS="${PROCESSING_DELAY_MS:-0}" node dist/payment/main.js
start_proc retry node dist/payment/retry-main.js
start_proc notification node dist/notification/main.js
sleep 1
start_proc order node dist/order/main.js

wait_http "$ORDER_URL/health" order-service
wait_http "$PAYMENT_URL/health" payment-service
wait_http "$NOTIFICATION_URL/health" notification-service
wait_log retry '"event":"ready"'

echo "semua service siap (log di $RUN/*.log)"
