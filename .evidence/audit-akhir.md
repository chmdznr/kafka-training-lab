# Audit akhir (2026-11-08)

Dijalankan dari salinan bersih repo (`rsync` tanpa `node_modules`, `dist`, `.git`) setelah `docker compose down -v`, berurutan seperti alur peserta.

| Langkah | Hasil |
|---|---|
| `./scripts/verify-env.sh` | PASS 13 / FAIL 0 (termasuk pull image Testcontainers) |
| Lab 01 `solution/lab01.sh` | PASS: leader berpindah, pesan tetap terbaca, ISR pulih, topic compacted |
| Lab 02 `ordering.js`, `durability.js` | PASS: 6 key x 10 pesan urut per key; ditolak `code=19` saat ISR < 2, kirim ulang tanpa duplikat |
| Lab 03 `envelope-check.js`, `publish-order.js` | PASS: 12 pemeriksaan; event tanpa `correlationId` ditolak |
| Lab 04 `assignments.js`, `rebalance.js`, `replay.js` | PASS: 3 aktif + 1 idle; classic moved=743 ms / unaffected=31 ms, consumer moved=2970 ms / unaffected=37 ms; lag 0 setelah replay |
| Lab 05 SIGTERM (`PROCESSING_DELAY_MS=3000`) | exit 143; `payment_events` 3/3 lalu 4/4 setelah restart (rows = distinct event_id); lag 0; hanya 1 `invalid-event` (event rusak Lab 03 yang disengaja; sejak 2026-11-11 menjadi 0 karena Lab 03 memakai topic sandbox) |
| Lab 06 duplikat, poison, replay | `processed` 1 + `duplicate-skipped` 1; poison `dead-lettered` dengan header `original-topic:order.created.v1`; replay 1x dan 2x tetap 1 baris |
| Lab 07 outbox saat 3 broker mati | 3 baris pending saat mati, 0 dalam 7 detik setelah hidup; `npm run check` HASIL OK |
| Lab 07 payment-tx, abort-demo | LAG payment-tx 0 di 3 partisi; `read_committed [committed]`, `read_uncommitted [aborted, committed]` |
| Lab 08 (`lab08.order.created.v1`) | v1 dan v2 diterima; old reader membaca 4 event; `double -> string` ditolak `TYPE_MISMATCH` |
| Lab 09 | `correlationId` dan `traceparent` sama di log order (1 baris) dan payment (2 baris); id dibuat otomatis bila header kosong; SASL salah exit 1 `invalid credentials`, benar exit 0 |
| Proyek fault scripts | consumer-crash, duplicate, poison, broker-down: semua OK |
| Proyek `npm test` | 1 pass (~27 detik) |

Temuan yang diperbaiki selama audit:

- `scripts/smoke.js`: produce langsung setelah membuat topic gagal `Unknown topic or partition` di cluster baru; sekarang menunggu leader dan ISR.
- Lab 01, 02, 04 menulis ~12 ribu pesan uji ke `order.created.v1`; dipindah ke topic sandbox.
- Lab 08 menulis Avro ke `order.created.v1`; dipindah ke `lab08.order.created.v1`.
- Lab 07 payment-tx melewati record tidak valid tanpa commit; sekarang offset-nya ikut transaksi sehingga LAG kembali 0.
- Lab 07, 08, 09 mendeklarasikan dependency yang membayangi `node_modules` root; diseragamkan.
- `avsc` dipakai langsung oleh Lab 08 tetapi hanya transitif; kini dideklarasikan di root.
- `kafka-console-consumer --property` sudah deprecated di Kafka 4.4; README memakai `--formatter-property`.
- README Lab 01 sampai 05: alur `start/` lebih dulu dan folder kerja yang benar.

## Run ulang 2026-11-11

| Langkah | Hasil |
|---|---|
| `verify-env.sh`, `npm run smoke` | PASS 13 / FAIL 0; PLAINTEXT dan SASL PASS |
| Lab 01 sampai 04 | Semua exit 0; rebalance classic 733 ms / 32 ms, consumer 2975 ms / 36 ms |
| Lab 05 | HTTP 202, SIGTERM exit 143, tanpa duplikat setelah restart |
| Lab 06 | dedup 1 baris; poison ke DLT; replay 2x tetap 1 baris |
| Lab 07 | pending 3 -> 0; `npm run check` HASIL OK; payment-tx lag 0; abort-demo OK |
| Lab 08 | langkah 1 sampai 7 sama; langkah 8 hapus field: BACKWARD `KOMPATIBEL`, FORWARD ditolak `READER_FIELD_MISSING_DEFAULT_VALUE`, versi `[1,2]` |
| Lab 09 | traceparent sama di log order dan payment; lag 0; SASL salah exit 1, benar exit 0; traceparent sampai DLT |
| Jaeger 2.23.0 | UI 200, OTLP 200, `/api/v3/services` 200; trace Lab 09 `4bf92f35...` tampil di UI |
| Proyek | 4 fault script OK; `npm test` 1 pass (kasus `FAILED` baru ikut diuji) |
| Demo Debezium | connector RUNNING; offset 57 -> 57 saat paused -> 59 setelah resume; slot terhapus |

## Run ulang 2026-11-11 (putaran 2)

| Langkah | Hasil |
|---|---|
| Lab 02 | partisi per key diperiksa; 6 key, masing-masing satu partisi, seq 1-10 |
| Lab 03 | publish ke `lab03.order.created.v1`; `order.created.v1` 0 record setelahnya; `envelope:check` dihapus |
| Lab 05 | HTTP 202, SIGTERM exit 143, tanpa duplikat, 0 `invalid-event` |
| Lab 06 | dedup 1 baris; poison ke DLT; replay 2x tetap 1 baris |
| Lab 07 | `event lain diabaikan` 9; HASIL OK; abort-demo OK |
| Lab 08 dari `start/` | langkah 1-8 sesuai README; `start/` dikembalikan |
| Lab 09 | traceparent sama di order dan payment; lag 0; SASL benar/salah sesuai |
| Proyek | PK `(consumer_group, event_id)`; 4 fault OK; `npm test` pass 1; LATIHAN 7 dari `start/` pass 1 |
