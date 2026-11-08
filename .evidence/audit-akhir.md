# Audit akhir (2026-11-08)

Dijalankan dari salinan bersih repo (`rsync` tanpa `node_modules`, `dist`, `.git`) setelah `docker compose down -v`, berurutan seperti alur peserta.

| Langkah | Hasil |
|---|---|
| `./scripts/verify-env.sh` | PASS 13 / FAIL 0 (termasuk pull image Testcontainers) |
| Lab 01 `solution/lab01.sh` | PASS: leader berpindah, pesan tetap terbaca, ISR pulih, topic compacted |
| Lab 02 `ordering.js`, `durability.js` | PASS: 6 key x 10 pesan urut per key; ditolak `code=19` saat ISR < 2, kirim ulang tanpa duplikat |
| Lab 03 `envelope-check.js`, `publish-order.js` | PASS: 12 pemeriksaan; event tanpa `correlationId` ditolak |
| Lab 04 `assignments.js`, `rebalance.js`, `replay.js` | PASS: 3 aktif + 1 idle; classic moved=743 ms / unaffected=31 ms, consumer moved=2970 ms / unaffected=37 ms; lag 0 setelah replay |
| Lab 05 SIGTERM (`PROCESSING_DELAY_MS=3000`) | exit 143; `payment_events` 3/3 lalu 4/4 setelah restart (rows = distinct event_id); lag 0; hanya 1 `invalid-event` (event rusak Lab 03 yang disengaja) |
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
