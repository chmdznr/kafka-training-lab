# Evidence — Lab 07 Outbox dan transaksi

Tanggal: 2026-11-08; dijalankan ulang 2026-11-11 (lihat bagian "Run ulang 2026-11-11")
Stack: compose `lab/`, Kafka 4.4.0 (3 node KRaft, RF3, min.insync.replicas=2),
Postgres 17.

Catatan lingkungan: npm memblokir install script secara default di mesin
verifikasi, jadi saat verifikasi dependency dipasang lokal dengan
`npm install --ignore-scripts` dan native binding Node 24 disalin dari build
yang ada. Dokumentasi lab memakai mekanisme repo: dari root `lab/` jalankan
`npm ci` lalu `npm run setup:native`, dan perintah lab memakai `lab/node_modules`.
Sudah diuji: dengan `node_modules` lokal dipindahkan, `node scripts/setup.js`
dan `node scripts/check.js` tetap jalan dari dependency root.

Semua perintah Node dari `lab/labs/07-outbox-transaksi/solution`, perintah
compose dari root `lab/`.

## 1. Setup

```bash
$ node scripts/setup.js
topic order.created.v1: sudah ada
topic payment.completed.v1: dibuat
database dan topic siap
```

## 2. Outbox dasar

Order dibuat lewat HTTP:

```bash
$ curl -s -X POST http://localhost:3007/orders \
    -H 'content-type: application/json' \
    -d '{"customerId":"c-1","item":"kopi","amount":25000}'
{"orderId":"ab1fd8c9-d0fd-4b9a-b044-62e34a32c99a","correlationId":"lab07-b7a623d9-7b7e-4b46-87aa-cb488ee16828"}
```

Publisher:

```text
{"at":"...","msg":"outbox terkirim","count":1}
```

## 3. Fault injection: broker mati, order tetap masuk

```bash
$ docker compose stop kafka-1 kafka-2 kafka-3
 Container ... Stopped (x3)

$ for i in 1 2 3; do curl ... ; done
{"orderId":"27b35c17-812d-441c-ae87-cb2bf141d0c1",...}
{"orderId":"3572cd1f-0169-4669-92f9-fe55c8c855b4",...}
{"orderId":"65381480-4f9d-435c-b5f4-48ee7d0e67bc",...}

$ docker compose exec postgres psql -U app -d shop -tAc \
    "select count(*) from outbox where published_at is null;"
3
```

Publisher tetap hidup dan mencoba lagi (baris log sendiri, log librdkafka
dihilangkan):

```text
{"at":"...","msg":"publish gagal, baris outbox tetap pending dan akan dicoba lagi","error":"Local: Message timed out"}
{"at":"...","msg":"publish gagal, baris outbox tetap pending dan akan dicoba lagi","error":"Local: Message timed out"}
```

Broker dinyalakan lagi:

```bash
$ docker compose up -d --wait kafka-1 kafka-2 kafka-3
... Healthy
```

Publisher memulihkan sendiri tanpa restart:

```text
{"at":"...","msg":"outbox terkirim","count":3}
```

Query pending setelah pemulihan: `0`.

## 4. Bukti tidak ada order tanpa event / event tanpa order

```bash
$ node scripts/check.js

order di database      : 7
event lab ini          : 7 (key unik: 7)
event lain diabaikan   : 23
outbox belum terkirim  : 0
event tidak valid      : 0
key != data.orderId    : 0
eventId duplikat       : 0

HASIL: OK - tidak ada order tanpa event, tidak ada event tanpa order
```

`event lain diabaikan` adalah record dari lab/agen lain di topik yang sama;
order-service menandai event Lab 07 dengan `correlationId` berawalan `lab07-`.

## 5. Read-process-write dengan transaksi Kafka

Consumer `payment-tx` (`autoCommit: false`, `read_committed`,
`transactionalId=payment-tx-1`) memproses order dan meng-commit offset di
transaksi yang sama:

```text
{"msg":"order diproses, payment.completed.v1 dan offset ter-commit bersama","orderId":"a817ab4f-a9a5-4c0c-bf1c-96164fd92318","paymentId":"1a0f95ec-4278-4c8b-8195-2642b81a4173","partition":0,"offset":"..."}
```

Event hasilnya (dibaca `read_committed`):

```text
a817ab4f-a9a5-4c0c-bf1c-96164fd92318 | {"eventId":"d32ed6ed-d658-42b3-bb9a-1df06e0ce68b","eventType":"payment.completed.v1","version":1,"occurredAt":"2026-11-08T08:58:04.969Z","correlationId":"lab07-6607c7b7-5b27-465d-827e-e63a459fbcbb","causationId":"4ce739ef-fec0-48b2-bbd1-c9007e7369dd","data":{"paymentId":"1a0f95ec-4278-4c8b-8195-2642b81a4173","orderId":"a817ab4f-a9a5-4c0c-bf1c-96164fd92318","amount":20000,"status":"completed"}}
```

`causationId` menunjuk `eventId` order asal.

Record yang bukan `order.created.v1` valid dilewati, tetapi offset-nya ikut
di-commit dalam transaksi (hanya `sendOffsets`, tanpa record hasil). Contoh log
saat backlog berisi banyak record uji:

```text
{"msg":"lewati record yang bukan order.created.v1 valid, offset akan ikut di-commit","partition":0,"offset":"463","totalDilewati":1}
...
{"msg":"lewati record yang bukan order.created.v1 valid, offset akan ikut di-commit","partition":2,"offset":"16500","totalDilewati":49000}
{"msg":"offset record tidak valid di-commit agar lag kembali 0","partitions":1}
```

Setelah backlog habis, lag grup `payment-tx` kembali nol:

```text
GROUP           TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
payment-tx      order.created.v1 0          17114           17114           0
payment-tx      order.created.v1 1          16927           16927           0
payment-tx      order.created.v1 2          16916           16916           0
```

## 6. Transaksi abort vs read_committed

```bash
$ node scripts/abort-demo.js
topic tx-demo.v1 dibuat ulang
transaksi 1: ABORT (key=aborted) pada tx-demo.v1
transaksi 2: COMMIT (key=committed) pada tx-demo.v1

read_committed   melihat : [committed]
read_uncommitted melihat : [aborted, committed]

HASIL: OK - record dari transaksi abort tidak terlihat oleh read_committed, terlihat oleh read_uncommitted
```

## 7. outbox.id == envelope.eventId

Diverifikasi bersama demo Debezium:

```bash
$ docker compose exec postgres psql -U app -d shop -tAc \
    "select id, payload->>'eventId' from outbox where aggregateid = 'c3a2bd08-80c6-410c-a619-302431fcfa83';"
f5d04d3f-9646-4045-911a-12e258e779d3|f5d04d3f-9646-4045-911a-12e258e779d3
```

## Kesimpulan

- Saat semua broker mati, 3 order tetap tersimpan dan 3 baris outbox tetap
  pending; setelah broker hidup, ketiganya terkirim tanpa restart publisher.
- `check` menyatakan tidak ada order tanpa event dan tidak ada event tanpa
  order.
- Read-process-write meng-commit payment event dan offset consumer bersama;
  record dari transaksi abort tidak terlihat oleh `read_committed`.
- Record tidak valid tetap dilewati, tetapi offset-nya di-commit lewat
  transaksi `sendOffsets`; lag grup `payment-tx` kembali `0` di ketiga partisi.

## Catatan akhir

Setelah demo CDC, empat baris outbox demo masih `pending` karena Debezium tidak
meng-update `published_at`. Poller dijalankan sekali untuk mengirimnya
(at-least-once, jadi keempat event itu menjadi duplikat). `check` tetap OK:

```text
order di database      : 12
event lab ini          : 16 (key unik: 12)
outbox belum terkirim  : 0
eventId duplikat       : 4
HASIL: OK - tidak ada order tanpa event dan tidak ada event tanpa order
```

Angka `event lain diabaikan` besar karena topik yang sama dipakai bersamaan oleh
lab/agen lain saat verifikasi; di kelas berurutan angkanya kecil.

## Uji ulang di stack bersih

Setelah `docker compose down -v` dan rebuild oleh LabInfra:

```text
$ npm run setup
topic order.created.v1: sudah ada
topic payment.completed.v1: dibuat
database dan topic siap

$ npm run check            # satu order baru, topik bersih
order di database      : 1
event lab ini          : 1 (key unik: 1)
event lain diabaikan   : 2
outbox belum terkirim  : 0
HASIL: OK - tidak ada order tanpa event dan tidak ada event tanpa order
```

`payment-tx` memproses order itu dalam transaksi dan meng-commit offsetnya.
LAG 0 pada partisi yang punya record; partisi kosong menampilkan `-`:

```text
payment-tx order.created.v1 0 2 2 0
payment-tx order.created.v1 1 1 1 0
payment-tx order.created.v1 2 - 0 -
```

Event pembayaran untuk order tersebut ada di `payment.completed.v1` dengan
`causationId` yang cocok.

## Run ulang 2026-11-11

```
POST /orders -> HTTP 201; publisher: outbox terkirim
consumer (localhost:29092) menemukan event order tersebut; outbox terkirim = t

3 broker dihentikan, 3 order masuk (201 201 201)
pending while down: 3     publisher fail logs: 2
broker dinyalakan:   pending after start: 0

npm run check
order di database      : 4
event lab ini          : 4 (key unik: 4)
event lain diabaikan   : 15
outbox belum terkirim  : 0
event tidak valid      : 0
key != data.orderId    : 0
eventId duplikat       : 0
HASIL: OK - tidak ada order tanpa event, tidak ada event tanpa order

payment-tx: lag 0 di ketiga partisi order.created.v1
abort-demo:
read_committed   melihat : [committed]
read_uncommitted melihat : [aborted, committed]
HASIL: OK
```
