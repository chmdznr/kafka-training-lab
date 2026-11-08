# Evidence — Lab 06 (idempotency, retry, DLT, replay)

Dijalankan ulang: 2026-11-08, compose `kafka-training-lab` (3 broker sehat, Postgres 17).
Working dir: `labs/06-idempotency-retry-dlt/solution`.

Dependensi lab memakai `node_modules` di akar `lab/` (lihat README akar:
`npm ci` + `npm run setup:native`). `npm ci` di folder lab hanya memasang
dependensi NestJS/kafkajs/pg; modul native `@confluentinc/kafka-javascript`
diresolusi dari akar.

## Build, start, topik

```
$ npm run build
> tsc -p tsconfig.json          (exit 0)

$ npm run create-topics
topic order.created.v1 sudah ada
topic order.created.v1.payment-service.retry sudah ada
topic order.created.v1.payment-service.dlt sudah ada
```

```
$ docker compose exec -T postgres psql -U app -d shop \
    -c "insert into payment_limits (id, max_amount) values (1, 1000000) \
        on conflict (id) do update set max_amount = 1000000"
INSERT 0 1
```

Tiga proses: `start:payment`, `start:retry`, `start:order`.

## 1. Duplikat diproses sekali

```
$ npm run send:duplicate
{"sent":2,"orderId":"5ec1d346-ade5-4fc2-b0b2-10b875898c2c",
 "eventId":"e1099b10-6f5d-456f-8100-57a2fec78880"}
```

Log payment-service untuk event yang sama:

```
"event":"processed"          offset 0...
"event":"duplicate-skipped"  offset 1...
```

```
$ select count(*) from payments where order_id='5ec1d346-...'            -> 1
$ select count(*) from processed_events where event_id='e1099b10-...'    -> 1
```

## 2. Poison message ke DLT (setelah perbaikan `original-topic`)

```
$ npm run send:poison
{"orderId":"b9d606e8-bc2c-4492-b037-accc7994ef7e",
 "eventId":"cd334d56-abd7-49bd-b3b0-9073a8ff9376","amount":5000000}
```

Rantai log:

```
payment-service  retry-scheduled  topic=order.created.v1  partition=0 offset=1388 retryCount=1
payment-service-retry retry-scheduled  topic=...retry  partition=0 offset=9 retryCount=2
payment-service-retry dead-lettered    topic=...retry  partition=0 offset=10 retryCount=2
```

Header dan isi DLT (dibaca dengan `kafka-console-consumer.sh
--property print.headers=true`, difilter eventId):

```
original-topic:order.created.v1
original-partition:0
original-offset:1388
retry-count:2
error-message:amount 5000000 melebihi batas 1000000; perbaiki batas lalu replay
{"eventId":"cd334d56-...","eventType":"order.created.v1",...}
```

`original-topic` menunjuk topic asal, bukan topic retry. `original-partition`
dan `original-offset` juga berasal dari topic asal (offset 1388), bukan dari
topic retry (offset 10). Tidak ada baris pembayaran:

```
$ select count(*) from payments where order_id='b9d606e8-...'
0
```

## 3. Replay setelah perbaikan

Perbaikan adalah keputusan bisnis, bukan perubahan kode:

```
$ update payment_limits set max_amount = 10000000 where id = 1
UPDATE 1
```

Replay difilter ke eventId poison agar pesan insiden lama tidak ikut:

```
$ npm run replay -- --event-id cd334d56-abd7-49bd-b3b0-9073a8ff9376
{"event":"replayed","eventId":"cd334d56-...","orderId":"b9d606e8-...",
 "target":"order.created.v1",
 "originalError":"amount 5000000 melebihi batas 1000000; perbaiki batas lalu replay"}
{"event":"replay-done","dltTopic":"order.created.v1.payment-service.dlt",
 "replayed":1,"skipped":0,"filtered":9}
```

`target` adalah `order.created.v1` (dari header `original-topic`). Hasil:

```
$ select count(*) from payments where event_id='cd334d56-...'          -> 1
$ select count(*) from processed_events where event_id='cd334d56-...'  -> 1
```

Replay kedua dengan filter yang sama:

```
$ npm run replay -- --event-id cd334d56-...
{"event":"replay-done","replayed":1,"skipped":0,"filtered":9}
```

Log consumer: `processed` lalu `duplicate-skipped`; jumlah baris `payments`
tetap 1 karena `processed_events` sudah mengklaim eventId tersebut.

## Start folder

```
$ cd ../start && npm ci        (added 115 packages)
$ npm run build
> tsc -p tsconfig.json          (exit 0)
```

`start/` berisi salinan Lab 05 dengan penanda `// LATIHAN:` di tiga area:
tabel/klaim `processed_events` (`payment.service.ts`), publish retry dan DLT
(`payment.controller.ts`, `retry.controller.ts`), dan pengiriman ulang di
`scripts/replay-dlt.js`. Gap bersifat perilaku: build lulus, tetapi duplikat
tetap menulis dua baris, pesan gagal tidak pindah ke retry/DLT, dan replay
tidak menerbitkan ulang.

## Catatan

- `retry-policy.ts` sekarang mempertahankan `original-topic`,
  `original-partition`, dan `original-offset` di setiap lompatan retry, jadi
  DLT dan replay memakai koordinat topic asal. Sebelumnya header ini menunjuk
  topic retry.
- Topic `order.created.v1` dipakai bersama lab/agen lain. Consumer memvalidasi
  envelope dan mencatat `invalid-event` untuk pesan yang tidak sesuai kontrak;
  pesan tersebut tidak memblokir partisi.
