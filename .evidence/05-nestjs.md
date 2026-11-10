# Evidence — Lab 05 (NestJS + Kafka)

Dijalankan: 2026-11-08, compose `kafka-training-lab` (3 broker sehat, Postgres 17).
Working dir: `labs/05-nestjs/solution`.

Catatan pembaruan: paket `@confluentinc/kafka-javascript` sekarang dipasang
sekali di akar `lab/` (`npm ci` + `npm run setup:native`) dan dipakai lewat
resolusi direktori induk. `npm ci` di folder lab hanya memasang dependensi
NestJS/kafkajs/pg. Build `start/` tercatat di `nestjs-start-builds.md`.

## Build & topic

```
$ npm install && npm run build
> tsc -p tsconfig.json          (exit 0)

$ npm run create-topics
topic order.created.v1 sudah ada   (dibuat dengan 3 partisi, RF 3, min.insync.replicas=2)
```

```
$ docker compose exec kafka-1 kafka-topics.sh --bootstrap-server localhost:29092 \
    --describe --topic order.created.v1
PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2
```

## Alur normal

```
$ curl -s -X POST localhost:3000/orders -H 'content-type: application/json' \
    -H 'x-correlation-id: demo-correlation-1' \
    -d '{"customerId":"cust-1","amount":125000,"currency":"IDR"}'
{"orderId":"f64806b5-...","eventId":"45fb760c-5f4e-4f53-8fc7-ac37a1104922","correlationId":"demo-correlation-1"}
```

Log payment-service (header `correlation-id` sampai, offset di-commit manual):

```
{"level":"info","service":"payment-service","event":"received","topic":"order.created.v1","partition":0,"offset":"2","eventId":"45fb760c-...","correlationId":"demo-correlation-1","orderId":"f64806b5-..."}
{"level":"info","service":"payment-service","event":"committed","topic":"order.created.v1","partition":0,"offset":"2",...}
```

```
$ docker compose exec postgres psql -U app -d shop -c \
    "select event_id, order_id, amount from payment_events"
45fb760c-5f4e-4f53-8fc7-ac37a1104922 | f64806b5-... | 125000
```

## Fault: SIGTERM saat memproses

Payment dijalankan dengan `PROCESSING_DELAY_MS=3000`. Tiga order dikirim; SIGTERM
dikirim ±4 detik kemudian, saat pesan kedua sedang diproses.

Log saat shutdown (drain berjalan sebelum consumer ditutup):

```
{"event":"received", ...,"partition":0,"offset":"3",...}
{"event":"committed",...,"partition":0,"offset":"3",...}
[Nest] LOG [ServerKafka] INFO [ConsumerGroup] Pausing fetching from 1 topics
{"event":"committed",...,"partition":0,"offset":"3",...}   (pesan yang sedang berjalan selesai)
[Nest] LOG [ServerKafka] INFO [Consumer] Stopped
exit=143
```

Setelah restart, hanya pesan yang belum di-commit yang diproses:

```
{"event":"received","topic":"order.created.v1","partition":0,"offset":"4","eventId":"ff0109e6-...","correlationId":"sigterm-final-3"}
{"event":"committed",...,"offset":"4",...}
```

Hasil akhir:

```
$ select count(*) rows, count(distinct event_id) distinct_events from payment_events
 3 | 3
```

`kafka-consumer-groups --describe --group payment-service`: lag 0 di ketiga partisi,
tidak ada pesan yang diproses dua kali.

## Catatan yang ditemukan saat pengujian

- Tanpa `ConsumerDrain` (`src/common/consumer-drain.ts`), `kill -TERM` membuat
  kafkajs menandai runner berhenti lebih dulu; commit manual dari handler yang
  masih berjalan diabaikan tanpa error, sehingga pesan tersebut diproses ulang
  setelah restart. Drain menghentikan fetch, menunggu handler selesai, baru
  transport ditutup.
- Pesan `{"seq":1}` dari smoke test agen lain ada di topic yang sama. Consumer
  memvalidasi envelope dan melewati pesan yang tidak sesuai kontrak (log
  `invalid-event`) supaya partisi tidak macet. Di Lab 06 jalur ini tetap
  dilewati; hanya kegagalan bisnis yang masuk retry/DLT.
- `payment_events` sengaja tanpa unique constraint: duplikat masih mungkin,
  itulah yang diperbaiki di Lab 06.

## Run ulang 2026-11-11

NestJS `12.4.2` (`@nestjs/common`, `core`, `microservices`, `platform-express`).
`npm ci` dan `npm run build` exit 0 di `start/` dan `solution/`.

```
POST /orders -> HTTP 202 {"orderId":"c9d181f3-...","correlationId":"demo-1"}
payment-service: received -> committed; baris payment_events tersimpan
SIGTERM saat memproses (PROCESSING_DELAY_MS=3000) -> exit 143
restart: rows=9, distinct event_id=9 (tanpa duplikat)
```

Jumlah baris kumulatif karena run ini mengikuti run pertama di stack yang sama
(run pertama: rows=5, events=5).

## Run ulang 2026-11-11 (putaran 2)

Stack bersih, setelah Lab 01 sampai 03:

```
POST /orders -> HTTP 202 {"orderId":"5e9da4c0-...","correlationId":"demo-1"}
SIGTERM saat memproses -> exit 143
restart: rows=4, distinct event_id=4 (tanpa duplikat)
invalid-event di log payment-service: 0
```

Sebelumnya consumer mencatat satu `invalid-event` untuk event rusak Lab 03;
sejak Lab 03 memakai topic sandbox, angka itu 0.
