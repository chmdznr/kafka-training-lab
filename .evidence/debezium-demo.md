# Evidence — Demo Debezium (CDC outbox)

Tanggal: 2026-11-08
Stack: compose `lab/`, Kafka 4.4.0 (3 node KRaft), Postgres 17
(`wal_level=logical`), Debezium Connect `quay.io/debezium/connect:3.7.1.Final`
(profile `demo`).

## 1. Postgres logical replication

```bash
$ docker compose exec postgres psql -U app -d shop -tAc "show wal_level;"
logical
```

## 2. Connect dan connector

```bash
$ docker compose --profile demo up -d connect
 Container kafka-training-lab-connect-1 Started

$ curl -s http://localhost:8083/connectors
[]

$ curl -s -X POST http://localhost:8083/connectors \
    -H 'content-type: application/json' --data @demo/debezium/connector.json
{"name":"order-outbox-connector","config":{...}}
```

Status:

```json
{
  "name": "order-outbox-connector",
  "connector": { "state": "RUNNING", "worker_id": "192.168.155.8:8083", "version": "3.7.1.Final" },
  "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "192.168.155.8:8083", "version": "3.7.1.Final" } ],
  "type": "source"
}
```

Poller Lab 07 dihentikan sebelum demo, jadi satu-satunya penulis ke
`order.created.v1` adalah Debezium.

## 3. Order dipancarkan dari outbox lewat WAL

```bash
$ curl -s -X POST http://localhost:3007/orders -H 'content-type: application/json' \
    -d '{"customerId":"demo-cdc","item":"kopi","amount":25000}'
{"orderId":"f3c821d8-85bf-48d9-8100-73e1c6e69896","correlationId":"lab07-593377cd-91f0-4f23-b164-4e2ad162bca8"}
```

Event di Kafka (header dipotong ke yang relevan):

```text
...,id:e7d8b84e-5033-4726-a1dd-fd630e3997f2,event-type:order.created.v1 | f3c821d8-85bf-48d9-8100-73e1c6e69896 | {"data":{"item":"kopi","amount":25000,"orderId":"f3c821d8-85bf-48d9-8100-73e1c6e69896","customerId":"demo-cdc"},"eventId":"df01ac41-47e9-490c-a9cc-7393906391d2","version":1,"eventType":"order.created.v1","occurredAt":"2026-11-08T08:59:34.274Z","correlationId":"lab07-593377cd-91f0-4f23-b164-4e2ad162bca8"}
```

Key = `aggregateid` (orderId). Value = payload envelope sebagai JSON, bukan
string ter-escape (`table.expand.json.payload=true`). Header `event-type` dari
kolom `type`.

## 4. outbox.id == payload.eventId == header id

Setelah perubahan order-service memakai `envelope.eventId` sebagai `outbox.id`:

```bash
$ docker compose exec postgres psql -U app -d shop -tAc \
    "select id, payload->>'eventId', aggregateid from outbox where aggregateid = 'c3a2bd08-80c6-410c-a619-302431fcfa83';"
f5d04d3f-9646-4045-911a-12e258e779d3|f5d04d3f-9646-4045-911a-12e258e779d3|c3a2bd08-80c6-410c-a619-302431fcfa83
```

Event Kafka (header `id` = `eventId` payload):

```text
...,id:f5d04d3f-9646-4045-911a-12e258e779d3,event-type:order.created.v1 | c3a2bd08-80c6-410c-a619-302431fcfa83 | {...,"eventId":"f5d04d3f-9646-4045-911a-12e258e779d3",...}
```

## 5. Pause dan resume tanpa kehilangan event

```bash
$ curl -s -i -X PUT http://localhost:8083/connectors/order-outbox-connector/pause | head -1
HTTP/1.1 202 Accepted

$ curl -s http://localhost:8083/connectors/order-outbox-connector/status
connector: PAUSED
task: PAUSED
```

Dua order dibuat saat paused:

```bash
{"orderId":"ba275b8f-0fd5-4b6e-baf9-644e93ab2adf","correlationId":"lab07-4f09b4b6-1390-43c1-ab39-13121b5427bd"}
{"orderId":"5628924d-dbf5-4c99-bc04-7d32697f2aa5","correlationId":"lab07-b4bf58d6-cc88-4ea8-9546-23eec25e3e48"}
```

Konsumen membaca seluruh topik dan mencari kedua orderId: tidak ada hasil.

```text
TIDAK ADA event untuk kedua order saat paused
```

(Lingkungan verifikasi dipakai bersama agen lain; penulis lain membuat offset
total topik berubah saat pause. Karena itu bukti memakai ada/tidaknya orderId,
bukan angka offset.)

Resume:

```bash
$ curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8083/connectors/order-outbox-connector/resume
202

$ curl -s http://localhost:8083/connectors/order-outbox-connector/status
connector: RUNNING
task: RUNNING
```

Kedua event muncul setelah resume, tanpa dibuat ulang:

```text
...,event-type:order.created.v1 | 5628924d-dbf5-4c99-bc04-7d32697f2aa5 | {...,"orderId":"5628924d-dbf5-4c99-bc04-7d32697f2aa5",...}
...,event-type:order.created.v1 | ba275b8f-0fd5-4b6e-baf9-644e93ab2adf | {...,"orderId":"ba275b8f-0fd5-4b6e-baf9-644e93ab2adf",...}
```

## Kesimpulan

- Debezium membaca INSERT pada `public.outbox` dan menerbitkan ke
  `order.created.v1` dengan key orderId.
- Header `id` sama dengan `eventId` di payload setelah outbox.id diisi
  `envelope.eventId`.
- Pause menghentikan pengiriman; insert tetap tertahan dan menyusul setelah
  resume. Tidak ada event hilang.
