# Evidence clean run (urutan peserta)

Tanggal: 2026-11-08
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1

Urutan yang dijalankan dari `lab/`:

```bash
docker compose down -v
./scripts/verify-env.sh
cd labs/01-cluster && bash solution/lab01.sh
cd ../02-producer && node solution/ordering.js && node solution/durability.js
cd ../03-kontrak-event && node solution/envelope-check.js && node solution/publish-order.js
cd ../04-consumer-group && node solution/assignments.js && node solution/rebalance.js && node solution/replay.js
```

Hasil: `down=0`, `verify=0` (PASS 12/12), `lab01=0`, `lab02=0`, `lab03=0`,
`lab04=0`.

## Offset akhir per topic

```
order.created.v1:0:2
order.created.v1:1:0
order.created.v1:2:0          -> total 2

lab01.orders.v1:0:4
lab01.orders.v1:1:2
lab01.orders.v1:2:0           -> total 6 (traffic uji Lab 01)

lab02.orders.v1:0:35
lab02.orders.v1:1:10
lab02.orders.v1:2:20          -> total 65 (traffic uji Lab 02)

lab04.orders.v1:0:4165
lab04.orders.v1:1:4136
lab04.orders.v1:2:4135        -> total 12.436 (traffic uji Lab 04)

lab.verify.v1:0:1
lab.verify.v1:1:0
lab.verify.v1:2:1             -> total 2 (smoke verify-env)
```

`order.created.v1`:

```
Topic: order.created.v1  PartitionCount: 3  ReplicationFactor: 3
  Configs: min.insync.replicas=2
  Partition: 0  Leader: 1  Replicas: 2,3,1  Isr: 1,2,3
  Partition: 1  Leader: 1  Replicas: 3,1,2  Isr: 1,2,3
  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

## Isi `order.created.v1`

Consumer dari awal hanya menemukan 2 record, keduanya dari Lab 03:

```
correlation-id:checkout-acd9a6ef-...,event-type:order.created.v1  order-439fea12-...  {"eventId":"...","eventType":"order.created.v1",...,"correlationId":"checkout-acd9a6ef-...","data":{...}}
event-type:order.created.v1                                       order-439fea12-...  {"eventId":"...","eventType":"order.created.v1",...,"data":{...}}
Processed a total of 2 messages
```

Record pertama valid (punya `correlation-id`); record kedua sengaja rusak
(tidak punya `correlationId`). Tidak ada record uji Lab 01, 02, atau 04 di
topic kanonik.
