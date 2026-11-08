# Evidence - Lab 09 (observability dan koneksi aman)

Dijalankan: 2026-11-08, compose `kafka-training-lab` (3 broker sehat, Postgres 17,
Kafka UI, Schema Registry). Working dir: `labs/09-observability-security/solution`.

## Catatan lingkungan

- Dependensi mengikuti konvensi repo: satu instalasi bersama di akar `lab/`
  (`npm ci` + `npm run setup:native`), lalu `npm ci` di folder lab. Detail dan
  bukti resolusi binary ada di bagian Instalasi dan build.
- Verifikasi berjalan paralel dengan agen lain yang menjalankan `proyek/solution`
  dan memakai grup `payment-service` pada topic `order.created.v1`. Supaya tidak
  saling mencuri pesan, run ini memakai topic dan grup terpisah lewat variabel
  lingkungan yang tersedia di kode:
  `ORDER_CREATED_TOPIC=lab09.order.created.v1`,
  `PAYMENT_SERVICE_GROUP=lab09-payment-service`,
  `PAYMENT_SERVICE_RETRY_GROUP=lab09-payment-service-retry`.
  Nilai default di README tetap `order.created.v1` dan `payment-service`.
- HTTP port digeser ke 3100 (order) dan 3101 (payment) karena 3000/3001 dipakai
  proses `proyek/solution` milik agen lain.

## Instalasi dan build

Konvensi repo (lihat README akar): dari akar `lab/` jalankan `npm ci` lalu
`npm run setup:native`. Root `node_modules` sudah berisi binary native dari
mekanisme itu; yang diverifikasi di bawah adalah pemakaiannya dari folder lab.

Dari `labs/09-observability-security/solution`, dengan `node_modules` dibersihkan
lebih dulu:

```text
$ rm -rf node_modules && npm ci
added 115 packages in 612ms

$ ls node_modules/@confluentinc 2>/dev/null || echo NO_LOCAL_CONFLUENT
NO_LOCAL_CONFLUENT

$ node -e "console.log(require.resolve('@confluentinc/kafka-javascript'))"
/Users/chmdznr/work/achmad/training-kafka/lab/node_modules/@confluentinc/kafka-javascript/lib/index.js

$ npm run create-topics
topic order.created.v1 sudah ada
topic order.created.v1.payment-service.retry sudah ada
topic order.created.v1.payment-service.dlt sudah ada

$ npm run build
> lab09-observability-security-solution@1.0.0 build
> tsc -p tsconfig.json
(exit 0)
```

`package.json` dan `package-lock.json` lab tidak memuat
`@confluentinc/kafka-javascript`; paket dan binary native diresolusi dari
`node_modules` akar.

Dari `labs/09-observability-security/start`:

```text
$ rm -rf node_modules && npm ci
added 115 packages in 726ms

$ node -e "console.log(require.resolve('@confluentinc/kafka-javascript'))"
/Users/chmdznr/work/achmad/training-kafka/lab/node_modules/@confluentinc/kafka-javascript/lib/index.js

$ npm run build
> lab09-observability-security-start@1.0.0 build
> tsc -p tsconfig.json
(exit 0)
```

Setelah `npm ci`, service dijalankan ulang pada port 3110/3111 dengan topic
`lab09.recheck.order.created.v1`. Satu order melewati kedua service dengan
`correlationId` dan `traceparent` yang sama:

```json
{"level":"info","service":"payment-service","event":"processed","topic":"lab09.recheck.order.created.v1","partition":2,"offset":"1","eventId":"2f11a59f-3152-4190-9d32-1663a7c30876","correlationId":"lab09-recheck-2","orderId":"037bf4fe-ad70-4880-adcf-1a75e39d1528","traceparent":"00-feedfacefeedfacefeedfacefeedface-abcdefabcdefabcd-01"}
```

Proses pemeriksaan dihentikan setelah itu.

## Topic

```text
$ ORDER_CREATED_TOPIC=lab09.order.created.v1 PAYMENT_SERVICE_GROUP=lab09-payment-service \
    npm run create-topics
topic lab09.order.created.v1 dibuat (3 partisi, RF 3, min.insync.replicas=2)
topic lab09.order.created.v1.lab09-payment-service.retry dibuat
topic lab09.order.created.v1.lab09-payment-service.dlt dibuat
```

## 1. Satu order ditelusuri lewat correlationId

Request:

```bash
$ curl -s -X POST localhost:3100/orders \
    -H 'content-type: application/json' \
    -H 'x-correlation-id: lab09-correlation-1' \
    -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' \
    -d '{"customerId":"cust-lab09","amount":125000,"currency":"IDR"}'
{"orderId":"5e80c7f7-3160-4631-ab13-7969a57912e9","eventId":"6e36483c-3215-4222-935f-84a3ed81829c","correlationId":"lab09-correlation-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
```

order-service:

```json
{"level":"info","service":"order-service","event":"order.created","orderId":"5e80c7f7-3160-4631-ab13-7969a57912e9","eventId":"6e36483c-3215-4222-935f-84a3ed81829c","correlationId":"lab09-correlation-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","topic":"lab09.order.created.v1","partition":0,"offset":"0"}
```

payment-service:

```json
{"level":"info","service":"payment-service","event":"received","topic":"lab09.order.created.v1","partition":0,"offset":"0","eventId":"6e36483c-3215-4222-935f-84a3ed81829c","correlationId":"lab09-correlation-1","orderId":"5e80c7f7-3160-4631-ab13-7969a57912e9","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
{"level":"info","service":"payment-service","event":"processed","topic":"lab09.order.created.v1","partition":0,"offset":"0","eventId":"6e36483c-3215-4222-935f-84a3ed81829c","correlationId":"lab09-correlation-1","orderId":"5e80c7f7-3160-4631-ab13-7969a57912e9","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
```

Nilai `correlationId`, `eventId`, dan `traceparent` sama di kedua service;
`topic`, `partition`, dan `offset` ikut di setiap baris.

## 2. Nilai dibuat otomatis bila header tidak dikirim

```bash
$ curl -s -X POST localhost:3100/orders -H 'content-type: application/json' \
    -d '{"customerId":"cust-lab09-noheader","amount":50000}'
{"orderId":"85f04bbf-ffa1-4fd0-847c-3926ba3d424a","eventId":"bba1fc94-33c8-4b3b-b08d-09bb99ecd432","correlationId":"f881b15f-6ddd-4d4d-bf90-3ed3b4c20281","traceparent":"00-9ab7444b3d5b4153a03fce7d7bf6cfcc-b8d27ab498d84bcb-01"}
```

```json
{"level":"info","service":"order-service","event":"order.created","orderId":"85f04bbf-ffa1-4fd0-847c-3926ba3d424a","eventId":"bba1fc94-33c8-4b3b-b08d-09bb99ecd432","correlationId":"f881b15f-6ddd-4d4d-bf90-3ed3b4c20281","traceparent":"00-9ab7444b3d5b4153a03fce7d7bf6cfcc-b8d27ab498d84bcb-01","topic":"lab09.order.created.v1","partition":1,"offset":"0"}
{"level":"info","service":"payment-service","event":"received","topic":"lab09.order.created.v1","partition":1,"offset":"0","eventId":"bba1fc94-33c8-4b3b-b08d-09bb99ecd432","correlationId":"f881b15f-6ddd-4d4d-bf90-3ed3b4c20281","orderId":"85f04bbf-ffa1-4fd0-847c-3926ba3d424a","traceparent":"00-9ab7444b3d5b4153a03fce7d7bf6cfcc-b8d27ab498d84bcb-01"}
```

## 3. Header Kafka

```bash
$ docker compose exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 --topic lab09.order.created.v1 \
    --from-beginning --max-messages 1 --timeout-ms 5000 \
    --property print.key=true --property print.headers=true
correlation-id:f881b15f-6ddd-4d4d-bf90-3ed3b4c20281,event-type:lab09.order.created.v1,traceparent:00-9ab7444b3d5b4153a03fce7d7bf6cfcc-b8d27ab498d84bcb-01	85f04bbf-ffa1-4fd0-847c-3926ba3d424a	{"eventId":"bba1fc94-...","eventType":"lab09.order.created.v1",...}
```

## 4. Lag naik lalu pulih

Payment dijalankan ulang dengan `PROCESSING_DELAY_MS=2000`, lalu 24 order dikirim.

Sesaat setelah pengiriman:

```text
GROUP                  TOPIC                   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
lab09-payment-service  lab09.order.created.v1  0          1               7               6    payment-service-80ba61cc-...
lab09-payment-service  lab09.order.created.v1  1          1               7               6    payment-service-80ba61cc-...
lab09-payment-service  lab09.order.created.v1  2          1               12              11   payment-service-80ba61cc-...
```

Kafka UI melihat group yang sama (API yang dipakai halaman Consumers):

```bash
$ curl -s 'http://localhost:8080/api/clusters/local/consumer-groups/lab09-payment-service'
{"inherit":"details","groupId":"lab09-payment-service","members":1,"topics":1,"state":"STABLE",
 "consumerLag":21,
 "partitions":[{"topic":"lab09.order.created.v1","partition":0,"currentOffset":2,"endOffset":7,"consumerLag":5,...},
               {"topic":"lab09.order.created.v1","partition":2,"currentOffset":1,"endOffset":12,"consumerLag":11,...}]}
```

Enam detik kemudian lag turun menjadi 5 + 5 + 7 = 17 karena consumer menyelesaikan
satu pesan per 2 detik.

Setelah payment dihentikan dan dijalankan ulang tanpa `PROCESSING_DELAY_MS`:

```text
GROUP                  TOPIC                   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
lab09-payment-service  lab09.order.created.v1  0          7               7               0
lab09-payment-service  lab09.order.created.v1  1          7               7               0
lab09-payment-service  lab09.order.created.v1  2          12              12              0
```

## 5. SASL/SCRAM

Password salah:

```bash
$ node scripts/sasl-check.js --password salah
{
  message: '[thrd:sasl_plaintext://localhost:9192/bootstrap]: ... SASL authentication error: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512 (after 306ms in state AUTH_REQ)',
  fac: 'FAIL', ...
}
{"event":"sasl-check-failed","error":"KafkaJSConnectionError","message":"Local: Broker transport failure"}
(exit 1)
```

Baris librdkafka muncul untuk setiap broker dan percobaan ulang; intinya sama:
`Authentication failed ... invalid credentials ... SCRAM-SHA-512`.

Password benar:

```bash
$ node scripts/sasl-check.js
{"event":"produced","topic":"lab09.sasl-check.v1","partition":1,"offset":"1"}
{"event":"consumed","topic":"lab09.sasl-check.v1","partition":1,"offset":"1","username":"app","mechanism":"SCRAM-SHA-512"}
{"event":"sasl-check-ok","brokers":["localhost:9192","localhost:9193","localhost:9194"],"username":"app","mechanism":"SCRAM-SHA-512"}
(exit 0)
```

Kedua run dijalankan setelah `node_modules` lab diisi ulang dengan `npm ci`;
paket klien dan binary native diresolusi dari `node_modules` akar (lihat bagian
Instalasi dan build).

## Catatan nama properti SSL CA

Nama properti `ssl.ca.location` diverifikasi di paket klien:
`node_modules/@confluentinc/kafka-javascript/lib/kafkajs/_common.js` memakai
`new Kafka({ kafkaJS: { ssl: true }, "ssl.ca.location": ... })` pada konfigurasi
level atas, dan `types/config.d.ts` mendeklarasikan
`"ssl.ca.location"?: string` di luar blok `kafkaJS`. README Lab 09 memakai nama
dan posisi properti yang sama.

## 6. traceparent ikut ke retry topic dan DLT

Order dengan `amount` 50.000.000 melewati batas 10.000.000.

Retry topic:

```text
event-type:lab09.order.created.v1,correlation-id:lab09-retry-2,traceparent:00-aaaabbbbccccddddeeeeffff00001111-2222333344445555-01,original-topic:lab09.order.created.v1,retry-count:1,next-retry-at:...,last-error:amount 50000000 melebihi batas 10000000; perbaiki batas lalu replay
```

Log retry worker (nilai `traceparent` sama, `retryCount` 2 lalu dead-lettered):

```json
{"level":"info","service":"payment-service","event":"retry-scheduled","topic":"lab09.order.created.v1.lab09-payment-service.retry","partition":1,"offset":"0","eventId":"96294eae-e71e-4f3c-9f6e-1369e6edeedb","correlationId":"lab09-retry-2","orderId":"03536de3-712b-449c-8744-82a01def1bf9","traceparent":"00-aaaabbbbccccddddeeeeffff00001111-2222333344445555-01","retryCount":2,...}
{"level":"info","service":"payment-service","event":"dead-lettered","topic":"lab09.order.created.v1.lab09-payment-service.retry","partition":1,"offset":"1","eventId":"96294eae-e71e-4f3c-9f6e-1369e6edeedb","correlationId":"lab09-retry-2","orderId":"03536de3-712b-449c-8744-82a01def1bf9","traceparent":"00-aaaabbbbccccddddeeeeffff00001111-2222333344445555-01","retryCount":2,...}
```

DLT:

```text
event-type:lab09.order.created.v1,correlation-id:lab09-retry-2,traceparent:00-aaaabbbbccccddddeeeeffff00001111-2222333344445555-01,original-topic:lab09.order.created.v1,original-partition:1,original-offset:1,retry-count:2,error-message:amount 50000000 melebihi batas 10000000; perbaiki batas lalu replay,failed-at:2026-11-08T09:15:54.404Z
```

## 7. start/ gagal secara perilaku, bukan sintaks

`start/` build sukses (lihat bagian Build). Dengan celah SASL dan header belum diisi:

```text
$ node scripts/sasl-check.js --password salah
{"event":"sasl-check-failed","error":"KafkaJSConnectionError","message":"Local: Broker transport failure"}
(exit 1, karena klien belum memakai SASL)
```

Order dari `start/` yang dikirim dengan header `traceparent`:

```json
{"orderId":"07cb0255-310b-4e9f-88fc-ec44f079cc5d","eventId":"397b3c53-2de5-4074-af89-d64dbc7adf5c","correlationId":"start-gap-1"}
```

Tidak ada `traceparent` di respons, log, atau header Kafka:

```json
{"level":"info","service":"order-service","event":"order.created","orderId":"07cb0255-...","eventId":"397b3c53-...","correlationId":"start-gap-1","topic":"lab09.start.order.created.v1","partition":0,"offset":"0"}
{"level":"info","service":"payment-service","event":"received","topic":"lab09.start.order.created.v1","partition":0,"offset":"0","eventId":"397b3c53-...","correlationId":"start-gap-1","orderId":"07cb0255-..."}
```

```text
correlation-id:start-gap-1,event-type:lab09.start.order.created.v1
```

## 8. Proses dihentikan

Semua proses `payment-service`, `order-service`, dan retry worker dari run ini
dihentikan dengan SIGTERM. Port 3100, 3101, 3102, dan 3103 tidak lagi listen.

Jaeger tidak dipakai untuk lab ini; OpenTelemetry tidak disertakan (lihat README).
