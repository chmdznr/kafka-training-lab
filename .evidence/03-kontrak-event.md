# Evidence Lab 03

Tanggal: 2026-11-08
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1
Commands (dari `labs/03-kontrak-event/`):

```
node solution/envelope-check.js
node solution/publish-order.js
```

## Pemeriksaan envelope

```
PASS  createEnvelope menghasilkan eventId UUID dan occurredAt ISO-8601
PASS  validateEnvelope menerima envelope lengkap
PASS  topicFor mengembalikan nama topic dari eventType
PASS  headersFor mengisi correlation-id dan event-type
PASS  menolak event tanpa eventId
PASS  menolak event tanpa correlationId
PASS  menolak eventType yang bukan pola domain.event
PASS  menolak version yang tidak cocok dengan akhiran eventType
PASS  menolak occurredAt tanpa zona waktu
PASS  menolak data yang bukan object
PASS  menolak causationId kosong
PASS  event turunan menerima causationId dari event sebelumnya
PASS lab03 envelope: 12 pemeriksaan
```

## Envelope dipakai dan event rusak ditolak consumer

```
PASS  produce 2 record ke order.created.v1 (1 valid, 1 tanpa correlationId)
PASS  event valid lolos validateEnvelope dan header cocok
PASS  event tanpa correlationId ditolak: Envelope tidak valid: correlationId wajib string tidak kosong
PASS lab03 publish: envelope dipakai, event rusak ditolak consumer
```

## `order.created.v1` hanya berisi record Lab 03

Setelah clean run Labs 01-04:

```
order.created.v1:0:2
order.created.v1:1:0
order.created.v1:2:0
```

Consumer dari awal menemukan tepat 2 record:

```
correlation-id:checkout-...,event-type:order.created.v1  order-...  {"eventId":"...","eventType":"order.created.v1",...,"correlationId":"checkout-...","data":{...}}
event-type:order.created.v1                              order-...  {"eventId":"...","eventType":"order.created.v1",...,"data":{...}}
Processed a total of 2 messages
```

Record pertama valid; record kedua sengaja tidak punya `correlationId`. Tidak
ada record dari Lab 01, 02, atau 04 di topic ini.

## Validasi alur start

Sebelum diisi, kedua entry point berhenti dengan pesan LATIHAN (exit 1):

```
$ node start/envelope-check.js
FAIL lab03 envelope: LATIHAN: tulis pemeriksaan eventId, occurredAt, topicFor, headersFor, dan kasus yang harus ditolak

$ node start/publish-order.js
FAIL lab03 publish: LATIHAN: buat envelope dengan createEnvelope dan kirim event valid + event tanpa correlationId
```

Setelah diisi, hasilnya sama dengan solution: 12 pemeriksaan envelope PASS dan
event tanpa correlationId ditolak consumer.
