# Evidence Lab 03

Tanggal: 2026-11-08; dijalankan ulang 2026-11-11 (lihat bagian "Run ulang 2026-11-11")
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

## `order.created.v1` hanya berisi record Lab 03 (sebelum 2026-11-11)

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

## Run ulang 2026-11-11

Perintah sama, ditambah `npm run envelope:check` dari akar `lab/` (semua exit 0):

```
PASS lab03 envelope: 12 pemeriksaan
PASS  produce 2 record ke order.created.v1 (1 valid, 1 tanpa correlationId)
PASS  event valid lolos validateEnvelope dan header cocok
PASS  event tanpa correlationId ditolak: Envelope tidak valid: correlationId wajib string tidak kosong
PASS lab03 publish: envelope dipakai, event rusak ditolak consumer

$ npm run envelope:check
PASS lab03 envelope: 12 pemeriksaan
```

Catatan run ini: `npm run envelope:check` saat itu menjalankan `solution/`.
Script itu dihapus di putaran 2 (lihat di bawah).

## Run ulang 2026-11-11 (putaran 2)

Uji publish dipindah ke topic sandbox `lab03.order.created.v1` (start dan
solution); `eventType` tetap `order.created.v1`. Stack bersih, setelah Lab 01:

```
$ node solution/envelope-check.js
PASS lab03 envelope: 12 pemeriksaan
$ node solution/publish-order.js
PASS  produce 2 record ke lab03.order.created.v1 (1 valid, 1 tanpa correlationId)
PASS  event valid lolos validateEnvelope dan header cocok
PASS  event tanpa correlationId ditolak: Envelope tidak valid: correlationId wajib string tidak kosong
PASS lab03 publish: envelope dipakai, event rusak ditolak consumer
$ node start/publish-order.js
FAIL lab03 publish: LATIHAN: buat envelope dengan createEnvelope dan kirim event valid + event tanpa correlationId
```

Langkah 4 README (`--topic lab03.order.created.v1`): 2 record terbaca, 1 di
antaranya memuat `correlationId`.

```
lab03.order.created.v1:0:0 lab03.order.created.v1:1:2 lab03.order.created.v1:2:0
order.created.v1:0:0 order.created.v1:1:0 order.created.v1:2:0
```

`order.created.v1` tetap kosong setelah Lab 03. Script `envelope:check`
dihapus dari `package.json` akar karena tidak dirujuk README, slide, atau
CHEATSHEET; `npm run envelope:check` kini menjawab `Missing script`.
