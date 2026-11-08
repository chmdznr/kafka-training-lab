# Evidence — Lab 08 Schema Registry

Tanggal: 2026-11-08
Stack: compose root `lab/` (Kafka 4.4.0 KRaft 3 node, Schema Registry 8.4.0).
Topic sandbox: `lab08.order.created.v1`, subject `lab08.order.created.v1-value`.

Catatan lingkungan: npm di mesin verifikasi memblokir install script secara
default, jadi saat verifikasi dependency dipasang lokal dengan
`npm install --ignore-scripts` dan native binding `@confluentinc/kafka-javascript`
diambil dari build Node 24 yang ada. Dokumentasi lab memakai mekanisme repo:
dari root `lab/` jalankan `npm ci` lalu `npm run setup:native`, dan perintah lab
memakai `lab/node_modules`. Sudah diuji: tanpa `node_modules` lokal,
`node scripts/register.js` jalan dari dependency root (termasuk `avsc`).

Semua perintah dari `lab/labs/08-schema-registry/solution`.

## 1. Register v1

```bash
$ node scripts/register.js avro/order-created-v1.avsc
subject lab08.order.created.v1-value
versi 1, schema id 1
status: DITERIMA registry
```

## 2. Compatibility BACKWARD

```bash
$ node scripts/set-compat.js BACKWARD
compatibility lab08.order.created.v1-value: BACKWARD

$ curl -s http://localhost:8081/config/lab08.order.created.v1-value
{"compatibilityLevel":"BACKWARD"}
```

## 3. Produce v1

```bash
$ node scripts/produce.js --version 1 --count 2
terkirim: schema v1 id=1 orderId=f5bdd47f-9fe4-4c57-b34e-678433e0f126 bytes=186
terkirim: schema v1 id=1 orderId=1da53a44-aa5b-41cc-abc6-f492a4dc1978 bytes=186
```

## 4. Register v2 (field opsional dengan default)

```bash
$ node scripts/register.js avro/order-created-v2.avsc
subject lab08.order.created.v1-value
versi 2, schema id 2
status: DITERIMA registry

$ node scripts/produce.js --version 2 --count 2
terkirim: schema v2 id=2 orderId=b03a3d95-cd50-427e-b5d7-6893a2769bb1 bytes=190
terkirim: schema v2 id=2 orderId=b679a47e-b303-4ae3-8464-a3befc365aaa bytes=190
```

## 5. Consumer baru membaca kedua versi

```bash
$ node scripts/consume.js
...
[consumer] orderId=b03a3d95-cd50-427e-b5d7-6893a2769bb1 amount=25000 channel=web source=v2+
{"eventId":"982585e6-e6c9-4ae4-990d-42c2014986e6","eventType":"lab08.order.created.v1","version":1,...,"data":{...,"amount":25000,"channel":"web"}}
...
dibaca: 4 event Avro
dilewati: 0 record yang bukan Avro Schema Registry
consumer memakai writer schema dari registry, termasuk field baru bila ada.
```

Topik sandbox hanya berisi Avro dari lab ini, jadi tidak ada record JSON yang
tercampur.

## 6. Consumer lama (reader schema v1) membaca event v2

```bash
$ node scripts/consume.js --old-reader
[consumer lama, skema v1] eventId=982585e6-e6c9-4ae4-990d-42c2014986e6 orderId=b03a3d95-cd50-427e-b5d7-6893a2769bb1 amount=25000 channel=(tidak ada di skema v1)
...
dibaca: 4 event Avro
dilewati: 0 record yang bukan Avro Schema Registry
consumer lama (reader schema v1) tetap membaca semua event di atas.
```

Event v2 (`channel`) dibaca tanpa error oleh reader v1; field baru diabaikan
oleh Avro schema resolution.

## 7. Perubahan breaking ditolak

```bash
$ node scripts/register.js avro/order-created-breaking.avsc
subject lab08.order.created.v1-value
status: DITOLAK registry
Schema being registered is incompatible with an earlier schema for subject "lab08.order.created.v1-value", details: [{errorType:"TYPE_MISMATCH", description:"The type (path '/fields/5/type/fields/3/type') of a field in the new schema does not match with the old schema", additionalInfo:"reader type: STRING not compatible with writer type: DOUBLE"}, {oldSchemaVersion: 2}, ...
$ echo $?
1
```

Versi tidak bertambah:

```bash
$ curl -s http://localhost:8081/subjects/lab08.order.created.v1-value/versions
[1,2]
```

## Kesimpulan

- Field opsional dengan default diterima (`versi 2`).
- Consumer lama (reader v1) tetap membaca event v2.
- Perubahan tipe `double` -> `string` ditolak registry dengan HTTP 409 /
  `TYPE_MISMATCH`, versi tetap `[1,2]`.
- Semua event Avro berada di topic sandbox `lab08.order.created.v1`, sehingga
  topic JSON `order.created.v1` tetap bersih untuk Lab 09 dan proyek.

## Uji ulang di stack bersih

Setelah `docker compose down -v` (Schema Registry kosong, topik sandbox belum
ada), seluruh urutan dijalankan lagi dan hasilnya sama: v1 id 1, v2 id 2,
consumer membaca 4 event Avro dengan 0 record dilewati, old reader membaca 4
event v2, breaking ditolak, `versions [1,2]`. `produce.js` membuat topik
`lab08.order.created.v1` dari nol pada run ini.
