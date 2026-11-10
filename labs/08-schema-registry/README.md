# Lab 08 — Schema Registry

Lab ini menunjukkan evolusi skema Avro untuk event `order.created.v1`. Producer
dan consumer memakai serializer/deserializer dari
`@confluentinc/schemaregistry`; payload di Kafka berisi magic byte, schema id,
lalu body Avro (bukan JSON lagi).

Lab ini memakai topic sandbox sendiri, `lab08.order.created.v1`, bukan topic
`order.created.v1`. Topic itu dipakai lab lain dengan payload JSON; mencampur
byte Avro di sana akan membuat consumer JSON (Lab 09 dan proyek) menerima pesan
yang tidak bisa di-parse dan menganggapnya poison.

Tujuan:

1. Mendaftarkan skema Avro ke Schema Registry.
2. Menambah field opsional dengan default dan melihat registry menerimanya.
3. Membuktikan consumer lama (reader schema v1) tetap bisa membaca event yang
   ditulis dengan skema v2.
4. Melihat registry menolak perubahan yang breaking, lengkap dengan pesan
   error-nya.

## Prasyarat

- Stack lab menyala: `docker compose up -d` dari root repo `lab/`.
- Node.js 24 (`nvm use`). Dari root repo `lab/`, jalankan sekali:

  ```bash
  npm ci
  npm run setup:native
  ```

  Kedua perintah memasang dependency bersama dan binary native Kafka. Perintah
  Node di folder lab memakai `lab/node_modules`; tidak perlu `npm ci` di folder
  lab. Jangan menjalankan `npm install` di folder lab: `package.json` di sana
  hanya berisi script, dan `node_modules` lokal akan menutupi dependency root.
- Topik `lab08.order.created.v1` (dibuat otomatis oleh `scripts/produce.js` bila
  belum ada, 3 partisi, RF3, `min.insync.replicas=2`).

Semua perintah Node di bawah dijalankan dari
`lab/labs/08-schema-registry/solution`. Kalau mengerjakan latihan, jalankan dari
`.../start`; bagian yang harus diisi ditandai `// LATIHAN:` di
`scripts/register.js`, dan file `avro/order-created-v2.avsc` belum ada.
Folder `solution/` adalah referensi lengkap.

## 1. Daftarkan skema v1

```bash
cd labs/08-schema-registry/solution
node scripts/register.js avro/order-created-v1.avsc
```

Keluaran:

```text
subject lab08.order.created.v1-value
versi 1, schema id 1
status: DITERIMA registry
```

Subject bernama `lab08.order.created.v1-value` karena strategi penamaan default
adalah `<topic>-value`. Schema id berbeda di setiap instalasi.

## 2. Set compatibility BACKWARD

```bash
node scripts/set-compat.js BACKWARD
```

Keluaran: `compatibility lab08.order.created.v1-value: BACKWARD`.

Arti `BACKWARD`: skema baru harus bisa membaca data yang ditulis dengan skema
sebelumnya. Ini mode yang paling sering dipakai untuk consumer yang di-upgrade
lebih dulu.

## 3. Kirim event dengan skema v1

```bash
node scripts/produce.js --version 1 --count 2
```

Keluaran per event:

```text
terkirim: schema v1 id=1 orderId=<uuid> bytes=...
```

## 4. Tambah field opsional (v2)

Skema v2 menambah `data.channel` dengan `default: "web"`. Field baru **wajib**
punya default supaya data lama tetap bisa dibaca skema baru.

File referensi: `avro/order-created-v2.avsc`. Di folder `start/`, buat file
tersebut dari v1 lalu tambahkan field berikut di akhir record `OrderData`:

```json
{ "name": "channel", "type": "string", "default": "web" }
```

Daftarkan:

```bash
node scripts/register.js avro/order-created-v2.avsc
```

Keluaran yang diharapkan:

```text
subject lab08.order.created.v1-value
versi 2, schema id 2
status: DITERIMA registry
```

Registry menerima karena field baru punya default dan tipenya cocok.

## 5. Kirim event dengan skema v2

```bash
node scripts/produce.js --version 2 --count 2
```

Sekarang topik berisi event yang ditulis dengan schema id 1 dan id 2.

## 6. Consumer lama membaca event baru

Consumer dengan reader schema v1 hanya tahu field lama. Avro melakukan schema
resolution: field baru di writer diabaikan, field yang hilang di writer memakai
default reader.

```bash
node scripts/consume.js --old-reader
```

Keluaran yang diharapkan (per event, termasuk event v2):

```text
[consumer lama, skema v1] eventId=... orderId=... amount=25000 channel=(tidak ada di skema v1)
...
dibaca: 4 event Avro
consumer lama (reader schema v1) tetap membaca semua event di atas.
```

Jumlahnya bisa lebih besar kalau lab ini sudah pernah dijalankan sebelumnya,
karena consumer membaca topik dari awal.

Consumer biasa memakai writer schema dari registry, jadi ia melihat `channel`
pada event v2:

```bash
node scripts/consume.js
```

Topik sandbox ini hanya berisi event Avro dari lab ini. Skrip consumer tetap
melewati record yang bukan Avro Schema Registry sebagai pengaman, dan mencetak
jumlah yang dilewati di akhir.

## 7. Perubahan breaking ditolak

File `avro/order-created-breaking.avsc` mengubah tipe `data.amount` dari
`double` menjadi `string`. Di bawah `BACKWARD`, itu tidak kompatibel.

```bash
node scripts/register.js avro/order-created-breaking.avsc
```

Keluaran yang diharapkan:

```text
subject lab08.order.created.v1-value
status: DITOLAK registry
Schema being registered is incompatible with an earlier schema for subject "lab08.order.created.v1-value", details: [{errorType:"TYPE_MISMATCH", ...}]
```

Registry tetap menyimpan dua versi sebelumnya; schema id tidak bertambah.
Periksa dengan:

```bash
curl -s http://localhost:8081/subjects/lab08.order.created.v1-value/versions
```

Harus tetap `[1,2]`.

## Catatan penting tentang menghapus field

Menghapus field **tidak** ditolak oleh mode `BACKWARD`. Skema baru (reader)
mengabaikan field lama yang tidak dikenalnya, jadi skema baru masih bisa
membaca data lama. Yang rusak adalah arah sebaliknya: consumer dengan skema
lama tidak bisa membaca data baru yang tidak lagi punya field tersebut. Itu
masalah `FORWARD`, bukan `BACKWARD`.

Perubahan yang ditolak `BACKWARD` adalah yang membuat skema baru tidak bisa
membaca data lama, misalnya:

- mengubah tipe field (`double` menjadi `string`), seperti pada langkah 7;
- menambah field baru **tanpa** default;
- mempersempit union, misalnya `["null","string"]` menjadi `"string"`.

Kalau ingin membuktikan penghapusan field ditolak, ubah compatibility ke
`FORWARD` dulu (`node scripts/set-compat.js FORWARD`) lalu daftarkan skema tanpa
field tersebut.

## Selesai jika

Perubahan breaking ditolak registry dan consumer lama tetap membaca event versi
baru.

Bukti yang dikumpulkan:

1. Langkah 4 menghasilkan `status: DITERIMA registry` dengan versi 2.
2. Langkah 6 (`--old-reader`) mencetak event v2 tanpa error.
3. Langkah 7 menghasilkan `status: DITOLAK registry` dan daftar versi tetap
   `[1,2]`.
