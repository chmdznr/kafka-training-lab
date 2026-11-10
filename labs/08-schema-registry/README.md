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
5. Melihat bahwa menghapus field lolos di `BACKWARD` tetapi ditolak di
   `FORWARD`, karena yang dirusak adalah consumer lama.

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
`avro/order-created-removed-field.avsc` dan `scripts/check-compat.js` sudah
tersedia di kedua folder.
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

## 8. Hapus field: BACKWARD menerima, FORWARD menolak

File `avro/order-created-removed-field.avsc` sama dengan v2, tetapi field
`data.customerId` dihapus. Langkah ini tidak mendaftarkan skema tersebut di
`BACKWARD`. Skrip `scripts/check-compat.js` hanya menguji kompatibilitas
terhadap versi terakhir (v2) memakai level compatibility subject saat ini,
tanpa menambah versi.

Uji di `BACKWARD`:

```bash
node scripts/check-compat.js avro/order-created-removed-field.avsc
```

Keluaran:

```text
subject lab08.order.created.v1-value, compatibility BACKWARD
avro/order-created-removed-field.avsc vs versi terakhir: KOMPATIBEL
```

`BACKWARD` menerima karena skema baru (reader) cukup mengabaikan `customerId`
pada data lama.

Sekarang ubah ke `FORWARD`, lalu uji dan coba daftarkan skema yang sama:

```bash
node scripts/set-compat.js FORWARD
node scripts/check-compat.js avro/order-created-removed-field.avsc
node scripts/register.js avro/order-created-removed-field.avsc
```

Keluaran:

```text
compatibility lab08.order.created.v1-value: FORWARD
subject lab08.order.created.v1-value, compatibility FORWARD
avro/order-created-removed-field.avsc vs versi terakhir: TIDAK KOMPATIBEL
subject lab08.order.created.v1-value
status: DITOLAK registry
Schema being registered is incompatible with an earlier schema for subject "lab08.order.created.v1-value", details: [{errorType:"READER_FIELD_MISSING_DEFAULT_VALUE", description:"The field 'customerId' at path '/fields/5/type/fields/1' in the old schema has no default value and is missing in the new schema", ...}]
```

`FORWARD` menuntut skema lama bisa membaca data yang ditulis skema baru.
Consumer lama masih membutuhkan `customerId`, dan field itu tidak punya default
di skema lama, jadi data tanpa `customerId` tidak bisa dibacanya.

Urutannya penting. Uji `FORWARD` selagi versi terakhir masih punya
`customerId`. Kalau skema tanpa `customerId` sudah telanjur didaftarkan di
`BACKWARD`, pemeriksaan `FORWARD` berikutnya membandingkan dengan versi tanpa
field itu dan bisa lolos.

Kembalikan ke `BACKWARD` dan pastikan versi tidak bertambah:

```bash
node scripts/set-compat.js BACKWARD
curl -s http://localhost:8081/subjects/lab08.order.created.v1-value/versions
```

Keluaran: `compatibility lab08.order.created.v1-value: BACKWARD`, lalu `[1,2]`.
`node scripts/consume.js --old-reader` tetap membaca semua event seperti di
langkah 6.

Perubahan lain yang ditolak `BACKWARD` adalah yang membuat skema baru tidak
bisa membaca data lama, misalnya:

- mengubah tipe field (`double` menjadi `string`), seperti pada langkah 7;
- menambah field baru **tanpa** default;
- mempersempit union, misalnya `["null","string"]` menjadi `"string"`.

## Selesai jika

Perubahan breaking ditolak registry, consumer lama tetap membaca event versi
baru, dan hapus field lolos di `BACKWARD` tetapi ditolak di `FORWARD`.

Bukti yang dikumpulkan:

1. Langkah 4 menghasilkan `status: DITERIMA registry` dengan versi 2.
2. Langkah 6 (`--old-reader`) mencetak event v2 tanpa error.
3. Langkah 7 menghasilkan `status: DITOLAK registry` dan daftar versi tetap
   `[1,2]`.
4. Langkah 8 menghasilkan `KOMPATIBEL` di `BACKWARD`, `DITOLAK registry`
   (`READER_FIELD_MISSING_DEFAULT_VALUE`) di `FORWARD`, compatibility kembali
   `BACKWARD`, dan daftar versi tetap `[1,2]`.
