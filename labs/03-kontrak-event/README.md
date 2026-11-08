# Lab 03 — Desain event dan kontrak

Tujuan: menyepakati bentuk event sebelum menulis producer dan consumer, lalu
menegakkannya dengan helper envelope yang dipakai semua service.

Durasi: 40 menit.

## Prasyarat

- Stack menyala dan topic `order.created.v1` sudah dibuat (Lab 01).
- Semua perintah dijalankan dari folder lab ini, `lab/labs/03-kontrak-event/`.
  Perintah `docker compose` juga jalan dari sini karena Compose mencari
  `docker-compose.yml` di folder induk.

Lab ini satu-satunya yang menulis ke `order.created.v1`, karena payload-nya
mengikuti kontrak. Satu event rusak tetap dikirim dengan sengaja untuk
membuktikan consumer menolaknya. Lab 01, 02, dan 04 memakai topic sandbox
masing-masing.

## Alur latihan

Kerjakan `start/` dulu, bukan `solution/`:

- `start/kontrak-event.md` — isi template kontrak (topic, key, envelope, tiga
  event, contoh payload).
- `start/envelope-check.js` — lengkapi pemeriksaan envelope.
- `start/publish-order.js` — lengkapi penggunaan envelope untuk produce dan
  consumer yang menolak event rusak.

Jalankan:

```bash
node start/envelope-check.js
node start/publish-order.js
```

Sebelum diisi, keduanya berhenti dengan pesan `LATIHAN` (bukan syntax error).
Kalau tertinggal, gunakan file di `solution/` sebagai cadangan.

## 1. Baca kontrak bersama

Buka `shared/envelope.js`. Perhatikan aturan mainnya:

- `eventType` sama dengan nama topic, termasuk versi (`order.created.v1`).
- `eventId` wajib UUID; `correlationId` wajib diisi.
- `occurredAt` wajib ISO-8601 dengan zona waktu.
- `data` wajib object.
- Header `correlation-id` dan `event-type` diturunkan dari envelope.

Buka template `labs/03-kontrak-event/start/kontrak-event.md`, lalu isi bagian
`LATIHAN`: topic, key, envelope, tiga event (`order.created.v1`,
`payment.completed.v1`, `payment.failed.v1`), dan contoh payload. Kontrak yang
sudah jadi ada di `solution/kontrak-event.md`; bandingkan setelah selesai.

Checklist review (cek sendiri, lalu tukar kontrak dengan teman sebelah):

- Nama topic mengikuti `<domain>.<event>.v<versi>`.
- Key `orderId` dipakai konsisten untuk event order dan payment.
- Field `data` setiap event punya tipe dan kewajiban yang jelas.
- Event turunan memakai `causationId` dari `eventId` event sebelumnya.

## 2. Jalankan pemeriksaan envelope

Setelah `start/envelope-check.js` diisi, jalankan:

```bash
node start/envelope-check.js
```

Cadangan: `node solution/envelope-check.js`.

Yang harus terlihat: envelope valid diterima, lalu setiap kasus rusak ditolak
dengan pesan yang menyebut field bermasalah. Kasus mínimum yang wajib:

- event tanpa `eventId`
- event tanpa `correlationId`
- `eventType` yang tidak mengikuti pola
- `occurredAt` tanpa zona waktu
- `data` yang bukan object


## 3. Terbitkan event dengan envelope

Setelah `start/publish-order.js` diisi, jalankan:

```bash
node start/publish-order.js
```

Cadangan: `node solution/publish-order.js`.

Skrip ini membuat satu event valid dan satu event rusak (tanpa
`correlationId`), mengirim keduanya ke `order.created.v1`, lalu membacanya
kembali. Event valid lolos `validateEnvelope`; event rusak ditolak consumer.

## 4. Fault injection: broker tidak memvalidasi payload

Kafka hanya menyimpan bytes. Buktikan bahwa record rusak tetap masuk ke topic:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 --topic order.created.v1 \
  --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true | grep -c correlationId
```

Angka yang muncul bukan bukti validasi; record tanpa `correlationId` tetap
tersimpan. Yang mencegah data rusak diproses adalah `validateEnvelope` di
consumer. Ini alasan kontrak dan validasi harus ada di kode, bukan di broker.

## Selesai jika

1. `validateEnvelope` menolak event tanpa `eventId` dan tanpa `correlationId`
   (ditunjukkan oleh `start/envelope-check.js` atau `solution/envelope-check.js`).
2. Kontrak `order.created.v1`, `payment.completed.v1`, dan `payment.failed.v1`
   sudah lengkap dan memenuhi checklist review di atas (dicek bersama teman
   sebelah).
