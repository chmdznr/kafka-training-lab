# Lab 05: NestJS + Kafka

**Tujuan.** Membuat dua service NestJS: `order-service` menerima HTTP dan menerbitkan `order.created.v1`, `payment-service` mengonsumsi topic itu dengan commit manual. Di akhir lab, peserta bisa mematikan consumer dengan SIGTERM di tengah proses dan menjelaskan pesan mana yang diproses ulang.

**Waktu:** 65 menit.

## Dari mana mulai

- `start/` adalah titik awal. Ada penanda `// LATIHAN:` di tiga tempat yang harus diisi.
- `solution/` adalah versi lengkap dan bisa langsung dijalankan.
- Kalau tertinggal, pindah ke `solution/` dan lanjutkan ke langkah 3.

Perintah Node dijalankan dari folder kerja, yaitu `labs/05-nestjs/start/` atau `labs/05-nestjs/solution/`. Perintah `docker compose` dijalankan dari akar repo `lab/`.

## Prasyarat

1. Docker Compose dari Lab 01 menyala dan sehat:

   ```bash
   cd ../../..          # ke akar repo lab/
   docker compose ps
   docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
   ```

2. Node.js 24 (lihat `.nvmrc` di akar repo) dan dependensi terpasang. Pasang dependensi bersama dan binary native sekali dari akar repo `lab/`:

   ```bash
   cd ../../..          # ke akar repo lab/
   npm ci
   npm run setup:native
   ```

   `npm ci` memasang `@confluentinc/kafka-javascript`; `setup:native` mengunduh binary native-nya lewat GitHub API. `.npmrc` di akar mematikan install-script karena endpoint unduhan rilis mengembalikan 504 di sebagian jaringan. Lab ini memakai paket itu dari `node_modules` akar, bukan salinan sendiri.

   Lalu pasang dependensi Nest untuk lab ini:

   ```bash
   cd labs/05-nestjs/solution
   npm ci
   npm run build
   ```

3. Topic tidak dibuat otomatis (`auto.create.topics.enable=false`), jadi buat eksplisit:

   ```bash
   npm run create-topics
   ```

   Topic `order.created.v1` dibuat dengan 3 partisi, replication factor 3, dan `min.insync.replicas=2`.

## Langkah

### 1. Baca kontraknya dulu

Envelope dibuat oleh helper bersama di `lab/shared/envelope.js` (hasil Lab 03). Service memakai `createEnvelope`, `validateEnvelope`, dan `headersFor`. Bentuknya:

```json
{
  "eventId": "uuid",
  "eventType": "order.created.v1",
  "version": 1,
  "occurredAt": "2026-11-08T08:00:00.000Z",
  "correlationId": "uuid",
  "data": { "orderId": "uuid", "customerId": "cust-1", "amount": 125000, "currency": "IDR" }
}
```

Key record adalah `orderId`. Header berisi `correlation-id` dan `event-type`.

### 2. Isi bagian yang kosong (kalau mulai dari `start/`)

Di `src/order-service/orders.service.ts`, kirim record dengan key dan header:

```ts
this.kafka.emit(ORDER_CREATED_TOPIC, {
  key: orderId,
  value: envelope,
  headers: headersFor(envelope),
});
```

Di `src/payment-service/main.ts`, matikan commit otomatis:

```ts
run: { autoCommit: false },
```

Di `src/payment-service/payment.controller.ts`, commit offset setelah proses selesai:

```ts
await context.getConsumer().commitOffsets([
  {
    topic: context.getTopic(),
    partition: context.getPartition(),
    offset: (BigInt(context.getMessage().offset) + 1n).toString(),
  },
]);
```

Offset yang di-commit adalah **offset berikutnya**, karena itu `+ 1`.

### 3. Jalankan kedua service

Terminal 1:

```bash
npm run start:payment
```

Terminal 2:

```bash
npm run start:order
```

Tunggu sampai muncul `"event":"ready"` dari payment dan `"event":"listening"` dari order.

### 4. Kirim satu order

```bash
curl -s -X POST localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-1' \
  -d '{"customerId":"cust-1","amount":125000,"currency":"IDR"}'
```

Yang harus terlihat:

- Respons `202` berisi `orderId`, `eventId`, dan `correlationId`.
- Log order-service mencatat `"event":"order.created"`.
- Log payment-service mencatat `"event":"received"` lalu `"event":"committed"` untuk event yang sama.
- Baris baru di Postgres:

  ```bash
  cd ../..
  docker compose exec postgres psql -U app -d shop \
    -c "select event_id, order_id, amount, currency from payment_events order by id desc limit 3"
  ```

- Offset group `payment-service` sudah maju dan lag 0:

  ```bash
  docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:29092 --describe --group payment-service
  ```

### 5. Suntik kegagalan: SIGTERM saat memproses

Hentikan payment-service, lalu jalankan lagi dengan handler yang lambat (3 detik per pesan):

```bash
# terminal 1
PROCESSING_DELAY_MS=3000 npm run start:payment
```

Biarkan order-service tetap jalan, lalu kirim tiga order dari terminal lain:

```bash
for i in 1 2 3; do
  curl -s -X POST localhost:3000/orders \
    -H 'content-type: application/json' \
    -H "x-correlation-id: sigterm-$i" \
    -d "{\"customerId\":\"cust-$i\",\"amount\":$((i*10000)),\"currency\":\"IDR\"}"
  echo
done
```

Sekitar empat detik kemudian, kirim SIGTERM ke proses payment (ambil PID dari terminal 1 atau `pgrep -f payment-service/main.js`):

```bash
kill -TERM <pid-payment>
```

Amati:

1. Payment tidak langsung mati. Ia berhenti mengambil pesan baru, menyelesaikan pesan yang sedang diproses, meng-commit offset-nya, lalu berhenti. Exit code 143.
2. Setelah restart (`npm run start:payment`), pesan yang sudah di-commit tidak muncul lagi di log. Hanya pesan yang belum sempat di-commit yang diproses.

Bandingkan jumlah baris sebelum dan sesudah restart:

```bash
docker compose exec postgres psql -U app -d shop \
  -c "select count(*) as rows, count(distinct event_id) as events from payment_events"
```

Jumlah `rows` sama dengan `events`: tidak ada event yang diproses dua kali.

**Kenapa ada `ConsumerDrain`?** Saat SIGTERM, kafkajs menandai runner berhenti lebih dulu. Commit manual dari handler yang masih berjalan lalu diabaikan tanpa error. `src/common/consumer-drain.ts` berjalan sebelum transport ditutup: ia menghentikan fetch, menunggu handler yang sedang jalan, baru setelah itu Nest memutus consumer. Tanpa ini, pesan yang sedang diproses akan diproses ulang setelah restart walaupun log-nya sudah menulis `committed`. Ini contoh nyata kenapa graceful shutdown harus diuji, bukan diasumsikan.

### 6. Variasi: matikan paksa

Ulangi langkah 5, tetapi ganti `kill -TERM` dengan `kill -9`. Tidak ada drain dan tidak ada commit. Setelah restart, pesan yang sedang diproses muncul lagi dan tabel mendapat satu baris duplikat. Ini adalah **at-least-once**, dan alasan Lab 06 memakai `processed_events`.

## Selesai jika

Payment-service berhenti bersih setelah SIGTERM dan pesan yang sudah di-commit tidak diproses ulang setelah restart; jumlah baris `payment_events` sama dengan jumlah `event_id` unik.
