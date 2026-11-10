# Lab 09: Observability dan koneksi aman

**Tujuan.** Menambahkan `correlationId` dan `traceparent` ke header dan log JSON di kedua service, memperlambat consumer untuk melihat lag naik lalu pulih, dan menyambungkan klien ke listener SASL/SCRAM dengan kredensial salah dan benar.

**Waktu:** 25 menit.

Lab ini melanjutkan Lab 06. Semua perintah dijalankan dari `labs/09-observability-security/solution/`, kecuali perintah `docker compose` yang dijalankan dari akar repo `lab/`.

## Dari mana mulai

- `start/` adalah salinan Lab 06 dengan helper observability. Bagian bertanda `// LATIHAN:` harus diisi:
  1. `src/order-service/orders.controller.ts` dan `src/order-service/orders.service.ts`: baca header `traceparent` dari HTTP, buat nilai baru bila tidak ada, kirim sebagai header Kafka, lalu catat di log.
  2. `src/payment-service/kafka-helpers.ts` dan `src/payment-service/retry-policy.ts`: baca `traceparent` dari header Kafka, catat di log, dan teruskan ke retry topic serta DLT.
  3. `scripts/sasl-check.js`: tambahkan `ssl: false` dan objek `sasl` dengan mekanisme SCRAM-SHA-512.
- `solution/` adalah versi lengkap.

Kalau tertinggal, pakai `solution/` dan ikuti langkah di bawah. Kalau mengerjakan `start/`, jalankan perintah yang sama dari folder `start/`.

## Prasyarat

1. Docker Compose dari Lab 01 menyala dan sehat. Jaeger tidak diperlukan untuk lab ini.

   ```bash
   cd ../../..          # ke akar repo lab/
   docker compose ps
   ```

2. Node.js 24 (lihat `.nvmrc` di akar repo). Pasang dependensi bersama dan binary native sekali dari akar repo `lab/`:

   ```bash
   npm ci
   npm run setup:native
   ```

   `npm ci` memasang `@confluentinc/kafka-javascript`; `setup:native` mengunduh binary native-nya lewat GitHub API. `.npmrc` di akar mematikan install-script karena endpoint unduhan rilis mengembalikan 504 di sebagian jaringan. Lab ini memakai paket itu dari `node_modules` akar, bukan salinan sendiri.

   Lalu pasang dependensi Nest untuk lab ini:

   ```bash
   cd labs/09-observability-security/solution
   npm ci
   npm run build
   ```

3. Buat topic secara eksplisit (`auto.create.topics.enable=false`):

   ```bash
   npm run create-topics
   ```

   Perintah ini membuat `order.created.v1`, `order.created.v1.payment-service.retry`, dan `order.created.v1.payment-service.dlt` dengan 3 partisi, replication factor 3, dan `min.insync.replicas=2`.

## Langkah

### 1. Jalankan kedua service

Terminal 1:

```bash
npm run start:payment
```

Terminal 2:

```bash
npm run start:order
```

Tunggu sampai muncul `"event":"ready"` dari payment-service dan `"event":"listening"` dari order-service.

### 2. Telusuri satu order lewat correlationId

Kirim satu order dengan `correlationId` dan `traceparent` dari luar:

```bash
curl -s -X POST localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: lab09-correlation-1' \
  -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' \
  -d '{"customerId":"cust-lab09","amount":125000,"currency":"IDR"}'
```

Respons berisi `orderId`, `eventId`, `correlationId`, dan `traceparent` yang dipakai. Nilainya bisa berbeda setiap kali, tetapi `correlationId` dan `traceparent` di respons harus sama dengan yang ada di log.

Log order-service:

```json
{"level":"info","service":"order-service","event":"order.created","orderId":"...","eventId":"...","correlationId":"lab09-correlation-1","traceparent":"00-4bf92f...-01","topic":"order.created.v1","partition":0,"offset":"0"}
```

Log payment-service menampilkan dua baris untuk event yang sama:

```json
{"level":"info","service":"payment-service","event":"received","topic":"order.created.v1","partition":0,"offset":"0","eventId":"...","correlationId":"lab09-correlation-1","orderId":"...","traceparent":"00-4bf92f...-01"}
{"level":"info","service":"payment-service","event":"processed","topic":"order.created.v1","partition":0,"offset":"0","eventId":"...","correlationId":"lab09-correlation-1","orderId":"...","traceparent":"00-4bf92f...-01"}
```

Perhatikan bahwa `topic`, `partition`, dan `offset` berasal dari record Kafka, bukan dari isi envelope. Di sisi producer, offset diambil dari metadata hasil `emit`. Di sisi consumer, offset diambil dari `KafkaContext`.

`correlationId` dan `traceparent` dibuat otomatis bila klien tidak mengirimnya. Coba kirim order tanpa kedua header, lalu cocokkan nilai di respons dengan log kedua service:

```bash
curl -s -X POST localhost:3000/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-tanpa-header","amount":50000}'
```

### 3. Lihat header yang ikut di record Kafka

```bash
cd ../../..
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic order.created.v1 \
  --from-beginning --timeout-ms 5000 \
  --formatter-property print.headers=true 2>/dev/null | grep lab09-correlation-1
```

Header yang harus terlihat: `correlation-id`, `event-type`, dan `traceparent`. Nilai `traceparent` berformat W3C: `00-<32 hex trace-id>-<16 hex span-id>-<2 hex flags>`.

Kembali ke folder solution sebelum lanjut:

```bash
cd labs/09-observability-security/solution
```

### 4. Perlambat consumer dan amati lag

Hentikan payment-service dengan `Ctrl+C` atau `kill -TERM <pid>`, lalu jalankan lagi dengan delay 2 detik per pesan:

```bash
PROCESSING_DELAY_MS=2000 npm run start:payment
```

Kirim 24 order berurutan dari terminal lain:

```bash
for i in $(seq 1 24); do
  curl -s -X POST localhost:3000/orders \
    -H 'content-type: application/json' \
    -H "x-correlation-id: lab09-lag-$i" \
    -d "{\"customerId\":\"cust-lag-$i\",\"amount\":$((i*1000))}" >/dev/null
done
```

Lihat lag group `payment-service`:

```bash
cd ../../..
docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --describe --group payment-service
```

Kolom `LAG` akan lebih besar dari 0 dan turun perlahan. Consumer memakai `partitionsConsumedConcurrently: 1` dan key record adalah `orderId`, jadi pesan diproses satu partisi pada satu waktu. Kafka UI di `localhost:8080` menampilkan group yang sama di menu **Consumers** dengan grafik lag.

Hentikan payment-service yang lambat. Kembali ke folder solution, lalu jalankan lagi tanpa `PROCESSING_DELAY_MS`:

```bash
cd labs/09-observability-security/solution
npm run start:payment
```

Setelah beberapa detik, `LAG` kembali 0 di ketiga partisi:

```bash
cd ../../..
docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --describe --group payment-service
```

Kembali ke folder solution sebelum lanjut:

```bash
cd labs/09-observability-security/solution
```

### 5. Koneksi SASL/SCRAM

Script `scripts/sasl-check.js` memakai `@confluentinc/kafka-javascript` dan menyambung ke listener SASL_PLAINTEXT di `localhost:9192`, `localhost:9193`, dan `localhost:9194`. User dan password berasal dari service `kafka-init` di compose: `app` / `app-secret`.

Coba dengan password salah:

```bash
node scripts/sasl-check.js --password salah
```

Klien mencetak error dari librdkafka, lalu keluar dengan status 1:

```text
SASL authentication error: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512
{"event":"sasl-check-failed","error":"KafkaJSConnectionError","message":"Local: Broker transport failure"}
```

Baris pertama bisa muncul beberapa kali, sekali untuk setiap broker dan percobaan ulang. Penyebabnya tetap satu: kredensial ditolak broker.

Coba dengan password benar:

```bash
node scripts/sasl-check.js
```

```json
{"event":"produced","topic":"lab09.sasl-check.v1","partition":2,"offset":"1"}
{"event":"consumed","topic":"lab09.sasl-check.v1","partition":2,"offset":"1","username":"app","mechanism":"SCRAM-SHA-512"}
{"event":"sasl-check-ok","brokers":["localhost:9192","localhost:9193","localhost:9194"],"username":"app","mechanism":"SCRAM-SHA-512"}
```

Script membuat topic `lab09.sasl-check.v1` bila belum ada, mengirim satu pesan, lalu membacanya kembali dengan consumer group baru.

Di production, listener memakai `SASL_SSL`, bukan `SASL_PLAINTEXT`. Di klien tambahkan `ssl: true`; mekanisme SASL dan kredensialnya sama. Kalau sertifikat broker ditandatangani CA internal, klien juga perlu lokasi CA lewat properti librdkafka `ssl.ca.location` di luar blok `kafkaJS`:

```ts
const kafka = new Kafka({
  kafkaJS: {
    ssl: true,
    sasl: { mechanism: 'scram-sha-512', username, password },
  },
  'ssl.ca.location': '/path/ke/ca.pem',
});
```

Listener di lab ini sengaja tanpa TLS supaya tidak perlu membuat sertifikat.

### 6. Opsional: traceparent saat pesan gagal

Jalankan retry worker di terminal ketiga:

```bash
npm run start:retry
```

Kirim order yang pasti gagal validasi bisnis (di atas `payment_limits.max_amount`):

```bash
curl -s -X POST localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: lab09-retry-1' \
  -H 'traceparent: 00-aaaabbbbccccddddeeeeffff00001111-2222333344445555-01' \
  -d '{"customerId":"cust-retry","amount":50000000,"currency":"IDR"}'
```

Payment-service mencatat `retry-scheduled`, retry worker mencatat `dead-lettered`, dan header di retry topic serta DLT tetap membawa `correlation-id` dan `traceparent` yang sama:

```bash
cd ../../..
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic order.created.v1.payment-service.dlt \
  --from-beginning --timeout-ms 5000 \
  --formatter-property print.headers=true 2>/dev/null | grep lab09-retry-1
```

### 7. Jaeger tidak disertakan

Lab ini berhenti di propagasi `traceparent` manual. Instrumentasi OpenTelemetry ke Jaeger tidak disertakan karena belum diverifikasi end to end di lab ini, dan menambahkan SDK tanpa auto-instrumentasi hanya akan mengulang pekerjaan yang sudah dilakukan helper `traceparent`. Profile `tracing` tetap tersedia di compose kalau ingin mencoba sendiri:

```bash
docker compose --profile tracing up -d
```

## Kalau mulai dari start/

Isi semua bagian bertanda `// LATIHAN:` berikut, lalu `npm run build` dan jalankan seperti di atas.

`src/order-service/orders.controller.ts`:

```ts
create(
  @Body() body: Partial<OrderInput>,
  @Headers('x-correlation-id') correlationId?: string,
  @Headers('traceparent') traceparent?: string,
) {
  return this.orders.accept(body, correlationId, traceparent);
}
```

`src/order-service/orders.service.ts` (tambahkan parameter `incomingTraceparent?: string` pada `accept`):

```ts
const traceparent = traceparentFrom(incomingTraceparent);
// ...
headers: headersFor(envelope, { traceparent }),
// di logLine:
logLine('order-service', 'order.created', { /* ... */ traceparent }),
```

`src/payment-service/kafka-helpers.ts` (tambahkan juga `import { headerValue } from '../common/kafka.js'`):

```ts
traceparent: headerValue(source.headers, 'traceparent'),
```

`src/payment-service/retry-policy.ts`:

```ts
const traceparent = headerValue(source.headers, 'traceparent');
// di header retry dan DLT:
...(traceparent ? { traceparent } : {}),
```

`scripts/sasl-check.js`:

```ts
ssl: false,
sasl: { mechanism: 'scram-sha-512', username, password },
```

## Baca ulang kode yang penting

- `src/common/observability.ts`: pembuatan `traceparent` W3C dan format satu baris log.
- `src/order-service/orders.service.ts`: membaca header HTTP, membuat nilai yang hilang, dan mengambil `topic`, `partition`, `offset` dari metadata producer.
- `src/payment-service/kafka-helpers.ts`: `topic`, `partition`, `offset`, `correlationId`, dan `traceparent` dari record Kafka.
- `src/payment-service/retry-policy.ts`: `traceparent` ikut ke retry topic dan DLT.
- `scripts/sasl-check.js`: konfigurasi SASL, produce, dan consume dengan `@confluentinc/kafka-javascript`.

## Selesai jika

Satu order bisa ditelusuri lewat `correlationId` yang sama di log order-service dan payment-service, dengan `traceparent`, `topic`, `partition`, dan `offset` yang juga tercatat, serta klien gagal dengan kredensial salah (`SASL authentication error ... invalid credentials`) dan berhasil produce lalu consume satu pesan dengan kredensial benar.
