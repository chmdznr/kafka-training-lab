# Lab 07 — Outbox dan transaksi

Lab ini menjawab pertanyaan: kalau database dan Kafka harus berubah bersama,
bagaimana menghindari dual-write?

Tujuan:

1. Menyimpan order dan baris outbox dalam satu transaksi Postgres.
2. Mem-publish event dari tabel outbox dengan poller `FOR UPDATE SKIP LOCKED`.
3. Membuktikan tidak ada order tanpa event dan tidak ada event tanpa order
   setelah Kafka dimatikan lalu dinyalakan.
4. Memakai transaksi Kafka untuk read-process-write: `order.created.v1` dibaca,
   `payment.completed.v1` ditulis, dan offset consumer di-commit di transaksi
   yang sama.

## Prasyarat

- Stack lab menyala: `docker compose up -d` dari root repo `lab/`.
- Node.js 24 (`nvm use`). Dari root repo `lab/`, jalankan sekali:

  ```bash
  npm ci
  npm run setup:native
  ```

  `npm ci` memasang dependency bersama (termasuk `pg`,
  `@confluentinc/kafka-javascript`, dan `@confluentinc/schemaregistry`) dan
  `setup:native` memasang binary native Kafka. Perintah Node di folder lab
  memakai `lab/node_modules`; tidak perlu `npm ci` di folder lab. Jangan
  menjalankan `npm install` di folder lab: `package.json` di sana hanya berisi
  script, dan `node_modules` lokal akan menutupi dependency root.
- Topic `order.created.v1` dan `payment.completed.v1` (3 partisi, RF3,
  `min.insync.replicas=2`) dibuat oleh langkah pertama di bawah. Cluster memakai
  `auto.create.topics.enable=false`, jadi topic tidak muncul otomatis.

Dua lokasi kerja dipakai di lab ini:

- Perintah Node dijalankan dari `lab/labs/07-outbox-transaksi/solution`
  (atau `.../start` kalau sedang mengerjakan bagian `// LATIHAN:`).
- Perintah `docker compose` dijalankan dari root repo `lab/`. Setiap terminal
  baru di bawah dianggap dibuka di root repo `lab/`.

## Bagian A — Outbox

### 1. Siapkan database dan topic

```bash
cd labs/07-outbox-transaksi/solution
npm run setup
```

Keluaran yang diharapkan:

```text
topic order.created.v1: sudah ada (atau dibuat kalau belum ada)
topic payment.completed.v1: dibuat
database dan topic siap
```

### 2. Jalankan order-service dan publisher

Terminal 1:

```bash
cd labs/07-outbox-transaksi/solution
npm run order-service
```

Terminal 2:

```bash
cd labs/07-outbox-transaksi/solution
npm run publisher
```

### 3. Buat satu order

Terminal 3:

```bash
curl -s -X POST http://localhost:3007/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"c-1","item":"kopi","amount":25000}'
```

Respons berisi `orderId` dan `correlationId`. Publisher akan menulis satu baris
log `outbox terkirim`.

### 4. Lihat event dan isi tabel outbox

```bash
# dari akar repo lab/
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic order.created.v1 \
  --from-beginning \
  --formatter-property print.key=true \
  --formatter-property key.separator=' | '
```

Key adalah `orderId`; value adalah envelope lengkap (`eventId`, `eventType`,
`version`, `occurredAt`, `correlationId`, `data`).

```bash
docker compose exec postgres psql -U app -d shop -c \
  "select aggregateid, published_at is not null as terkirim from outbox order by created_at desc limit 5;"
```

Semua baris harus `terkirim = t`.

### 5. Fault injection: Kafka mati saat order tetap masuk

Matikan ketiga broker. Order-service tetap bisa menulis ke Postgres.

```bash
# dari akar repo lab/
docker compose stop kafka-1 kafka-2 kafka-3
```

Buat tiga order:

```bash
for i in 1 2 3; do
  curl -s -X POST http://localhost:3007/orders \
    -H 'content-type: application/json' \
    -d "{\"customerId\":\"c-$i\",\"item\":\"teh\",\"amount\":15000}"
  echo
done
```

Lihat outbox: tiga baris baru masih pending sementara publisher mencatat
`publish gagal, baris outbox tetap pending dan akan dicoba lagi`.

```bash
docker compose exec postgres psql -U app -d shop -c \
  "select count(*) as pending from outbox where published_at is null;"
```

Nyalakan broker lagi:

```bash
docker compose start kafka-1 kafka-2 kafka-3
```

Publisher akan mengirim sendiri tanpa perlu di-restart. Tunggu sampai log
berhenti menampilkan error (sekitar 10–30 detik), lalu jalankan pengecekan:

```bash
cd labs/07-outbox-transaksi/solution
npm run check
```

Keluaran yang diharapkan:

```text
order di database      : 4
event lab ini          : 4 (key unik: 4)
event lain diabaikan   : 9    (event dari Lab 05 dan 06; jumlahnya bervariasi)
outbox belum terkirim  : 0
...
HASIL: OK - tidak ada order tanpa event, tidak ada event tanpa order
```

Skrip `check` membaca seluruh topic dari awal, membandingkan key event dengan
tabel `orders`, dan memastikan tidak ada baris outbox yang tertinggal. Topic
`order.created.v1` juga dipakai lab lain, jadi order-service menandai event
dengan `correlationId` berawalan `lab07-`; `check` hanya menghitung event
bertanda itu dan mengabaikan sisanya. Jumlah `order di database` bisa lebih
besar dari 4 kalau lab ini sudah pernah dijalankan sebelumnya. Kalau
sebelumnya sudah pernah menjalankan lab ini dan ingin mulai dari nol:

```bash
npm run reset -- --topics
```

Perintah itu mengosongkan tabel `orders` dan `outbox`, lalu menghapus dan
membuat ulang `order.created.v1` dan `payment.completed.v1`. Ini destruktif
untuk kedua topic tersebut; jangan jalankan saat lab lain sedang memakainya.
Tanpa `--topics`, tabel tetap dikosongkan tetapi event lama masih ada di topic,
sehingga `check` akan menganggapnya event tanpa order. Karena itu untuk mulai
dari nol selalu pakai `--topics`.

## Bagian B — Transaksi Kafka (read-process-write)

Consumer di bagian ini membaca `order.created.v1`, menulis
`payment.completed.v1`, dan meng-commit offset consumer di dalam transaksi
Kafka. Producer memakai `transactional.id`, consumer memakai
`read_committed` dan `autoCommit: false`.

### 6. Jalankan payment-tx

Terminal baru (biarkan publisher tetap jalan atau hentikan, tidak dipakai di
sini):

```bash
cd labs/07-outbox-transaksi/solution
npm run payment-tx
```

Karena `fromBeginning: true`, semua order yang sudah ada akan diproses satu per
satu. Setiap baris log berarti event payment dan offset sudah di-commit
bersama.

### 7. Lihat hasil dan offset

```bash
cd ../../..          # dari solution/ ke akar repo lab/
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic payment.completed.v1 \
  --from-beginning \
  --isolation-level read_committed \
  --formatter-property print.key=true \
  --formatter-property key.separator=' | '

docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --describe --group payment-tx
```

`payment.completed.v1` berisi `causationId` yang menunjuk `eventId` order asal.
Kolom `LAG` untuk group `payment-tx` harus bernilai `0` pada partisi yang punya
record; partisi yang belum pernah menerima record bisa menampilkan `-`. Record
yang bukan `order.created.v1` valid (misalnya data uji) dilewati dengan log
peringatan, tetapi offset-nya tetap di-commit lewat `sendOffsets` dalam
transaksi Kafka. Dengan begitu record rusak tidak diproses dan lag tidak
tertahan.

### 8. Bukti transaksi abort

```bash
cd labs/07-outbox-transaksi/solution
npm run abort-demo
```

Skrip menulis dua transaksi ke topic `tx-demo.v1`: satu di-abort, satu
di-commit. Lalu dua consumer membaca topic yang sama dengan isolation berbeda.

```text
read_committed   melihat : [committed]
read_uncommitted melihat : [aborted, committed]
HASIL: OK - record dari transaksi abort tidak terlihat oleh read_committed, terlihat oleh read_uncommitted
```

`read_committed` hanya melihat record dari transaksi yang sudah commit.
`read_uncommitted` juga melihat record dari transaksi yang abort. Ini alasan
consumer produksi memakai `read_committed` ketika producer memakai transaksi.

## Catatan

- Kolom `outbox.id` memakai `eventId` dari envelope, bukan UUID baru. Dengan
  begitu event yang sama punya satu id di database, di payload, dan (untuk demo
  Debezium) di header Kafka `id`. Consumer bisa memakai salah satunya untuk
  dedup.
- Outbox memberi at-least-once, bukan exactly-once. Kalau proses mati setelah
  `producer.send` sukses tetapi sebelum `COMMIT` database, baris yang sama akan
  dikirim ulang. Consumer harus idempotent (Lab 06).
- `FOR UPDATE SKIP LOCKED` membuat dua publisher bisa berjalan bersamaan tanpa
  saling menunggu atau mengirim baris yang sama.
- Producer di lab ini idempotent dan `acks=all`. Kalau broker mati, `send()`
  gagal setelah `message.timeout.ms` (5 detik), transaksi database di-rollback,
  dan baris outbox dicoba lagi pada loop berikutnya. Tidak ada baris yang
  ditandai terkirim sebelum Kafka menerimanya.
- Alternatif polling adalah CDC: Debezium membaca WAL Postgres dan mem-publish
  baris outbox. Demo instruktur ada di `lab/demo/debezium/`.
- Transaksi Kafka hanya berlaku untuk topic di cluster yang sama. Menulis ke
  Postgres di dalam transaksi Kafka bukan transaksi dua fase; itulah sebabnya
  Lab 06 memakai idempotent consumer, dan lab ini memakai outbox untuk arah
  database → Kafka.
- Log `group coordinator is not available` beberapa detik setelah cluster
  menyala adalah normal, sama seperti di Lab 01.

## Selesai jika

Tidak ada order tanpa event dan tidak ada event tanpa order setelah Kafka
dimatikan lalu dinyalakan.

Bukti yang dikumpulkan:

1. `npm run check` berakhir `HASIL: OK`.
2. Sebelum broker dinyalakan, query outbox menunjukkan baris `pending > 0`.
3. Setelah broker dinyalakan, query yang sama menunjukkan `pending = 0`.
4. `npm run abort-demo` menunjukkan `read_committed` tidak melihat record yang
   di-abort.
