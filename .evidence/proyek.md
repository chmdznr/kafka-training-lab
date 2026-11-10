# Evidence — Mini-proyek (order, payment, notification)

Dijalankan: 2026-11-08, compose `kafka-training-lab` (3 broker sehat, Postgres 17).
Working dir: `proyek/solution`. Node 24.22.1 (`.nvmrc`), karena binary native
`@confluentinc/kafka-javascript` di akar `lab/` dibangun untuk ABI Node 24
(`NODE_MODULE_VERSION 137`).

Dependensi lab memakai `node_modules` di akar `lab/` (`npm ci` +
`npm run setup:native`). `npm ci` di folder proyek hanya memasang
dependensi NestJS, kafkajs, pg, dan Testcontainers.

## Build, topik, start

```
$ npm run build
> tsc -p tsconfig.json          (exit 0)

$ npm run create-topics
topic order.created.v1 sudah ada
topic payment.completed.v1 sudah ada
topic payment.failed.v1 sudah ada
topic order.created.v1.payment-service.retry sudah ada
topic order.created.v1.payment-service.dlt sudah ada

$ scripts/start-all.sh
payment pid 77408
retry pid 77413
notification pid 77416
order pid 77503
semua service siap (log di .../proyek/solution/.run/*.log)
```

Verifikasi PID: setiap PID di `.run/*.pid` adalah proses `node dist/...`
langsung, sehingga `stop-all.sh` dan `fault-consumer-crash.sh` benar-benar
menghentikan service.

## 1. `fault-consumer-crash.sh`

Payment dijalankan dengan `PROCESSING_DELAY_MS=3000`, order dikirim, lalu
payment dibunuh dengan `kill -9` saat pesan sedang diproses. Payment
dinyalakan ulang dan pesan yang belum di-commit diproses lagi.

```
1) payment dijalankan ulang dengan handler lambat (3 detik)
payment pid 77617
2) buat order, lalu bunuh payment saat pesan sedang diproses
   orderId=3c41d106-81af-4985-8be7-484d258c4c66
   payment pid 77617 dibunuh dengan SIGKILL
3) nyalakan payment lagi tanpa delay
payment pid 77643
OK: order 3c41d106-81af-4985-8be7-484d258c4c66 berstatus PAID
4) verifikasi efeknya tepat sekali
OK: baris payments = 1
OK: baris notifications = 1
OK: baris proyek_processed_events = 1
selesai: crash tidak menggandakan pembayaran dan order tetap PAID
```

## 2. `fault-duplicate.sh`

```
orderId=3e3b1829-79ce-4b82-bf6e-8a5a9b3c4444
eventId=a4dbcafa-7ae3-4948-a7cf-3de2c7308a2a (dikirim dua kali)
OK: baris payments = 1
OK: baris proyek_processed_events = 1
OK: baris notifications = 1
selesai: duplikat diproses sekali
```

## 3. `fault-poison.sh`

```
orderId=26f5186f-9ab3-401d-abaa-630a2b1f0769
eventId=37b324ac-7094-48cb-97c4-dc7fe3b8f262 (amount negatif, tidak mungkin valid)
OK: 37b324ac-7094-48cb-97c4-dc7fe3b8f262 ada di DLT
OK: baris payments untuk pesan rusak = 0
selesai: pesan rusak ada di DLT dan tidak membuat pembayaran
```

## 4. `fault-broker-down.sh`

```
1) matikan ketiga broker
2) terima order saat Kafka mati
   orderId=42beef5b-7c3a-4540-b010-adaa4193b1de
3) order tersimpan, event belum terkirim
OK: outbox belum terkirim = 1
OK: status order saat Kafka mati = PENDING
4) nyalakan broker lagi dan tunggu cluster siap
5) poller mengirim event, pembayaran selesai
OK: order 42beef5b-7c3a-4540-b010-adaa4193b1de berstatus PAID
OK: outbox terkirim = 1
OK: baris payments = 1
OK: baris notifications = 1
selesai: tidak ada order tanpa event dan tidak ada event tanpa order
```

## Integration test

```
$ npm test
> npm run build && node --test test/integration.test.js

{"service":"payment-service","event":"processed","outcome":"paid",...}
{"service":"order-service","event":"order.status-applied","status":"PAID","changed":true,...}
{"service":"notification-service","event":"notification-stored",...}
{"event":"duplicate-skipped",...}   (dua kali untuk event yang sama)
{"event":"retry-scheduled","retryCount":1,...}
{"event":"retry-scheduled","retryCount":2,...}
{"event":"dead-lettered","retryCount":2,...}

✔ order -> payment -> notification dengan idempotency dan DLT (24253ms)
ℹ tests 1
ℹ pass 1
ℹ fail 0
```

Test menyalakan `confluentinc/cp-kafka:8.4.1` dan `postgres:17` lewat
Testcontainers, membuat topic RF 1, lalu memeriksa order `PAID`, duplikat yang
tidak menambah baris, dan poison message yang masuk DLT.

## Perbaikan yang ditemukan saat menjalankan

Semua di bawah ini adalah bug nyata yang terlihat saat run pertama dan sudah
diperbaiki di `solution/`:

1. `payment.service.ts` membuat tabel `payments`, tetapi seluruh kode dan test
   memakai `proyek_payments`. Schema diganti ke `proyek_payments` supaya
   konsisten dengan prefix proyek dan tidak bentrok dengan tabel Lab 06.
2. `notification-service` tidak punya route `/health`, sehingga
   `start-all.sh` selalu gagal menunggu service ini. Route ditambahkan.
3. `start_proc()` mencatat PID subshell, bukan PID node. Akibatnya `stop-all`
   dan `kill -9` pada fault crash tidak mematikan proses. Subshell sekarang
   `exec` ke proses node supaya PID yang dicatat benar.
4. `wait_http()` hanya menunggu 20 detik; anggota consumer group yang hilang
   butuh satu session timeout sebelum pengganti bisa join. Default dinaikkan
   ke 60 detik.
5. Test memakai `node --test test/`, yang tidak menerima direktori pada Node
   24. Diganti ke file test eksplisit.
6. `NestFactory.create()` tidak menjalankan lifecycle hook. `bootHybrid()`
   sekarang memanggil `app.init()` supaya `onModuleInit` membuat tabel di
   Postgres Testcontainers.
7. `brokers` di test harus array; sebelumnya string.
8. Pesan duplikat di test memakai objek yang sama dua kali. Klien mengubah
   `headers` di tempat, jadi kiriman kedua mengirim array header yang sudah
   dikonversi. Setiap kiriman sekarang membuat record baru.
9. `dlt-check.js` menaruh `groupId`/`fromBeginning` di luar blok `kafkaJS`.
   Sudah dipindah sesuai API `@confluentinc/kafka-javascript`.
10. Path `shared/envelope.js` di skrip `send-*.js` kurang satu `../`.

## Run ulang 2026-11-11

NestJS `12.4.2`, Testcontainers `confluentinc/cp-kafka:8.4.1`.

```
npm ci / npm run build: exit 0 (start/ dan solution/)
order cust-1 amount 125000  -> PAID
order cust-big amount 2000000 -> FAILED
proyek_notifications: payment.failed.v1 "... gagal", payment.completed.v1 "... berhasil"

fault-consumer-crash: OK PAID, payments 1, notifications 1, processed_events 1   exit 0
fault-duplicate:      OK payments 1, processed_events 1, notifications 1         exit 0
fault-poison:         OK ada di DLT, payments 0                                  exit 0
fault-broker-down:    OK PAID, outbox terkirim 1, payments 1, notifications 1    exit 0
stop-all: exit 0

npm test
✔ order -> payment -> notification dengan idempotency dan DLT (26286ms)
ℹ tests 1  ℹ pass 1  ℹ fail 0
```

`test/integration.test.js` di `solution/` kini punya kasus keempat: order
dengan `amount` 2000000 menjadi `FAILED`, satu baris `proyek_payments`
berstatus `failed`, satu notifikasi `payment.failed.v1` berisi "gagal". Di log
test: `order.status-applied ... "status":"FAILED"` dan
`notification-stored ... gagal`.

Uji mutasi (salinan sementara dengan `'FAILED'` diganti `'PAID'` pada kasus
keempat, lalu dihapus):

```
ℹ pass 0
ℹ fail 1
  Error: timeout menunggu order FAILED; hasil terakhir: false
```

## Run ulang 2026-11-11 (putaran 2)

`proyek_processed_events` kini berkunci `(consumer_group, event_id)`;
payment-service (dan retry worker) mengklaim dengan `payment-service`,
notification-service dengan `notification-service`.

```
\d proyek_processed_events
 consumer_group text not null | event_id uuid not null | topic text not null | processed_at timestamptz
 "proyek_processed_events_pkey" PRIMARY KEY, btree (consumer_group, event_id)

select consumer_group, count(*) ... group by 1
 notification-service | 18
 payment-service      |  5

order cust-1 -> PAID; cust-big 2000000 -> FAILED
fault-consumer-crash, fault-duplicate, fault-poison, fault-broker-down: semua OK, exit 0
stop-all: exit 0
npm test: ✔ (26115ms), pass 1, fail 0
```

Tabel versi lama (PK `event_id` saja) diuji dengan sengaja: service tetap
menyala, order masuk, lalu payment gagal dengan
`column "consumer_group" of relation "proyek_processed_events" does not exist`
dan pesan masuk retry. README menjelaskan cara menghapus tabel lama. Tabel uji
dihapus setelahnya.

LATIHAN 7 dari `start/`: LATIHAN 1 sampai 6 diisi dengan implementasi
`solution/`, LATIHAN 7 ditulis sebagai peserta mengikuti petunjuk README, lalu:

```
$ npm test            (dari proyek/start)
order.status-applied ... "status":"FAILED"
✔ order -> payment -> notification dengan idempotency dan DLT (25992ms)
ℹ pass 1
ℹ fail 0
```

`start/src` dan `start/test` dikembalikan dari salinan sebelum uji
(`diff -r` kosong, 7 berkas masih berisi `LATIHAN`, `npm run build` exit 0).
