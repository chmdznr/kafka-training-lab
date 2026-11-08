# Lab Kafka 2 Hari — Node.js / NestJS

Repo ini berisi stack Docker dan latihan hands-on untuk training Kafka dua hari.
Skenario yang dipakai adalah toko daring sederhana: `order → payment → notification`.

## Prasyarat

- Docker Desktop (macOS/Windows via WSL2) atau Docker Engine + Compose v2.
- Node.js 24 LTS. Versi ada di `.nvmrc`; jalankan `nvm use`.
- RAM minimal 16 GB dan ruang disk kosong 20 GB.
- Perintah di README ini dijalankan dari akar repo (folder hasil clone, disebut `lab/` di materi).

## Mulai cepat

```bash
git clone https://github.com/chmdznr/kafka-training-lab.git
cd kafka-training-lab
nvm use
npm ci
npm run setup:native
docker compose up -d
```

`npm ci` sengaja tidak menjalankan install script dependency (`.npmrc`
berisi `ignore-scripts=true`). `npm run setup:native` memasang binary native
`@confluentinc/kafka-javascript` yang cocok dengan platform dan versi Node.
Perintah ini aman dijalankan berulang; kalau binary sudah ada, tidak ada yang
diunduh.

Tunggu sampai ketiga broker sehat (`docker compose ps`). Setelah itu cek stack:

```bash
npm run smoke
```

Atau jalankan pemeriksaan lengkap yang juga memeriksa versi Docker/Node dan
melatih listener SASL:

```bash
./scripts/verify-env.sh
```

`verify-env.sh` mencetak ringkasan PASS/FAIL. Kirim keluarannya ke trainer
kalau ada yang gagal.

Cluster baru butuh beberapa detik sebelum topic internal `__consumer_offsets`
siap. Selama periode itu klien bisa mencetak `The group coordinator is not
available`; log ini normal dan akan hilang sendiri.

## Port

| Host | Service | Keterangan |
|---|---|---|
| 9092 | kafka-1 | PLAINTEXT, untuk klien di host |
| 9093 | kafka-2 | PLAINTEXT, untuk klien di host |
| 9094 | kafka-3 | PLAINTEXT, untuk klien di host |
| 9192 | kafka-1 | SASL_PLAINTEXT / SCRAM-SHA-512, user `app` |
| 9193 | kafka-2 | SASL_PLAINTEXT / SCRAM-SHA-512 |
| 9194 | kafka-3 | SASL_PLAINTEXT / SCRAM-SHA-512 |
| 8080 | kafka-ui | http://localhost:8080 |
| 8081 | schema-registry | http://localhost:8081 |
| 5432 | postgres | db `shop`, user `app`, password `app` |
| 8083 | connect | hanya profile `demo` |
| 16686 | jaeger UI | hanya profile `tracing` |
| 4318 | OTLP HTTP | hanya profile `tracing` |

Kredensial SASL: `app` / `app-secret`. User dibuat otomatis oleh service
satu-kali `kafka-init` setelah ketiga broker sehat.

Listener SASL mengiklankan `localhost:919x`, jadi hanya bisa dipakai dari host.
Service yang berjalan di dalam jaringan Compose memakai listener internal
PLAINTEXT `kafka-1:29092`, `kafka-2:29092`, `kafka-3:29092`.

## Isi stack

| Komponen | Versi |
|---|---|
| Kafka (KRaft, 3 node broker+controller) | `apache/kafka:4.4.0` |
| Kafka UI | `kafbat/kafka-ui:v1.5.0` |
| Schema Registry | `confluentinc/cp-schema-registry:8.4.0` |
| Postgres | `postgres:17` (`wal_level=logical`) |
| Debezium Connect (profile `demo`) | `quay.io/debezium/connect:3.7.1.Final` |
| Jaeger (profile `tracing`) | `jaegertracing/jaeger:2.0.0` |

Konfigurasi cluster: 3 partisi default, replication factor 3,
`min.insync.replicas=2`, `auto.create.topics.enable=false`. Topic Kafka harus
dibuat eksplisit (lewat admin client atau `kafka-topics.sh`).

## Kontrak event

Semua lab memakai kontrak yang sama:

- Topic: `<domain>.<event>.v<versi>`, contoh `order.created.v1`.
- Key: `orderId` untuk event order dan payment.
- Envelope: lihat `shared/envelope.js` (`createEnvelope`, `validateEnvelope`).
- Header: `correlation-id`, `event-type`, `traceparent` (sesi observability).

`order.created.v1` hanya untuk event yang memenuhi kontrak. Lab 01, 02, dan 04
memakai topic sandbox `lab01.orders.v1`, `lab02.orders.v1`, dan
`lab04.orders.v1` agar record uji tidak bercampur dengan event kontrak yang
dibaca consumer dari awal.

Detail kontrak untuk tiga event utama ada di
`labs/03-kontrak-event/solution/kontrak-event.md`.

Ringkasan konfigurasi producer, consumer, NestJS, pola reliability, dan perintah
CLI ada di [CHEATSHEET.md](CHEATSHEET.md) (versi cetak satu halaman:
[CHEATSHEET.pdf](CHEATSHEET.pdf)).

## Daftar lab

| Lab | Topik | Folder |
|---|---|---|
| 01 | Cluster KRaft, replikasi, ISR, leader election | `labs/01-cluster` |
| 02 | Producer, key ordering, `acks=all`, idempotent producer | `labs/02-producer` |
| 03 | Desain event, kontrak, envelope | `labs/03-kontrak-event` |
| 04 | Consumer group, rebalance, lag, replay | `labs/04-consumer-group` |
| 05 | NestJS + Kafka | `labs/05-nestjs` |
| 06 | Idempotency, retry, DLT, replay | `labs/06-idempotency-retry-dlt` |
| 07 | Outbox dan transaksi | `labs/07-outbox-transaksi` |
| 08 | Schema Registry | `labs/08-schema-registry` |
| 09 | Observability dan koneksi aman | `labs/09-observability-security` |
| 10 | Mini-proyek dan fault injection | `proyek` |

Setiap lab punya `start/` (titik awal dengan bagian `LATIHAN`) dan `solution/`
(jawaban lengkap). Peserta yang tertinggal boleh langsung pindah ke `solution/`
lab sebelumnya.

## Profile opsional

```bash
# Demo CDC Debezium (instruktur)
docker compose --profile demo up -d

# Jaeger untuk sesi observability
docker compose --profile tracing up -d
```

## Menghentikan stack

```bash
docker compose stop          # container berhenti, data tetap
docker compose down          # container dan network dihapus, volume tetap
docker compose down -v       # hapus semua data cluster
```

Jangan memakai `down -v` saat lab lain masih berjalan.
