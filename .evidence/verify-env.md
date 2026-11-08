# Evidence verify-env

Tanggal: 2026-11-08
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1
Command: `./scripts/verify-env.sh` (dari `lab/`)

```
Pemeriksaan environment lab Kafka
OS: Darwin 27.2.0 arm64
Direktori: /Users/chmdznr/work/achmad/training-kafka/lab

== Docker
  PASS  docker 29.8.2
  PASS  docker compose 5.5.0

== Node dan dependency
  PASS  node v24.22.1 (sesuai .nvmrc 24)
  PASS  npm ci
  PASS  binary native @confluentinc/kafka-javascript terpasang
  PASS  native @confluentinc/kafka-javascript termuat

== Image dan stack
  PASS  image docker tersedia
  PASS  docker compose up -d

== Cluster Kafka
  PASS  broker menerima koneksi
  PASS  3 broker berjalan
  PASS  KRaft quorum 3 voter

== Produce dan consume
PASS plaintext: produced + consumed lab.verify.v1 (RF3, 3 partitions)
PASS sasl: produced + consumed lab.verify.v1 (RF3, 3 partitions)
PASS smoke: PLAINTEXT dan SASL berfungsi
  PASS  smoke PLAINTEXT dan SASL

----------------------------------------
PASS: 12  FAIL: 0
Lingkungan siap dipakai untuk training.
```

Catatan pemasangan: `.npmrc` memakai `ignore-scripts=true`; binary native
dipasang oleh `npm run setup:native` (idempotent) karena endpoint download
node-pre-gyp mengembalikan 504 pada jaringan ini.
