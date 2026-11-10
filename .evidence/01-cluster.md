# Evidence Lab 01

Tanggal: 2026-11-08; dijalankan ulang 2026-11-11 (lihat bagian "Run ulang 2026-11-11")
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1
Command: `cd labs/01-cluster && bash solution/lab01.sh`

Topic: `order.created.v1` dibuat kosong (kanonik), `lab01.orders.v1` dipakai
untuk seluruh uji, `order.state.v1` untuk compacted.

```
== 2. Buat topic kanonik order.created.v1 (kosong) dan sandbox lab01.orders.v1
INFO  order.created.v1 berisi 0 record (topic kanonik untuk event kontrak)
PASS  6 pesan milik run ini terbaca

== 5. Fault injection: matikan leader partisi 0
Leader partisi 0: kafka-2. Perintah berikutnya lewat kafka-1 (kafka-1:29092).
PASS  leader partisi 0: 2 -> 3
PASS  Isr partisi 0 setelah broker mati: 2 broker
PASS  semua pesan tetap terbaca (6/6)

== 6. Nyalakan broker dan tunggu ISR pulih
PASS  Isr partisi 0 pulih: 3 broker

== 7. Buat topic compacted order.state.v1
PASS  order.state.v1 cleanup.policy=compact

PASS  order.created.v1 tetap kosong; lalu lintas uji hanya di lab01.orders.v1

PASS lab01: leader berpindah, pesan tetap terbaca, ISR pulih, topic compacted dibuat
```

Broker yang dimatikan (kafka-2) dinyalakan kembali oleh skrip; ISR dan quorum
pulih. Sejak 2026-11-11 Lab 03 menulis ke topic sandbox
`lab03.order.created.v1`, jadi `order.created.v1` tetap kosong sampai Lab 05.

## Validasi alur start

`bash start/lab01.sh` sebelum diisi (exit 1, bukan syntax error):

```
== 2. Buat topic kanonik order.created.v1 (kosong) dan sandbox lab01.orders.v1
LATIHAN: lengkapi pembuatan order.created.v1 (kosong) dan lab01.orders.v1 (sandbox uji)
```

Setelah semua LATIHAN diisi, perintah yang sama menjalankan seluruh langkah;
hasilnya sama dengan solution di atas.

## Run ulang 2026-11-11

Stack baru dari `docker compose up -d --wait`, perintah sama (exit 0):

```
PASS  leader partisi 0: 1 -> 2
PASS  Isr partisi 0 setelah broker mati: 2 broker
PASS  semua pesan tetap terbaca (6/6)

== 6. Nyalakan broker dan tunggu ISR pulih
PASS  Isr partisi 0 pulih: 3 broker

== 7. Buat topic compacted order.state.v1
PASS  order.state.v1 cleanup.policy=compact

PASS  order.created.v1 tetap kosong; lalu lintas uji hanya di lab01.orders.v1

PASS lab01: leader berpindah, pesan tetap terbaca, ISR pulih, topic compacted dibuat
```

Langkah 7 kini memakai `kafka-configs.sh --alter --entity-type topics
--add-config cleanup.policy=compact`; `kafka-topics.sh --alter --add-config`
ditolak Kafka 4.4 (`add-config is not a recognized option`).
