# Evidence Lab 01

Tanggal: 2026-11-08
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
pulih. Setelah Lab 03, `order.created.v1` berisi 2 record event kontrak
(lihat `.evidence/clean-run.md`).

## Validasi alur start

`bash start/lab01.sh` sebelum diisi (exit 1, bukan syntax error):

```
== 2. Buat topic kanonik order.created.v1 (kosong) dan sandbox lab01.orders.v1
LATIHAN: lengkapi pembuatan order.created.v1 (kosong) dan lab01.orders.v1 (sandbox uji)
```

Setelah semua LATIHAN diisi, perintah yang sama menjalankan seluruh langkah;
hasilnya sama dengan solution di atas.
