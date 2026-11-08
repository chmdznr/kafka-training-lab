# Lab 01 — Cluster KRaft, replikasi, dan ISR

Tujuan: melihat langsung bagaimana topic direplikasi, bagaimana leader partisi
bekerja, dan apa yang terjadi saat satu broker mati.

Durasi: 45 menit.

## Prasyarat

- Stack sudah menyala (`docker compose up -d`) dan ketiga broker sehat:
  `docker compose ps`.
- Semua perintah dijalankan dari folder lab ini, `lab/labs/01-cluster/`.
  Perintah `docker compose` juga jalan dari sini karena Compose mencari
  `docker-compose.yml` di folder induk.

## Alur latihan

Kerjakan `start/lab01.sh` dulu. Lengkapi penanda `LATIHAN` di langkah 2-6:
create topic kanonik + sandbox, produce dengan key, describe leader/ISR, fault
injection, dan topic compacted. Jalankan:

```bash
bash start/lab01.sh
```

Sebelum diisi, skrip berhenti dengan pesan `LATIHAN` (bukan syntax error).
Kalau tertinggal, `solution/lab01.sh` adalah cadangan yang sudah lengkap.

## 1. Periksa cluster

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:29092 describe --status
```

Perhatikan `CurrentVoters`: ada tiga voter (node 1, 2, 3). Ketiganya menjalankan
peran `broker,controller` sekaligus.

Cek broker yang terdaftar:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:29092 | grep 'id:'
```

## 2. Buat topic replicated

Buat topic kanonik `order.created.v1` (3 partisi, RF 3,
`min.insync.replicas=2`). Topic ini disiapkan untuk event yang memenuhi
kontrak, tetapi **tidak dipakai untuk uji di lab ini**. Semua lalu lintas uji
masuk topic sandbox `lab01.orders.v1`.

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create --topic order.created.v1 \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create --topic lab01.orders.v1 \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
```

Lihat leader dan ISR setiap partisi sandbox:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --describe --topic lab01.orders.v1
```

Catat broker yang menjadi `Leader` untuk partisi 0, 1, dan 2. Leader biasanya
tersebar merata. `Isr` berisi tiga broker.

`order.created.v1` sengaja dibiarkan kosong. Consumer yang membaca topic itu
dari awal di lab berikutnya akan memproses seluruh isinya; karena itu hanya
event yang memenuhi kontrak (Lab 03) yang boleh ditulis ke sana.

## 3. Produce dan consume lewat CLI

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic lab01.orders.v1 \
  --reader-property parse.key=true \
  --reader-property key.separator=: <<'EOF'
order-1:{"orderId":"order-1","seq":1}
order-2:{"orderId":"order-2","seq":1}
order-3:{"orderId":"order-3","seq":1}
order-1:{"orderId":"order-1","seq":2}
order-2:{"orderId":"order-2","seq":2}
order-3:{"orderId":"order-3","seq":2}
EOF
```

Baca kembali:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 \
  --topic lab01.orders.v1 \
  --from-beginning --max-messages 6 \
  --formatter-property print.key=true
```

Perhatikan bahwa pesan dengan key yang sama selalu berada di partisi yang sama,
tetapi urutan antar-key bisa berbeda saat dibaca.

## 4. Fault injection: matikan leader

Dari langkah 2, ambil broker yang menjadi leader partisi 0. Misalnya
`Leader: 2`, berarti broker tersebut adalah `kafka-2`.

Matikan broker itu:

```bash
docker compose stop kafka-2
```

Lihat topic lagi dari broker yang masih hidup. Kalau kafka-2 yang dimatikan,
gunakan kafka-1 atau kafka-3 sebagai target `exec` dan bootstrap:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --describe --topic lab01.orders.v1
```

Yang harus terlihat:

- `Leader` partisi tersebut berpindah ke broker lain.
- `Isr` tinggal dua broker.
- Pesan tetap bisa dibaca dari awal. Kafka UI di http://localhost:8080 juga
  memperlihatkan leader baru dan daftar ISR yang menyusut.

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:29092 --topic lab01.orders.v1 \
  --from-beginning --max-messages 6 --timeout-ms 10000
```

Nyalakan kembali broker:

```bash
docker compose start kafka-2
```

Tunggu beberapa detik, lalu jalankan `--describe` lagi sampai `Isr` kembali
berisi tiga broker. Broker yang kembali akan mengejar data sebagai follower
sebelum masuk ISR.

Saat cluster baru menyala atau baru pulih, klien KafkaJS dapat mencetak log
`The group coordinator is not available`. Log ini normal sampai topic internal
`__consumer_offsets` siap. Tunggu beberapa detik, lalu ulangi perintah.

## 5. Topic compacted

Compacted topic menyimpan nilai terakhir per key. Buat topic:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create --topic order.state.v1 \
  --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact
```

Kirim dua nilai untuk key yang sama:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:29092 --topic order.state.v1 \
  --reader-property parse.key=true --reader-property key.separator=: <<'EOF'
order-1:{"state":"created"}
order-1:{"state":"paid"}
EOF
```

Cek konfigurasi:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --describe --topic order.state.v1
```

`cleanup.policy=compact` muncul di baris `Configs`. Compaction berjalan malas
di background (saat segmen di-roll dan cleaner berjalan), jadi dua record masih
bisa terlihat kalau langsung dibaca. Yang penting adalah kebijakannya sudah
tersimpan di topic.

Setelah semua `LATIHAN` terisi, jalankan seluruh langkah otomatis:

```bash
bash start/lab01.sh
```

Cadangan: `bash solution/lab01.sh`.

## Selesai jika

Peserta menunjukkan dua hal:

1. `Leader` partisi berpindah ke broker lain setelah broker leader dimatikan.
2. Semua pesan yang dikirim sebelum broker mati tetap bisa dibaca setelah
   broker mati, lalu `Isr` kembali berisi tiga broker setelah broker dinyalakan.
