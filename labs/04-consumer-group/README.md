# Lab 04 — Consumer group, rebalance, lag, dan replay

Tujuan: melihat pembagian partisi di dalam consumer group, mengukur jeda
rebalance pada dua protokol, lalu memutar ulang pesan dengan offset reset.

Durasi: 55 menit.

## Prasyarat

- Stack menyala. Topic sandbox `lab04.orders.v1` dibuat oleh skrip lab ini.
- Topic `order.created.v1` hanya untuk event yang memenuhi kontrak; lab ini
  memakai sandbox `lab04.orders.v1` agar tidak mengotori topic itu.
- Semua perintah dijalankan dari folder lab ini, `lab/labs/04-consumer-group/`.
  Perintah `docker compose` juga jalan dari sini karena Compose mencari
  `docker-compose.yml` di folder induk.

## Alur latihan

Kerjakan `start/` dulu, bukan `solution/`:

- `start/worker.js` — lengkapi penanganan `SIGTERM` pada penanda `LATIHAN`.
- `start/lab04.js` — lengkapi tiga penanda `LATIHAN`: consumer keempat,
  pengukuran rebalance, dan reset offset.

Jalankan:

```bash
node start/lab04.js
```

Sebelum diisi, skrip menampilkan baseline tiga consumer lalu berhenti dengan
pesan `LATIHAN`. Setelah setiap bagian diisi, jalankan ulang. Kalau tertinggal,
pakai `solution/assignments.js`, `solution/rebalance.js`, dan
`solution/replay.js` sebagai cadangan.

## 1. Tiga consumer, tiga partisi

Lengkapi `LATIHAN` pertama di `start/lab04.js`, lalu jalankan:

```bash
node start/lab04.js
```

Skrip menyalakan tiga consumer dalam satu group, mengirim pesan terus-menerus,
lalu menambah consumer keempat. Karena topic hanya punya tiga partisi, satu
consumer tidak mendapat partisi dan tidak memproses pesan. Consumer mana yang
idle bisa berbeda-beda; yang penting hanya tiga yang aktif.

Cadangan: `node solution/assignments.js`.

Cek group dari CLI saat consumer masih berjalan (ganti `<group>` dengan group
yang dicetak skrip):

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 --describe --group <group>
```

Kolom `LAG` menunjukkan selisih offset consumer dengan ujung log.

## 2. Ukur jeda rebalance: classic vs consumer

Lengkapi `LATIHAN` kedua, lalu jalankan:

```bash
node start/lab04.js
```

Cadangan: `node solution/rebalance.js`.

Untuk setiap protokol, skrip menyalakan tiga consumer, mengalirkan pesan ke
ketiga partisi, lalu mengirim `SIGTERM` ke satu consumer. Skrip mencetak dua
angka:

- **pause partisi yang berpindah**: dari pesan terakhir di partisi milik
  consumer yang berhenti sampai pesan pertama setelah partisi itu dilayani
  consumer lain;
- **gap maksimum consumer tidak terdampak**: gap terbesar antar-pesan pada
  partisi yang tetap dilayani consumer yang tidak menerima partisi pindahan.

### Assignor yang benar-benar dipakai

Klien Confluent memetakan `partitionAssigners` KafkaJS ke
`partition.assignment.strategy`. Kalau properti itu tidak diisi, klien memakai
`roundrobin` untuk `group.protocol=classic`. `roundrobin` adalah assignor
**eager**: semua member mencabut partisinya dan join ulang saat rebalance.
Konfirmasi dari sisi broker:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 --describe --group <group> --state
```

Kolom `ASSIGNMENT-STRATEGY` menampilkan `roundrobin` untuk group classic.
`cooperative-sticky` hanya dipakai kalau `partitionAssigners` diisi eksplisit
dengan strategi itu.

Untuk `group.protocol=consumer` (KIP-848), `partition.assignment.strategy`
tidak didukung. Assignor dijalankan di broker: daftarnya diatur config broker
`group.consumer.assignors` (default `uniform,range`), dan group memakai yang
pertama kecuali client meminta lain lewat config client
`group.remote.assignor`. Heartbeat dan session timeout juga diatur broker:
`group.consumer.heartbeat.interval.ms` (default 5000 ms) dan
`group.consumer.session.timeout.ms` (default 45000 ms). Karena itu partisi yang
berpindah menunggu giliran assignment/heartbeat berikutnya, sedangkan partisi
pada consumer yang tidak menerima partisi pindahan terus berjalan. Consumer
yang menerima partisi pindahan bisa sempat mengejar backlog, sehingga partisi
lain yang dipegangnya ikut terlihat jeda singkat.

### Yang diharapkan terlihat

- `classic` (eager `roundrobin`): semua partisi dicabut sebentar saat
  rebalance. Jeda bisa sangat singkat di cluster lokal, tetapi semua consumer
  ikut berhenti pada periode itu.
- `consumer` (KIP-848): hanya partisi yang berpindah yang menunggu assignment
  berikutnya; consumer yang tidak terdampak terus memproses pesan. Jeda
  partisi pindahan bisa lebih besar daripada classic karena menunggu heartbeat,
  jadi jangan menyimpulkan "KIP-848 selalu lebih cepat" hanya dari satu angka.

Angka pasti bergantung pada mesin, beban, dan backlog. Yang penting, kedua jeda
itu bisa dijelaskan: apa penyebabnya dan kapan masing-masing muncul.

## 3. Lag dan replay

Lengkapi `LATIHAN` ketiga, lalu jalankan:

```bash
node start/lab04.js
```

Cadangan: `node solution/replay.js`.

Alur yang diharapkan:

1. Produce 30 pesan penanda.
2. Consumer pertama membaca dari awal, lalu berhenti. Lag harus 0.
3. Offset group di-reset ke earliest memakai `kafka-consumer-groups.sh`.
4. Consumer kedua membaca ulang pesan yang sama. Lag kembali 0.

Perintah reset yang dipakai:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:29092 \
  --group <group> --topic lab04.orders.v1 \
  --reset-offsets --to-earliest --execute
```

Reset offset hanya bisa dilakukan saat group tidak punya consumer aktif.
Setelah reset, consumer yang start akan membaca dari offset baru dan lag
dihitung ulang.

Kalau observer melihat `The group coordinator is not available` tepat setelah
cluster menyala, itu normal sampai topic internal `__consumer_offsets` siap.

## Selesai jika

Peserta menunjukkan:

1. Lag kembali ke 0 setelah replay.
2. Catatan jeda rebalance `classic` dan `consumer` (partisi berpindah dan
   consumer tidak terdampak), beserta penjelasan singkat perbedaannya.
