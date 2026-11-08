# Lab 02 — Producer, key ordering, dan jaminan tulis

Tujuan: membuktikan bahwa key menentukan partisi (dan urutan), lalu melihat
sendiri bagaimana `acks=all` dan `min.insync.replicas=2` menolak tulisan saat
terlalu sedikit replica yang hidup.

Durasi: 35 menit.

## Prasyarat

- Stack menyala, ketiga broker sehat.
- Topic `lab02.orders.v1` dibuat oleh skrip lab ini bila belum ada.
- Topic `order.created.v1` hanya untuk event yang memenuhi kontrak; lab ini
  memakai sandbox `lab02.orders.v1` agar tidak mengotori topic itu.
- Semua perintah dijalankan dari folder lab ini, `lab/labs/02-producer/`.
  Perintah `docker compose` juga jalan dari sini karena Compose mencari
  `docker-compose.yml` di folder induk.

## Alur latihan

Kerjakan `start/` dulu, bukan `solution/`:

- `start/ordering.js` — lengkapi `LATIHAN`: create topic, config producer,
  loop pengiriman, lalu consumer yang memvalidasi urutan per key.
- `start/durability.js` — lengkapi `LATIHAN`: pilih partisi/leader, matikan dua
  broker, tangkap error, nyalakan broker, kirim ulang, verifikasi tanpa
  duplikat.

Jalankan:

```bash
node start/ordering.js
node start/durability.js
```

Sebelum diisi, keduanya berhenti dengan pesan `LATIHAN` (bukan syntax error).
Kalau tertinggal, gunakan `solution/ordering.js` dan `solution/durability.js`
sebagai cadangan.

## 1. Urutan per key

Setelah `start/ordering.js` diisi, jalankan:

```bash
node start/ordering.js
```

Cadangan: `node solution/ordering.js`.

Skrip mengirim 6 key × 10 pesan secara bergantian. Setiap key selalu masuk
partisi yang sama karena Kafka memakai hash key, sehingga urutan pesan untuk
satu key terjaga. Consumer membaca ulang dan memeriksa `seq` per key.

Yang perlu terlihat:

- key tersebar di tiga partisi; beberapa key boleh berbagi partisi;
- `seq` untuk setiap key tetap 1, 2, ..., 10;
- pesan antar-key boleh saling menyela. Urutan global tidak dijamin, hanya
  urutan per key.

Producer memakai `acks=-1` (semua ISR) dan idempotent producer. Keduanya
penting untuk jaminan tulis.

## 2. Fault injection: ISR di bawah min.insync.replicas

Setelah `start/durability.js` diisi, jalankan:

```bash
node start/durability.js
```

Cadangan: `node solution/durability.js`.

Skrip melakukan:

1. Memilih satu partisi dan broker leader-nya.
2. Mematikan dua broker lain. Hanya broker leader yang hidup, jadi ISR
   partisi tersebut menyusut menjadi 1.
3. Mengirim batch dengan `acks=-1`. Broker menolak karena
   `min.insync.replicas=2` tidak terpenuhi. Error yang dicari:
   `NOT_ENOUGH_REPLICAS` (atau pesan setara dari broker).
4. Menyalakan kembali satu broker sampai ISR kembali 2.
5. Mengirim ulang batch yang gagal dengan idempotent producer.
6. Membaca partisi dari awal dan memastikan setiap pesan muncul tepat satu
   kali. Setelah verifikasi, broker kedua dinyalakan lagi sampai ISR kembali 3.

### Catatan quorum KRaft

Di compose ini setiap node menjalankan `broker,controller`. Mematikan dua dari
tiga node juga menghilangkan mayoritas quorum controller (2 dari 3). Selama
mayoritas hilang, metadata cluster tidak bisa berubah dan leader election tidak
bisa terjadi sampai satu node kembali. Broker yang tersisa tetap bisa menolak
tulisan karena ISR partisinya di bawah `min.insync.replicas`, dan itulah error
yang ditangkap lab ini. Setelah satu broker kembali, quorum 2 dari 3 pulih dan
pengiriman bisa dilanjutkan; setelah broker kedua kembali, ISR pulih ke 3.
Skrip menyalakan kembali semua broker di akhir, termasuk pada jalur error.

Kalau ingin mencoba manual, hentikan dua broker yang **bukan** leader partisi
target, lalu perhatikan error producer. Contoh di bawah menganggap leader
partisi target adalah `kafka-1`; sesuaikan nama broker.

```bash
docker compose stop kafka-2 kafka-3
# tunggu sampai ISR partisi target menyusut; shutdown broker yang graceful
# biasanya langsung memperbarui ISR
# jalankan producer dengan acks=-1 dan min.insync.replicas=2
docker compose start kafka-2
# setelah verifikasi selesai, pulihkan broker kedua
docker compose start kafka-3
```

`acks=all` berarti producer menunggu semua replica di ISR. Ketika ISR tinggal
satu, broker menolak lebih awal karena jumlah ISR sudah di bawah
`min.insync.replicas`. Inilah yang mencegah pesan "berhasil" padahal hanya
tersimpan di satu broker yang bisa saja mati.

## Selesai jika

1. Producer `acks=all` ditolak saat ISR < 2, dan error yang terlihat menyebut
   `NOT_ENOUGH_REPLICAS` (atau padanannya).
2. Setelah satu broker kembali, pengiriman berhasil dan tidak ada duplikat:
   setiap pesan batch muncul tepat satu kali.
