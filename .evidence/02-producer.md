# Evidence Lab 02

Tanggal: 2026-11-08; dijalankan ulang 2026-11-11 (lihat bagian "Run ulang 2026-11-11")
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1
Commands (dari `labs/02-producer/`):

```
node solution/ordering.js
node solution/durability.js
```

Topic uji: `lab02.orders.v1` (sandbox). `order.created.v1` tidak disentuh.

## Urutan per key

```
PASS  order-1 -> partisi 1, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-2 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-3 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-4 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-5 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-6 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS lab02 ordering: 6 key x 10 pesan, urutan per key benar
```

## ISR di bawah min.insync.replicas

```
Mengirim batch dengan acks=-1 saat ISR < 2...
PASS  ditolak: code=19 Broker: Not enough in-sync replicas
Menyalakan kafka-2 dan menunggu ISR >= 2...
PASS  ISR partisi 0 kembali 2
PASS  batch dikirim ulang dan diterima
PASS  verifikasi: 5 pesan, 5 key, tanpa duplikat
ISR partisi 0 setelah semua broker hidup: 3
PASS lab02 durability: ditolak saat ISR < 2, berhasil setelah broker kembali, tanpa duplikat
```

Dua broker dimatikan selama pengujian dan dinyalakan kembali oleh skrip
(termasuk jalur error). Error broker code 19 = `NOT_ENOUGH_REPLICAS`.

## Validasi alur start

Scaffold sebelum diisi berhenti dengan pesan LATIHAN (exit 1):

```
$ node start/ordering.js
FAIL lab02 ordering: LATIHAN: buat topic lab02.orders.v1 (3 partisi, RF3, min.insync.replicas=2)

$ node start/durability.js
FAIL lab02 durability: LATIHAN: baca metadata dan pilih partisi + broker leader
```

Setelah `start/ordering.js` dan `start/durability.js` diisi dengan implementasi
solution, keduanya dijalankan ulang dan lulus:

```
PASS lab02 ordering: 6 key x 10 pesan, urutan per key benar
PASS lab02 durability: ditolak saat ISR < 2, berhasil setelah broker kembali, tanpa duplikat
```

Exit code kedua run: 0.

## Run ulang 2026-11-11

Perintah sama (keduanya exit 0):

```
PASS  order-1 -> partisi 1, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-2 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-3 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-4 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-5 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-6 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS lab02 ordering: 6 key x 10 pesan, urutan per key benar

PASS  ISR partisi 1 kembali 2
PASS  batch dikirim ulang dan diterima
PASS  verifikasi: 5 pesan, 5 key, tanpa duplikat
ISR partisi 1 setelah semua broker hidup: 3
PASS lab02 durability: ditolak saat ISR < 2, berhasil setelah broker kembali, tanpa duplikat
```

Catatan run ini: `solution/ordering.js` saat itu hanya mencatat partisi dari
pesan pertama setiap key. Diperbaiki di putaran 2 (lihat di bawah).

## Run ulang 2026-11-11 (putaran 2)

`solution/ordering.js` kini menyimpan himpunan partisi per key dan FAIL bila
satu key muncul di lebih dari satu partisi. Stack bersih (exit 0 keduanya):

```
PASS  order-1 -> partisi 1, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-2 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-3 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-4 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-5 -> partisi 2, seq 1,2,3,4,5,6,7,8,9,10
PASS  order-6 -> partisi 0, seq 1,2,3,4,5,6,7,8,9,10
PASS lab02 ordering: 6 key x 10 pesan, satu partisi dan urutan benar per key

ISR partisi 2 setelah semua broker hidup: 3
PASS lab02 durability: ditolak saat ISR < 2, berhasil setelah broker kembali, tanpa duplikat
```
