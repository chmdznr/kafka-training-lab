# Evidence Lab 04

Tanggal: 2026-11-08
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0, Node 24.22.1
Commands (dari `labs/04-consumer-group/`, clean run):

```
node solution/assignments.js
node solution/rebalance.js
node solution/replay.js
```

Topic uji: `lab04.orders.v1` (sandbox). `order.created.v1` tidak disentuh.

## Tiga consumer + consumer keempat

```
  c1: delta=33, partisi dilihat=[0,1,2]
  c2: delta=33, partisi dilihat=[1]
  c3: delta=33, partisi dilihat=[2]
  c4: delta=0, partisi dilihat=[]
PASS lab04 assignments: 3 consumer aktif, 1 idle, 99 pesan diproses setelah rebalance
```

## Jeda rebalance classic vs consumer (KIP-848)

```
  protokol  partisi berpindah  gap maks consumer tidak terdampak
  classic     703 ms              41 ms
  consumer   2938 ms              39 ms
PASS lab04 rebalance: classic moved=703 ms/unaffected=41 ms, consumer moved=2938 ms/unaffected=39 ms
```

- `classic`: assignor efektif `roundrobin` (eager), dikonfirmasi dari
  `kafka-consumer-groups.sh --describe --group <group> --state` ->
  `ASSIGNMENT-STRATEGY roundrobin`. Semua partisi dicabut saat rebalance;
  jedanya singkat di cluster lokal.
- `consumer`: KIP-848, `group.remote.assignor` dipilih broker (default
  `uniform`); heartbeat diatur broker `group.consumer.heartbeat.interval.ms`
  (default 5000 ms). Partisi yang berpindah menunggu assignment berikutnya,
  sedangkan consumer yang tidak menerima partisi pindahan terus memproses.
- Angka bergantung mesin, beban, dan backlog; yang dibandingkan adalah dua
  definisi jeda, bukan janji bahwa satu protokol selalu lebih cepat.

## Reset offset dan replay

```
PASS  produce 30 pesan penanda (runId 27ef5447-...)
PASS  lag 0 sebelum reset
lab04.orders.v1 0  4165  4165  0
lab04.orders.v1 1  4136  4136  0
lab04.orders.v1 2  4135  4135  0
== Reset offset ke earliest
lab04.orders.v1 0  0
lab04.orders.v1 2  0
lab04.orders.v1 1  0
PASS  lag 0 sesudah replay
PASS lab04 replay: lag 0 sebelum reset, 30 pesan diputar ulang dengan offset earliest, lag 0 sesudah
```

Reset memakai `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`
dan lag dikembalikan ke 0 setelah consumer kedua selesai membaca.

## Validasi alur start

Sebelum diisi, `node start/lab04.js` menampilkan baseline tiga consumer lalu
berhenti dengan pesan LATIHAN (exit 1); worker yang sempat hidup dimatikan lagi
oleh handler error.

Setelah `start/lab04.js` dan `start/worker.js` diisi dengan implementasi
referensi, run gabungan `assignments -> rebalance -> replay` selesai exit 0
dengan tiga PASS di atas. Scaffold dikembalikan setelah verifikasi.
