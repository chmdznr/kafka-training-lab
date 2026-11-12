# Demo Debezium — outbox lewat CDC

Demo instruktur, bukan lab peserta. Durasi sekitar 10 menit.

Tujuannya memperlihatkan alternatif polling di Lab 07: Debezium membaca WAL
Postgres dan mem-publish baris tabel `outbox` tanpa poller dari aplikasi.

Prasyarat:

- Stack lab menyala dan profil `demo` ikut dijalankan:
  `docker compose --profile demo up -d` dari root repo `lab/`.
- Postgres berjalan dengan `wal_level=logical`.
- Kafka Connect (`localhost:8083`) dan Schema Registry (`localhost:8081`) sehat.

Perintah `docker compose` dijalankan dari root repo `lab/`; perintah Node
dijalankan dari `lab/labs/07-outbox-transaksi/solution`.

## 1. Pastikan logical replication aktif

```bash
docker compose exec postgres psql -U app -d shop -tAc "show wal_level;"
```

Harus mencetak `logical`. Kalau bukan, compose belum memakai
`command: ["postgres", "-c", "wal_level=logical"]`; hubungi pemilik compose.

## 2. Siapkan database dan order-service

Dependency sudah dipasang dari root `lab/` (`npm ci` lalu
`npm run setup:native`). Kalau Lab 07 sudah pernah dijalankan, tabelnya sudah
ada. Kalau belum:

```bash
cd labs/07-outbox-transaksi/solution
npm run setup
```

Jalankan order-service **tanpa** publisher. Poller tidak dipakai pada demo ini;
Debezium yang akan mem-publish event. Menjalankan keduanya sekaligus membuat
event ganda.

```bash
# terminal 1
cd labs/07-outbox-transaksi/solution
npm run order-service
```

## 3. Daftarkan connector

```bash
cd demo/debezium
curl -s -X POST http://localhost:8083/connectors \
  -H 'content-type: application/json' \
  --data @connector.json
```

Periksa status sampai `RUNNING`:

```bash
curl -s http://localhost:8083/connectors/order-outbox-connector/status
```

Keluaran yang diharapkan:

```json
{
  "name": "order-outbox-connector",
  "connector": { "state": "RUNNING", "worker_id": "..." },
  "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "..." } ],
  "type": "source"
}
```

Yang penting `connector.state` dan `tasks[0].state` bernilai `RUNNING`.

Kalau state `FAILED`, lihat penyebabnya:

```bash
curl -s http://localhost:8083/connectors/order-outbox-connector/status | python3 -m json.tool
docker compose logs --tail=100 connect
```

## 4. Buat order dan lihat event

Terminal 2:

```bash
curl -s -X POST http://localhost:3007/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"demo-cdc","item":"kopi","amount":25000}'
```

Debezium membaca insert ke `outbox` dari WAL, lalu EventRouter SMT mem-publish event
ke `order.created.v1` dengan key `aggregateid` (orderId). Biasanya kurang dari
satu detik.

Terminal 3:

```bash
# dari akar repo lab/
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic order.created.v1 \
  --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.headers=true \
  --formatter-property key.separator=' | '
```

Keluaran yang diharapkan:

```text
__debezium.context... ,id=<eventId>,event-type=order.created.v1 | <orderId> | {"data":{...},"eventId":"<eventId>","version":1,"eventType":"order.created.v1",...}
```

Header `id` berasal dari kolom `outbox.id`, yang oleh order-service diisi
`eventId` dari envelope. Jadi `<eventId>` di header dan di payload adalah UUID
yang sama; consumer bisa dedup memakai salah satunya. Header `event-type`
berasal dari kolom `type` lewat `table.fields.additional.placement`. Connect
juga menambahkan beberapa header `__debezium.context.*`. Urutan field JSON tidak
penting.

Bandingkan dengan Lab 07: di sana event dikirim oleh poller aplikasi. Di sini
aplikasi tidak pernah menyentuh Kafka; hanya Postgres yang berubah.

## 5. Pause lalu resume connector

Saat connector di-pause, task berhenti membaca WAL. Baris outbox yang masuk
tetap tertahan di Postgres dan tidak ada event yang di-publish; setelah resume,
Debezium melanjutkan dari posisi terakhir di replication slot, jadi tidak ada
event yang hilang.

Catat posisi topic sebelum pause:

```bash
# dari akar repo lab/
docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic order.created.v1
```

Pause:

```bash
curl -s -X PUT http://localhost:8083/connectors/order-outbox-connector/pause
```

Respons `202 Accepted` tanpa body. Cek status. Connector dan task berubah
menjadi `PAUSED`:

```bash
curl -s http://localhost:8083/connectors/order-outbox-connector/status | python3 -m json.tool
```

Buat dua order saat paused:

```bash
for i in 1 2; do
  curl -s -X POST http://localhost:3007/orders \
    -H 'content-type: application/json' \
    -d "{\"customerId\":\"demo-pause-$i\",\"item\":\"teh\",\"amount\":15000}"
  echo
done
```

Tunggu beberapa detik, lalu cek offset lagi. Angkanya harus sama dengan
sebelum pause; belum ada event baru.

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic order.created.v1
```

Kalau ada proses lain yang menulis ke topic yang sama (misalnya peserta lain
sedang menjalankan lab), offset total bisa berubah walau connector paused.
Bukti yang menentukan adalah dua `orderId` di atas tidak muncul saat paused dan
muncul setelah resume.

Resume:

```bash
curl -s -X PUT http://localhost:8083/connectors/order-outbox-connector/resume
```

Respons `202 Accepted`. Tunggu 2–5 detik, lalu cek offset dan event. Offset
harus bertambah, dan dua `orderId` yang dibuat saat paused muncul di topic:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic order.created.v1

docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic order.created.v1 \
  --from-beginning \
  --timeout-ms 5000 \
  --formatter-property print.key=true \
  --formatter-property key.separator=' | ' 2>/dev/null | grep -E 'demo-pause|demo-cdc'
```

Tidak ada event yang hilang: yang tertahan saat paused menyusul setelah
resume. Hentikan demo dengan resume kalau connector masih paused.

## 6. Bersihkan

```bash
curl -s -X DELETE http://localhost:8083/connectors/order-outbox-connector
```

`slot.drop.on.stop=true` membuat replication slot ikut dihapus saat connector
dihentikan. Kalau slot tertinggal, cek dengan:

```bash
docker compose exec postgres psql -U app -d shop \
  -c "select slot_name, active from pg_replication_slots;"
```

Connector menyimpan konfigurasinya di topic internal Connect. Kalau ingin
mengulang dari nol (termasuk lupa posisi WAL), hapus connector lalu daftarkan
ulang; `snapshot.mode=no_data` berarti hanya perubahan baru yang di-publish.

## Catatan

- Demo memakai `snapshot.mode=no_data`, jadi baris outbox yang sudah ada
  sebelum connector didaftarkan tidak ikut di-publish. Hanya INSERT baru yang
  terlihat, sesuai alur demo.
- `topic.prefix` hanya dipakai untuk topic internal Debezium; EventRouter
  mengganti nama topic keluaran menjadi `order.created.v1`.
- Jangan menjalankan `npm run publisher` dan Debezium bersamaan. Keduanya
  mem-publish baris outbox yang sama, dan itu menghasilkan event ganda
  (idempotent consumer yang menanganinya, tetapi membingungkan saat demo).
- Debezium tidak meng-update `outbox.published_at`; ia hanya membaca WAL. Baris
  yang dibuat saat demo tetap `pending` di tabel. Kalau setelah demo Lab 07
  dijalankan lagi, poller akan mem-publish baris itu sekali lagi (duplikat,
  bukan kehilangan data). Untuk mulai bersih, kosongkan tabel dan topic Lab 07
  dengan `npm run reset -- --topics` sebelum kembali ke lab.
- Debezium adalah demo konsep CDC. Peserta tidak menjalankan Connect di laptop
  masing-masing.
