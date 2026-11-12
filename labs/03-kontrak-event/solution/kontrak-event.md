# Kontrak event — toko daring

Dokumen ini adalah kesepakatan antara producer dan consumer. Perubahan pada
bagian yang wajib dianggap breaking dan harus lewat versi baru.

## Aturan umum

| Hal | Ketetapan |
|---|---|
| Nama topic | `<domain>.<event>.v<versi>`, huruf kecil, contoh `order.created.v1` |
| Key | `orderId` untuk semua event order dan payment |
| Partisi | 3 partisi, replication factor 3, `min.insync.replicas=2` |
| Retry | `<topic>.<consumer-group>.retry` |
| DLT | `<topic>.<consumer-group>.dlt` |
| Envelope | JSON, field di tabel bawah |
| Header | `correlation-id`, `event-type`, `traceparent` (bila tracing aktif) |

`eventType` berisi nama event lengkap dengan versi, sama dengan nama topic.
Field `version` berisi angka yang sama dengan akhiran `.v<versi>`.
Helper ada di `shared/envelope.js`.

## Envelope

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `eventId` | string UUID | ya | unik untuk satu event; dasar deduplikasi consumer |
| `eventType` | string | ya | nama event + versi, contoh `order.created.v1` |
| `version` | integer >= 1 | ya | sama dengan akhiran versi di `eventType` |
| `occurredAt` | string ISO-8601 | ya | waktu kejadian, harus menyertakan zona waktu (`Z` atau offset) |
| `correlationId` | string | ya | satu nilai untuk seluruh alur order → payment → notification |
| `causationId` | string | tidak | `eventId` dari event yang menyebabkan event ini |
| `data` | object | ya | payload khusus event |

Header yang wajib dikirim bersama setiap record:

| Header | Nilai |
|---|---|
| `correlation-id` | sama dengan `envelope.correlationId` |
| `event-type` | sama dengan `envelope.eventType` |
| `traceparent` | W3C trace context; hanya bila tracing aktif |

## `order.created.v1`

| Hal | Nilai |
|---|---|
| Producer | order-service |
| Consumer | payment-service, notification-service |
| Key | `orderId` |
| Pemicu | order baru tersimpan di database |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `orderId` | string | ya | ID order, juga dipakai sebagai key |
| `customerId` | string | ya | ID pelanggan |
| `amount` | integer | ya | nominal dalam satuan terkecil (rupiah) |
| `currency` | string | ya | kode mata uang, contoh `IDR` |

Contoh:

```json
{
  "eventId": "018f4c1e-6f1a-7a2b-9c3d-2f6b8a1d4e70",
  "eventType": "order.created.v1",
  "version": 1,
  "occurredAt": "2026-11-08T09:15:00.000Z",
  "correlationId": "checkout-7f3a2b",
  "data": {
    "orderId": "order-1001",
    "customerId": "customer-42",
    "amount": 250000,
    "currency": "IDR"
  }
}
```

## `payment.completed.v1`

| Hal | Nilai |
|---|---|
| Producer | payment-service |
| Consumer | notification-service, order-service |
| Key | `orderId` |
| Pemicu | pembayaran berhasil |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `orderId` | string | ya | order yang dibayar |
| `paymentId` | string | ya | ID pembayaran |
| `amount` | integer | ya | nominal yang dibayar |
| `currency` | string | ya | kode mata uang |
| `method` | string | ya | `card`, `transfer`, atau `ewallet` |
| `paidAt` | string ISO-8601 | ya | waktu pembayaran berhasil |

Contoh:

```json
{
  "eventId": "018f4c1e-7a10-7a2b-9c3d-5d1e2f3a4b5c",
  "eventType": "payment.completed.v1",
  "version": 1,
  "occurredAt": "2026-11-08T09:15:04.000Z",
  "correlationId": "checkout-7f3a2b",
  "causationId": "018f4c1e-6f1a-7a2b-9c3d-2f6b8a1d4e70",
  "data": {
    "orderId": "order-1001",
    "paymentId": "payment-9001",
    "amount": 250000,
    "currency": "IDR",
    "method": "card",
    "paidAt": "2026-11-08T09:15:04.000Z"
  }
}
```

## `payment.failed.v1`

| Hal | Nilai |
|---|---|
| Producer | payment-service |
| Consumer | notification-service, order-service |
| Key | `orderId` |
| Pemicu | pembayaran ditolak atau gagal |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `orderId` | string | ya | order yang gagal dibayar |
| `paymentId` | string | ya | ID percobaan pembayaran |
| `amount` | integer | ya | nominal percobaan |
| `currency` | string | ya | kode mata uang |
| `reason` | string | ya | alasan singkat yang aman ditampilkan |
| `failedAt` | string ISO-8601 | ya | waktu kegagalan |

Contoh:

```json
{
  "eventId": "018f4c1e-8b20-7a2b-9c3d-6e7f8a9b0c1d",
  "eventType": "payment.failed.v1",
  "version": 1,
  "occurredAt": "2026-11-08T09:16:12.000Z",
  "correlationId": "checkout-7f3a2b",
  "causationId": "018f4c1e-6f1a-7a2b-9c3d-2f6b8a1d4e70",
  "data": {
    "orderId": "order-1001",
    "paymentId": "payment-9002",
    "amount": 250000,
    "currency": "IDR",
    "reason": "insufficient_funds",
    "failedAt": "2026-11-08T09:16:12.000Z"
  }
}
```

## Aturan evolusi

1. Menambah field opsional di `data` dan consumer mengabaikan field yang tidak
   dikenal: aman, tetap versi yang sama.
2. Mengubah arti field, menghapus field wajib, atau mengubah tipe: breaking,
   buat topic dan `eventType` versi baru (`order.created.v2`).
3. Consumer harus memvalidasi envelope sebelum memproses `data`. Event yang
   tidak valid tidak boleh diproses; kirim ke DLT atau log sebagai poison
   message.
4. `causationId` diisi oleh consumer saat mem-publish event turunan, memakai
   `eventId` event yang sedang diproses. Dengan begitu alur bisa ditelusuri.
