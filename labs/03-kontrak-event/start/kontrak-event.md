# Kontrak event — toko daring (template)

Lengkapi bagian yang ditandai `LATIHAN` sebelum lab berakhir. Kontrak yang
sudah jadi ada di `solution/kontrak-event.md`.

## Aturan umum

| Hal | Ketetapan |
|---|---|
| Nama topic | LATIHAN: tulis pola `<domain>.<event>.v<versi>` |
| Key | LATIHAN: field apa yang dipakai sebagai key, dan mengapa |
| Partisi | LATIHAN |
| Retry | LATIHAN: pola nama topic retry |
| DLT | LATIHAN: pola nama topic DLT |
| Header | LATIHAN: header wajib |

## Envelope

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `eventId` | | | |
| `eventType` | | | |
| `version` | | | |
| `occurredAt` | | | |
| `correlationId` | | | |
| `causationId` | | | |
| `data` | | | |

## `order.created.v1`

| Hal | Nilai |
|---|---|
| Producer | LATIHAN |
| Consumer | LATIHAN |
| Key | LATIHAN |
| Pemicu | LATIHAN |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| LATIHAN | | | |

Contoh payload:

```json
{}
```

## `payment.completed.v1`

| Hal | Nilai |
|---|---|
| Producer | LATIHAN |
| Consumer | LATIHAN |
| Key | LATIHAN |
| Pemicu | LATIHAN |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| LATIHAN | | | |

Contoh payload:

```json
{}
```

## `payment.failed.v1`

| Hal | Nilai |
|---|---|
| Producer | LATIHAN |
| Consumer | LATIHAN |
| Key | LATIHAN |
| Pemicu | LATIHAN |

`data`:

| Field | Tipe | Wajib | Keterangan |
|---|---|---|---|
| LATIHAN | | | |

Contoh payload:

```json
{}
```

## Aturan evolusi

LATIHAN: tulis kapan perubahan dianggap kompatibel dan kapan harus membuat
versi topic baru.
