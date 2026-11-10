# Evidence — build folder `start/` NestJS

Dijalankan: 2026-11-08, Node 24.22.1. Dependensi native dipasang sekali di akar
`lab/` (`npm ci` + `npm run setup:native`); package lab tidak lagi mendeklarasikan
`@confluentinc/kafka-javascript` dan me-resolve-nya dari `lab/node_modules`.

```
$ cd labs/05-nestjs/start && npm ci
added 115 packages in 2s
$ npm run build
> tsc -p tsconfig.json          (exit 0)

$ cd labs/06-idempotency-retry-dlt/start && npm ci
added 115 packages in 2s
$ npm run build
> tsc -p tsconfig.json          (exit 0)

$ cd proyek/start && npm ci
npm warn install-scripts   ssh2@1.17.0 (install: node install.js)
$ npm run build
> tsc -p tsconfig.json          (exit 0)
```

Dari setiap folder, modul native dimuat lewat resolusi direktori induk:

```
$ node -e "import('@confluentinc/kafka-javascript').then(...)"
labs/05-nestjs/start                native-module-ok
labs/05-nestjs/solution             native-module-ok
labs/06-idempotency-retry-dlt/solution  native-module-ok
labs/06-idempotency-retry-dlt/start     native-module-ok
proyek/solution                     native-module-ok
```

Tidak ada perintah `npm install --ignore-scripts` di README lab. `npm ci` di
folder lab tidak menjalankan unduhan rilis GitHub karena paket native bukan
dependensi lokal; binary diambil dari `lab/node_modules` yang disiapkan
`npm run setup:native`.

Perbaikan yang diperlukan agar `start/` build:

- `labs/05-nestjs/start/src/common/consumer-drain.ts` sebelumnya memanggil
  `attach(consumer)` satu argumen, sedangkan controller memanggil dua argumen.
  Salinan ini disamakan dengan `solution/` (menyimpan daftar topic).
- `labs/06-idempotency-retry-dlt/start/` dibuat dari `solution/` dengan gap
  `// LATIHAN:` di tabel/klaim `processed_events`, publish retry dan DLT, serta
  skrip replay.
- `proyek/start/` dibuat dari `solution/` dengan gap `// LATIHAN:` di baris
  outbox order, publish outbox poller, dedup payment, retry/DLT payment, dan
  dedup notification.

## Run ulang 2026-11-11

Setelah NestJS dinaikkan ke `12.4.2` (`npm install --save-exact` di setiap
folder, lockfile hanya berubah pada paket `@nestjs/*`):

```
labs/05-nestjs/start                 ci=0 build=0 nest=12.4.2
labs/06-idempotency-retry-dlt/start  ci=0 build=0 nest=12.4.2
labs/09-observability-security/start ci=0 build=0 nest=12.4.2
proyek/start                         ci=0 build=0 nest=12.4.2
```

`solution/` keempat folder juga `npm ci` + `npm run build` exit 0 sebelum
skenario masing-masing dijalankan.
