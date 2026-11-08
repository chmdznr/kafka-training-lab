'use strict';

// Lab 02 bagian 2 — titik awal.
// Lengkapi bagian LATIHAN, lalu jalankan: node start/durability.js

const { execFileSync } = require('node:child_process');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const ROOT = path.resolve(__dirname, '../../..');
const TOPIC = 'lab02.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function latihan(message) {
  throw new Error(`LATIHAN: ${message}`);
}

function compose(...args) {
  execFileSync('docker', ['compose', ...args], { cwd: ROOT, stdio: 'inherit' });
}

async function main() {
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab02-durability', brokers: BROKERS, logLevel: KafkaJS.logLevel.ERROR },
  });
  const admin = kafka.admin();
  await admin.connect();

  // LATIHAN: pastikan topic ada, lalu cari partisi dan broker leader-nya.
  // const [meta] = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  // const target = meta.partitions[0];
  // Nama broker: `kafka-${target.leader}`.
  latihan('baca metadata dan pilih partisi + broker leader');

  await admin.disconnect();

  // LATIHAN: buat producer dengan acks=-1. Untuk menangkap error broker
  // dengan cepat, gunakan retries: 0 dan 'message.timeout.ms': 12000.
  // Untuk pengiriman ulang setelah pulih, gunakan idempotent: true.
  const producer = kafka.producer({
    kafkaJS: {
      /* LATIHAN */
    },
  });
  await producer.connect();
  latihan('lengkapi alur stop broker, kirim batch, tangkap NOT_ENOUGH_REPLICAS, nyalakan broker, kirim ulang, verifikasi tanpa duplikat');

  // LATIHAN:
  // 1. Matikan dua broker yang bukan leader partisi target.
  // 2. Tunggu ISR menyusut (sekitar 30 detik).
  // 3. Kirim batch dengan acks=-1, tangkap error, dan cetak pesannya.
  //    Error yang dicari mengandung NOT_ENOUGH_REPLICAS.
  // 4. Nyalakan kembali satu broker, tunggu ISR >= 2.
  // 5. Kirim ulang batch yang gagal.
  // 6. Baca partisi dari awal dan hitung bahwa setiap pesan hanya muncul
  //    sekali untuk runId ini.

  await producer.disconnect();
}

main().catch((error) => {
  console.error(`FAIL lab02 durability: ${error.message}`);
  process.exit(1);
});
