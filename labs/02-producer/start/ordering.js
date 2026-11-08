'use strict';

// Lab 02 bagian 1 — titik awal.
// Lengkapi bagian LATIHAN, lalu jalankan: node start/ordering.js

const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const TOPIC = 'lab02.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const KEYS = ['order-1', 'order-2', 'order-3', 'order-4', 'order-5', 'order-6'];
const PER_KEY = 10;

function latihan(message) {
  throw new Error(`LATIHAN: ${message}`);
}

function makeKafka(clientId) {
  return new Kafka({
    kafkaJS: { clientId, brokers: BROKERS, logLevel: KafkaJS.logLevel.ERROR },
    'socket.timeout.ms': 10000,
  });
}

async function main() {
  const runId = randomUUID();
  const kafka = makeKafka('lab02-ordering');
  const admin = kafka.admin();
  await admin.connect();

  // LATIHAN: buat topic lab02.orders.v1 bila belum ada:
  // 3 partisi, replication factor 3, min.insync.replicas=2.
  // Petunjuk: admin.createTopics({ topics: [...] }).
  latihan('buat topic lab02.orders.v1 (3 partisi, RF3, min.insync.replicas=2)');

  await admin.disconnect();

  // LATIHAN: buat producer dengan acks=-1 dan idempotent=true,
  // lalu kirim KEYS x PER_KEY pesan. Setiap pesan punya key dan value
  // JSON { runId, key, seq }.
  const producer = kafka.producer({
    kafkaJS: {
      /* LATIHAN */
    },
  });
  await producer.connect();
  // LATIHAN: loop pengiriman.
  latihan('kirim KEYS x PER_KEY pesan dengan key dan value JSON { runId, key, seq }');
  await producer.disconnect();

  // LATIHAN: buat consumer dengan groupId unik dan fromBeginning=true.
  // Kumpulkan pesan milik runId ini, lalu buktikan:
  // 1. semua pesan dengan key yang sama berada di partisi yang sama;
  // 2. seq per key berurutan 1..PER_KEY.
  const consumer = kafka.consumer({
    kafkaJS: {
      /* LATIHAN */
    },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });

  // LATIHAN: consumer.run({ eachMessage }) dan validasi.
  latihan('baca ulang pesan, lalu validasi partisi per key dan urutan seq');

  await consumer.disconnect();
}

main().catch((error) => {
  console.error(`FAIL lab02 ordering: ${error.message}`);
  process.exit(1);
});
