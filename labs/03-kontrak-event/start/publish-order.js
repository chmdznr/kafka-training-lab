'use strict';

// Lab 03 — titik awal.
// Lengkapi bagian LATIHAN, lalu jalankan: node start/publish-order.js
// Helper envelope ada di ../../../shared/envelope.js.

const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;
const { createEnvelope, validateEnvelope, headersFor } = require('../../../shared/envelope.js');

// Topic sandbox Lab 03. Event rusak tidak boleh masuk order.created.v1, karena
// lab berikutnya membaca topic itu dari awal.
const TOPIC = 'lab03.order.created.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');

function latihan(message) {
  throw new Error(`LATIHAN: ${message}`);
}

async function main() {
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab03-publish', brokers: BROKERS, logLevel: KafkaJS.logLevel.ERROR },
  });

  // LATIHAN: buat topic lab03.order.created.v1 (TOPIC) bila belum ada (3 partisi, RF3,
  // min.insync.replicas=2) memakai admin client.

  const orderId = `order-${randomUUID()}`;
  const correlationId = `checkout-${randomUUID()}`;

  // LATIHAN: buat envelope dengan createEnvelope:
  // eventType 'order.created.v1', version 1, correlationId di atas,
  // data { orderId, customerId, amount, currency }.
  const event = null;
  latihan('buat envelope dengan createEnvelope dan kirim event valid + event tanpa correlationId');

  // LATIHAN: buat satu event rusak yang tidak punya correlationId,
  // lalu kirim keduanya ke TOPIC dengan key orderId.
  // Header memakai headersFor(event); key harus sama agar satu partisi.

  // LATIHAN: consumer dengan groupId unik dan fromBeginning=true.
  // Panggil validateEnvelope untuk setiap record. Event valid harus lolos;
  // event tanpa correlationId harus melempar error.
}

main().catch((error) => {
  console.error(`FAIL lab03 publish: ${error.message}`);
  process.exit(1);
});
