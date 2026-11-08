'use strict';

// Lab 04 — titik awal worker consumer.
// Lengkapi bagian LATIHAN, lalu pakai file ini dari skrip orkestrasi.

const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const NAME = process.env.LAB04_NAME || 'worker';
const GROUP = process.env.LAB04_GROUP;
const PROTOCOL = process.env.LAB04_PROTOCOL || 'classic';
const TOPIC = process.env.LAB04_TOPIC || 'lab04.orders.v1';
const FROM_BEGINNING = process.env.LAB04_FROM_BEGINNING === '1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');

if (!GROUP) {
  console.error('LAB04_GROUP wajib diisi');
  process.exit(2);
}

function log(entry) {
  process.stdout.write(`${JSON.stringify({ ts: Date.now(), name: NAME, ...entry })}\n`);
}

const kafka = new Kafka({
  kafkaJS: { clientId: `lab04-${NAME}`, brokers: BROKERS, logLevel: KafkaJS.logLevel.NOTHING },
});

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP,
    fromBeginning: FROM_BEGINNING,
    autoCommit: true,
    autoCommitInterval: 300,
  },
  // classic or consumer (KIP-848), selected by LAB04_PROTOCOL.
  'group.protocol': PROTOCOL,
});

// LATIHAN: tangani SIGTERM: cetak event 'leaving', panggil
// consumer.disconnect(), lalu process.exit(0).

(async () => {
  await consumer.connect();
  log({ event: 'connected', group: GROUP, protocol: PROTOCOL, topic: TOPIC });
  await consumer.subscribe({ topic: TOPIC });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let value = null;
      try {
        value = JSON.parse(message.value.toString());
      } catch {
        value = message.value.toString();
      }
      log({ event: 'msg', partition, offset: message.offset, key: message.key?.toString(), value });
    },
  });
})().catch((error) => {
  log({ event: 'error', message: error.message });
  process.exit(1);
});
