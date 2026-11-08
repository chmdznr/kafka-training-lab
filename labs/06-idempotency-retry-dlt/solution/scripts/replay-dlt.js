#!/usr/bin/env node
// Replays every message in the DLT back to the main topic, keeping the event id
// so the idempotent consumer still processes each event exactly once.
//
// The consumer group is unique per run, so the script always reads the topic
// from the beginning. Stops after 1.5 seconds without a new message.
import { createRequire } from 'node:module';
import pkg from '@confluentinc/kafka-javascript';

const require = createRequire(import.meta.url);
const { headersFor, validateEnvelope } = require('../../../../shared/envelope.js');

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const dltTopic = process.env.DLT_TOPIC ?? 'order.created.v1.payment-service.dlt';
const defaultTarget = 'order.created.v1';

// Optional filter, useful when the DLT contains other incidents:
//   node scripts/replay-dlt.js --event-id <eventId>
const args = process.argv.slice(2);
const eventIdFilter = args.includes('--event-id')
  ? args[args.indexOf('--event-id') + 1]
  : undefined;

const kafka = new Kafka({
  kafkaJS: { clientId: 'lab06-replay', brokers, logLevel: logLevel.NOTHING },
});

// A fresh group every run: replay is an explicit operator action.
const consumer = kafka.consumer({
  "auto.offset.reset": "earliest",
  kafkaJS: { groupId: `replay-dlt-${Date.now()}` },
});
const producer = kafka.producer();

const finished = Promise.withResolvers();
let idleTimer = setTimeout(() => finished.resolve(), 3000);
const resetIdle = () => {
  clearTimeout(idleTimer);
  idleTimer = setTimeout(() => finished.resolve(), 1500);
};

let replayed = 0;
let skipped = 0;
let filtered = 0;

await consumer.connect();
try {
  await consumer.subscribe({ topics: [dltTopic], kafkaJS: { fromBeginning: true } });
} catch (error) {
  console.error(`tidak bisa membaca ${dltTopic}: ${error.message}`);
  await consumer.disconnect();
  process.exit(1);
}

await producer.connect();
const running = consumer.run({
  eachMessage: async ({ message }) => {
    resetIdle();
    let envelope;
    try {
      envelope = JSON.parse(message.value?.toString() ?? '');
      validateEnvelope(envelope);
    } catch (error) {
      skipped += 1;
      console.error(`lewati pesan rusak di ${dltTopic}: ${error.message}`);
      return;
    }

    if (eventIdFilter && envelope.eventId !== eventIdFilter) {
      filtered += 1;
      return;
    }

    const header = (name) => {
      const value = message.headers?.[name];
      return Buffer.isBuffer(value) ? value.toString() : value;
    };
    const target = header('original-topic') ?? defaultTarget;
    const headers = headersFor(envelope);

    await producer.send({
      topic: target,
      messages: [{ key: message.key, value: JSON.stringify(envelope), headers }],
    });
    replayed += 1;
    console.log(
      JSON.stringify({
        event: 'replayed',
        eventId: envelope.eventId,
        orderId: envelope.data?.orderId,
        target,
        originalError: header('error-message'),
      }),
    );
  },
});

await finished.promise;
await consumer.disconnect();
await running.catch((error) => console.error(`consumer berhenti: ${error.message}`));
await producer.disconnect();

console.log(
  JSON.stringify({
    event: 'replay-done',
    dltTopic,
    replayed,
    skipped,
    filtered,
  }),
);
