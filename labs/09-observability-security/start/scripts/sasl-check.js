#!/usr/bin/env node
// Connect to the SASL_PLAINTEXT listener with SCRAM-SHA-512, produce one
// message, and read it back. A wrong password must fail at connect; the
// correct one must round-trip.
//
//   node scripts/sasl-check.js
//   node scripts/sasl-check.js --password salah
//
// Production uses SASL_SSL; this lab cluster exposes SASL_PLAINTEXT because the
// certificates would add setup noise without changing the client config below
// (`ssl: true` is the only difference).
import { randomUUID } from 'node:crypto';
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;

const args = process.argv.slice(2);
const argOf = (name, fallback) => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? fallback : args[index + 1];
};

const brokers = argOf(
  'brokers',
  process.env.SASL_BROKERS ?? 'localhost:9192,localhost:9193,localhost:9194',
).split(',');
const username = argOf('user', process.env.SASL_USER ?? 'app');
const password = argOf('password', process.env.SASL_PASSWORD ?? 'app-secret');
const topic = argOf('topic', process.env.SASL_TOPIC ?? 'lab09.sasl-check.v1');

const kafka = new Kafka({
  kafkaJS: {
    clientId: 'lab09-sasl-check',
    brokers,
    // LATIHAN: listener 9192-9194 memakai SASL_PLAINTEXT dengan SCRAM-SHA-512.
    // Tambahkan `ssl: false` dan objek `sasl` di sini:
    //   sasl: { mechanism: 'scram-sha-512', username, password }
    logLevel: logLevel.ERROR,
    connectionTimeout: 5000,
  },
});

const runId = randomUUID();
const message = JSON.stringify({
  event: 'sasl-check',
  runId,
  at: new Date().toISOString(),
});

const admin = kafka.admin();
const producer = kafka.producer();
const consumer = kafka.consumer({
  // The Confluent client reads auto.offset.reset from the top level; the
  // kafkaJS.fromBeginning option alone is not enough here.
  'auto.offset.reset': 'earliest',
  kafkaJS: { groupId: `lab09-sasl-check-${runId}` },
});

const finished = Promise.withResolvers();
let idleTimer;
const resetIdle = () => {
  clearTimeout(idleTimer);
  idleTimer = setTimeout(() => finished.resolve({ found: false }), 5000);
};

async function main() {
  await admin.connect();
  const existing = await admin.listTopics();
  if (!existing.includes(topic)) {
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: 3,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        },
      ],
    });
    console.log(JSON.stringify({ event: 'topic-created', topic }));
  }
  await admin.disconnect();

  await producer.connect();
  const [metadata] = await producer.send({
    topic,
    messages: [{ key: runId, value: message }],
  });
  await producer.disconnect();
  console.log(
    JSON.stringify({
      event: 'produced',
      topic: metadata.topicName,
      partition: metadata.partition,
      offset: metadata.baseOffset,
    }),
  );

  await consumer.connect();
  await consumer.subscribe({ topics: [topic], kafkaJS: { fromBeginning: true } });
  resetIdle();
  const running = consumer.run({
    eachMessage: async ({ topic: name, partition, message: record }) => {
      resetIdle();
      if (record.value?.toString() !== message) {
        return;
      }
      finished.resolve({
        found: true,
        topic: name,
        partition,
        offset: record.offset,
      });
    },
  });

  const result = await finished.promise;
  await consumer.disconnect();
  await running.catch(() => {});
  clearTimeout(idleTimer);

  if (!result.found) {
    throw new Error(`pesan ${runId} tidak ditemukan di ${topic}`);
  }
  console.log(
    JSON.stringify({
      event: 'consumed',
      topic: result.topic,
      partition: result.partition,
      offset: result.offset,
      username,
      mechanism: 'SCRAM-SHA-512',
    }),
  );
  console.log(
    JSON.stringify({
      event: 'sasl-check-ok',
      brokers,
      username,
      mechanism: 'SCRAM-SHA-512',
    }),
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify({
      event: 'sasl-check-failed',
      error: error.name,
      message: error.message,
    }),
  );
  process.exit(1);
});
