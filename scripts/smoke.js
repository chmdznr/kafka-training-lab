'use strict';

// Produce and consume one message through a listener, using the same
// Confluent KafkaJS-compatible API the labs use. verifies the topic is
// replicated, the key round-trips, and the contract headers survive.

const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const TOPIC = 'lab.verify.v1';
const PLAINTEXT_BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094'];
const SASL_BROKERS = ['localhost:9192', 'localhost:9193', 'localhost:9194'];
const SASL = { mechanism: 'scram-sha-512', username: 'app', password: 'app-secret' };

function withTimeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timeout ${ms}ms: ${label}`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function makeClient(label, brokers, sasl) {
  return new Kafka({
    kafkaJS: {
      clientId: `verify-env-${label}`,
      logLevel: KafkaJS.logLevel.NOTHING,
      brokers,
      ...(sasl ? { sasl, ssl: false } : {}),
    },
  });
}

async function ensureTopic(admin) {
  const existing = await admin.listTopics();
  if (!existing.includes(TOPIC)) {
    await admin.createTopics({
      topics: [
        {
          topic: TOPIC,
          numPartitions: 3,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        },
      ],
    });
  }
  // A freshly created topic needs a moment before every broker knows its
  // leaders; producing right away fails with "Unknown topic or partition".
  const deadline = Date.now() + 30000;
  for (;;) {
    try {
      const [metadata] = await admin.fetchTopicMetadata({ topics: [TOPIC] });
      if (metadata.partitions.length === 3 && metadata.partitions.every((p) => p.leader >= 0 && p.isr.length >= 2)) return;
    } catch {
      // metadata not available yet
    }
    if (Date.now() > deadline) throw new Error(`${TOPIC}: partitions not ready after 30s`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

async function checkListener(label, brokers, sasl) {
  const kafka = makeClient(label, brokers, sasl);
  const admin = kafka.admin();
  await withTimeout(admin.connect(), 15000, `${label} admin connect`);
  await ensureTopic(admin);

  const [metadata] = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  if (metadata.partitions.length !== 3) {
    throw new Error(`${label}: expected 3 partitions, got ${metadata.partitions.length}`);
  }
  const replicas = metadata.partitions[0].replicas.length;
  if (replicas !== 3) {
    throw new Error(`${label}: expected replication factor 3, got ${replicas}`);
  }

  const runId = randomUUID();
  const producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await withTimeout(producer.connect(), 15000, `${label} producer connect`);
  await withTimeout(
    producer.send({
      topic: TOPIC,
      messages: [
        {
          key: runId,
          value: JSON.stringify({ runId, source: label }),
          headers: { 'correlation-id': runId, 'event-type': 'order.created.v1' },
        },
      ],
    }),
    20000,
    `${label} produce`,
  );
  await producer.disconnect();

  const groupId = `verify-env-${label}-${Date.now()}`;
  const consumer = kafka.consumer({ kafkaJS: { groupId, fromBeginning: true } });
  await withTimeout(consumer.connect(), 15000, `${label} consumer connect`);
  await consumer.subscribe({ topic: TOPIC });

  let resolveFound;
  let rejectFound;
  const found = new Promise((resolve, reject) => {
    resolveFound = resolve;
    rejectFound = reject;
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (message.key?.toString() !== runId) return;
      const value = JSON.parse(message.value.toString());
      if (value.runId !== runId) return;
      if (message.headers?.['correlation-id']?.toString() !== runId) {
        rejectFound(new Error(`${label}: correlation-id header hilang`));
        return;
      }
      resolveFound(value);
    },
  });

  try {
    const received = await withTimeout(found, 30000, `${label} consume own message`);
    await consumer.disconnect();
    return { label, partition: metadata.partitions.length, received: received.source };
  } catch (error) {
    await consumer.disconnect().catch(() => {});
    throw error;
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

async function main() {
  const saslOnly = process.argv.includes('--sasl-only');
  const checks = saslOnly
    ? [['sasl', SASL_BROKERS, SASL]]
    : [
        ['plaintext', PLAINTEXT_BROKERS, null],
        ['sasl', SASL_BROKERS, SASL],
      ];

  for (const [label, brokers, sasl] of checks) {
    const result = await checkListener(label, brokers, sasl);
    console.log(`PASS ${result.label}: produced + consumed ${TOPIC} (RF3, ${result.partition} partitions)`);
  }
  console.log('PASS smoke: PLAINTEXT dan SASL berfungsi');
}

main().catch((error) => {
  console.error(`FAIL smoke: ${error.message}`);
  process.exit(1);
});
