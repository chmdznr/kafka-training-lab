'use strict';

// Lab 02 part 1: the same key always maps to the same partition, so order is
// preserved per key. Run: node solution/ordering.js

const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const TOPIC = 'lab02.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const KEYS = ['order-1', 'order-2', 'order-3', 'order-4', 'order-5', 'order-6'];
const PER_KEY = 10;

function makeKafka(clientId) {
  return new Kafka({
    kafkaJS: { clientId, brokers: BROKERS, logLevel: KafkaJS.logLevel.ERROR },
    'socket.timeout.ms': 10000,
  });
}

async function ensureTopic(admin) {
  const topics = await admin.listTopics();
  if (topics.includes(TOPIC)) return;
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

async function main() {
  const runId = randomUUID();
  const kafka = makeKafka('lab02-ordering');
  const admin = kafka.admin();
  await admin.connect();
  await ensureTopic(admin);
  await admin.disconnect();

  const producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await producer.connect();

  // Interleave keys. The producer awaits every batch, so the send order for
  // one key is strictly increasing.
  for (let seq = 1; seq <= PER_KEY; seq++) {
    for (const key of KEYS) {
      await producer.send({
        topic: TOPIC,
        messages: [{ key, value: JSON.stringify({ runId, key, seq }) }],
      });
    }
  }
  await producer.disconnect();

  const consumer = kafka.consumer({
    kafkaJS: { groupId: `lab02-ordering-${runId}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });

  const perKey = new Map();
  let received = 0;

  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timeout: hanya menerima ${received} dari ${KEYS.length * PER_KEY} pesan`)),
      60000,
    );

    consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          let value;
          try {
            value = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (value.runId !== runId) return;

          const key = message.key?.toString();
          const state = perKey.get(key) ?? { partitions: new Set(), seqs: [] };
          state.partitions.add(partition);
          state.seqs.push(value.seq);
          perKey.set(key, state);
          received += 1;

          if (received === KEYS.length * PER_KEY) {
            clearTimeout(timer);
            resolve();
          }
        },
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });

  await consumer.disconnect();

  let failed = false;
  for (const key of KEYS) {
    const state = perKey.get(key);
    if (!state) {
      console.log(`FAIL  key ${key} tidak pernah diterima`);
      failed = true;
      continue;
    }
    const expected = Array.from({ length: PER_KEY }, (_, i) => i + 1);
    const ordered = JSON.stringify(state.seqs) === JSON.stringify(expected);
    // Same key, same partition: every message of this key must land in one partition.
    const onePartition = state.partitions.size === 1;
    const ok = ordered && onePartition;
    console.log(
      `${ok ? 'PASS' : 'FAIL'}  ${key} -> partisi ${[...state.partitions].join(',')}, seq ${state.seqs.join(',')}`,
    );
    if (!onePartition) console.log(`FAIL  ${key} tersebar di ${state.partitions.size} partisi`);
    if (!ok) failed = true;
  }

  if (failed) {
    process.exit(1);
  }
  console.log(`PASS lab02 ordering: ${KEYS.length} key x ${PER_KEY} pesan, satu partisi dan urutan benar per key`);
}

main().catch((error) => {
  console.error(`FAIL lab02 ordering: ${error.message}`);
  process.exit(1);
});
