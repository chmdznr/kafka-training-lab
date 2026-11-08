'use strict';

const { pool, ensureSchema } = require('../src/db');
const { createKafka } = require('../src/kafka');
const { TOPICS } = require('../src/config');

// 3 partitions, replication factor 3, min.insync.replicas=2, as in the shared
// contract. auto.create.topics.enable=false on the cluster, so this is needed.
const TOPIC_CONFIGS = [
  { topic: TOPICS.orderCreated },
  { topic: TOPICS.paymentCompleted },
];

async function ensureTopics() {
  const admin = createKafka('lab07-setup').admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = TOPIC_CONFIGS.filter(({ topic }) => !existing.has(topic));

    if (missing.length > 0) {
      await admin.createTopics({
        timeout: 15000,
        topics: missing.map(({ topic }) => ({
          topic,
          numPartitions: 3,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        })),
      });
    }

    for (const { topic } of TOPIC_CONFIGS) {
      console.log(`topic ${topic}: ${existing.has(topic) ? 'sudah ada' : 'dibuat'}`);
    }
  } finally {
    await admin.disconnect();
  }
}

async function main() {
  await ensureSchema();
  await ensureTopics();
  await pool.end();
  console.log('database dan topic siap');
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { ensureTopics, TOPIC_CONFIGS };
