'use strict';

const { createKafka } = require('../src/kafka');
const { createEnvelope } = require('../../../../shared/envelope.js');

const TOPIC = 'tx-demo.v1';
const IDLE_MS = 6000;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function recreateTopic(admin) {
  const existing = await admin.listTopics();
  if (existing.includes(TOPIC)) {
    await admin.deleteTopics({ topics: [TOPIC], timeout: 15000 });
    for (let attempt = 0; attempt < 40; attempt += 1) {
      const now = await admin.listTopics();
      if (!now.includes(TOPIC)) break;
      await sleep(500);
    }
  }
  await admin.createTopics({
    timeout: 15000,
    topics: [{
      topic: TOPIC,
      numPartitions: 3,
      replicationFactor: 3,
      configEntries: [{ name: 'min.insync.replicas', value: '2' }],
    }],
  });
}

function makeEvent(label) {
  return createEnvelope({
    eventType: 'tx.demo.v1',
    correlationId: `abort-demo-${label}`,
    data: { label },
  });
}

// Write twice: first transaction is aborted, second is committed.
async function writeTransactions() {
  const producer = createKafka('abort-demo').producer({
    kafkaJS: {
      transactionalId: `abort-demo-${Date.now()}`,
      idempotent: true,
      maxInFlightRequests: 5,
      acks: -1,
    },
  });

  await producer.connect();

  await producer.transaction();
  await producer.send({
    topic: TOPIC,
    messages: [{ key: 'aborted', value: JSON.stringify(makeEvent('aborted')) }],
  });
  await producer.abort();
  console.log(`transaksi 1: ABORT (key=aborted) pada ${TOPIC}`);

  await producer.transaction();
  await producer.send({
    topic: TOPIC,
    messages: [{ key: 'committed', value: JSON.stringify(makeEvent('committed')) }],
  });
  await producer.commit();
  console.log(`transaksi 2: COMMIT (key=committed) pada ${TOPIC}`);

  await producer.disconnect();
}

async function readKeys(readUncommitted) {
  const isolation = readUncommitted ? 'read_uncommitted' : 'read_committed';
  const groupId = `abort-demo-${isolation}-${Date.now()}`;
  const consumer = createKafka(`abort-demo-${isolation}`).consumer({
    kafkaJS: { groupId, fromBeginning: true, readUncommitted },
  });

  const keys = [];
  let idleTimer;
  let resolveIdle;
  const idle = new Promise((resolve) => { resolveIdle = resolve; });
  const armIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(resolveIdle, IDLE_MS);
  };

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });
  armIdle();
  const running = consumer.run({
    eachMessage: async ({ message }) => {
      keys.push(message.key ? message.key.toString('utf8') : null);
      armIdle();
    },
  });

  await idle;
  await consumer.stop().catch(() => {});
  await running.catch(() => {});
  await consumer.disconnect().catch(() => {});
  return keys.sort();
}

async function main() {
  const admin = createKafka('abort-demo-admin').admin();
  await admin.connect();
  await recreateTopic(admin);
  await admin.disconnect();
  console.log(`topic ${TOPIC} dibuat ulang`);

  await writeTransactions();

  const committed = await readKeys(false);
  const uncommitted = await readKeys(true);

  console.log('');
  console.log(`read_committed   melihat : [${committed.join(', ')}]`);
  console.log(`read_uncommitted melihat : [${uncommitted.join(', ')}]`);
  console.log('');

  const committedOk = committed.length === 1 && committed[0] === 'committed';
  const uncommittedOk = uncommitted.length === 2
    && uncommitted.includes('aborted')
    && uncommitted.includes('committed');

  if (committedOk && uncommittedOk) {
    console.log('HASIL: OK - record dari transaksi abort tidak terlihat oleh read_committed, terlihat oleh read_uncommitted');
    process.exit(0);
  }
  console.log('HASIL: GAGAL - hasil tidak sesuai harapan');
  process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
