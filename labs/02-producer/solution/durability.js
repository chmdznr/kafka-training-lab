'use strict';

// Lab 02 part 2: with min.insync.replicas=2, acks=-1 is rejected while two
// brokers are down. After one broker returns, the failed batch is resent and
// verified to produce no duplicates.
// Run: node solution/durability.js

const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const path = require('node:path');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const ROOT = path.resolve(__dirname, '../../..');
const TOPIC = 'lab02.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const BATCH = 5;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function compose(...args) {
  execFileSync('docker', ['compose', ...args], { cwd: ROOT, stdio: 'inherit' });
}

function isrSize(broker, partition) {
  try {
    const output = execFileSync(
      'docker',
      [
        'compose', 'exec', '-T', broker,
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', `${broker}:29092`,
        '--describe', '--topic', TOPIC,
      ],
      { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    );
    const line = output
      .split('\n')
      .find((candidate) => new RegExp(`Partition: ${partition}[^0-9]`).test(candidate));
    const match = line?.match(/Isr: ([0-9,]*)/);
    return match ? match[1].split(',').filter(Boolean).length : 0;
  } catch {
    return 0;
  }
}

async function waitForIsr(broker, partition, wanted, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const size = isrSize(broker, partition);
    if (size >= wanted) return size;
    await sleep(500);
  }
  return isrSize(broker, partition);
}

function isNotEnoughReplicas(error) {
  return (
    error?.code === 19 ||
    /NOT_ENOUGH_REPLICAS|not enough in-sync replicas|not enough replicas/i.test(error?.message ?? '')
  );
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

async function collectRun(kafka, partition, runId, expected) {
  const consumer = kafka.consumer({
    kafkaJS: { groupId: `lab02-verify-${runId}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });

  const found = [];
  let settle;
  let resolveDone;
  let rejectDone;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  const timeout = setTimeout(() => rejectDone(new Error('timeout membaca ulang batch')), 45000);

  await consumer.run({
    eachMessage: async ({ partition: messagePartition, message }) => {
      if (messagePartition !== partition) return;
      let value;
      try {
        value = JSON.parse(message.value.toString());
      } catch {
        return;
      }
      if (value.runId !== runId) return;
      found.push({ key: message.key?.toString(), seq: value.seq });
      if (found.length >= expected) {
        clearTimeout(settle);
        settle = setTimeout(() => resolveDone(), 1500);
      }
    },
  });

  await done;
  clearTimeout(timeout);
  clearTimeout(settle);
  await consumer.disconnect();
  return found;
}

async function main() {
  const runId = randomUUID();
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab02-durability', brokers: BROKERS, logLevel: KafkaJS.logLevel.NOTHING },
  });

  const admin = kafka.admin();
  await admin.connect();
  await ensureTopic(admin);
  const [metadata] = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  const target = metadata.partitions.find((candidate) => candidate.leader === 1) ?? metadata.partitions[0];
  const partition = target.partitionId;
  const survivor = `kafka-${target.leader}`;
  const victims = ['kafka-1', 'kafka-2', 'kafka-3'].filter((broker) => broker !== survivor);
  await admin.disconnect();

  console.log(`Partisi ${partition} dipimpin ${survivor}; broker yang dimatikan: ${victims.join(', ')}`);

  const payloads = Array.from({ length: BATCH }, (_, index) => ({
    key: `durability-${runId}-${index}`,
    value: JSON.stringify({ runId, seq: index + 1 }),
  }));

  const failFast = kafka.producer({
    kafkaJS: { acks: -1 },
    retries: 0,
    'message.timeout.ms': 10000,
  });
  await failFast.connect();

  let failure;
  try {
    compose('stop', ...victims);
    await sleep(1500);

    console.log('Mengirim batch dengan acks=-1 saat ISR < 2...');
    for (let attempt = 0; attempt < 10; attempt += 1) {
      try {
        await failFast.send({
          topic: TOPIC,
          messages: payloads.map((payload) => ({
            partition,
            key: payload.key,
            value: payload.value,
            headers: { 'correlation-id': runId },
          })),
        });
        failure = null;
        break;
      } catch (error) {
        failure = error;
        if (isNotEnoughReplicas(error)) break;
        await sleep(1000);
      }
    }

    if (!failure || !isNotEnoughReplicas(failure)) {
      throw new Error(`error yang diharapkan tidak muncul: ${failure?.message ?? 'send berhasil'}`);
    }
    console.log(`PASS  ditolak: code=${failure.code} ${failure.message}`);
    await failFast.disconnect();

    console.log(`Menyalakan ${victims[0]} dan menunggu ISR >= 2...`);
    compose('start', victims[0]);
    const isrAfterRestart = await waitForIsr(survivor, partition, 2, 90000);
    if (isrAfterRestart < 2) {
      throw new Error(`ISR tidak pulih (${isrAfterRestart})`);
    }
    console.log(`PASS  ISR partisi ${partition} kembali ${isrAfterRestart}`);

    const sender = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
    await sender.connect();
    await sender.send({
      topic: TOPIC,
      messages: payloads.map((payload) => ({
        partition,
        key: payload.key,
        value: payload.value,
        headers: { 'correlation-id': runId },
      })),
    });
    await sender.disconnect();
    console.log('PASS  batch dikirim ulang dan diterima');

    const found = await collectRun(kafka, partition, runId, BATCH);
    const uniqueMessages = new Set(found.map((message) => `${message.key}:${message.seq}`));
    const uniqueKeys = new Set(found.map((message) => message.key));

    if (found.length !== BATCH || uniqueMessages.size !== BATCH || uniqueKeys.size !== BATCH) {
      throw new Error(
        `duplikat terdeteksi: diterima=${found.length}, unik=${uniqueMessages.size}, key unik=${uniqueKeys.size}, harus ${BATCH}`,
      );
    }
    console.log(`PASS  verifikasi: ${BATCH} pesan, ${uniqueKeys.size} key, tanpa duplikat`);
  } finally {
    await failFast.disconnect().catch(() => {});
    compose('start', ...victims);
  }

  const finalIsr = await waitForIsr(survivor, partition, 3, 90000);
  console.log(`ISR partisi ${partition} setelah semua broker hidup: ${finalIsr}`);
  if (finalIsr < 3) {
    throw new Error('ISR tidak kembali 3 setelah semua broker dinyalakan');
  }

  console.log('PASS lab02 durability: ditolak saat ISR < 2, berhasil setelah broker kembali, tanpa duplikat');
}

main().catch((error) => {
  console.error(`FAIL lab02 durability: ${error.message}`);
  process.exit(1);
});
