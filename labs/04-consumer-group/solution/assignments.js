'use strict';

// Lab 04 part 1: three consumers on a 3-partition topic, then add a fourth.
// With 3 partitions and 4 consumers, one consumer is always idle.
// Run: node solution/assignments.js

const { spawn } = require('node:child_process');
const path = require('node:path');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const WORKER = path.join(__dirname, 'worker.js');
const TOPIC = 'lab04.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureTopic(kafka) {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(TOPIC)) {
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
  await admin.disconnect();
}


async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(100);
  }
  throw new Error(`timeout: ${label}`);
}

function spawnWorker(name, group) {
  const child = spawn(process.execPath, [WORKER], {
    env: {
      ...process.env,
      LAB04_NAME: name,
      LAB04_GROUP: group,
      LAB04_PROTOCOL: 'classic',
      LAB04_FROM_BEGINNING: '0',
      LAB04_AUTO_COMMIT: '1',
      LAB04_TOPIC: TOPIC,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const state = { name, child, entries: [], messages: [], partitions: new Set(), stderr: '' };
  let buffer = '';

  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk) => {
    buffer += chunk;
    let index;
    while ((index = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, index).trim();
      buffer = buffer.slice(index + 1);
      if (!line) continue;
      try {
        const entry = JSON.parse(line);
        state.entries.push(entry);
        if (entry.event === 'msg') {
          state.messages.push(entry);
          state.partitions.add(entry.partition);
        }
      } catch {
        // ignore lines that are not JSON from the library
      }
    }
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => {
    state.stderr += chunk;
  });

  return state;
}

async function stopAll(workers) {
  for (const worker of workers) worker.child.kill('SIGTERM');
  await Promise.race([
    waitFor(() => workers.every((w) => w.child.exitCode !== null), 15000, 'consumer berhenti'),
    sleep(15000),
  ]);
  for (const worker of workers) {
    if (worker.child.exitCode === null) worker.child.kill('SIGKILL');
  }
}

async function main() {
  const runId = `lab04-assign-${Date.now()}`;
  const group = runId;
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab04-assignments', brokers: BROKERS, logLevel: KafkaJS.logLevel.NOTHING },
  });
  await ensureTopic(kafka);
  const producer = kafka.producer({ kafkaJS: { acks: 1 } });
  await producer.connect();

  const workers = [];
  for (let i = 1; i <= 3; i++) workers.push(spawnWorker(`c${i}`, group));

  await waitFor(
    () => workers.every((worker) => worker.entries.some((entry) => entry.event === 'connected')),
    30000,
    'tiga consumer terhubung',
  );
  await sleep(2000);
  console.log('== Tiga consumer pertama terhubung ke group');

  let seq = 0;
  let sending = true;
  const interval = setInterval(() => {
    if (!sending) return;
    producer
      .send({
        topic: TOPIC,
        messages: [
          { key: `key-${seq % 3}`, value: JSON.stringify({ runId, seq }) },
        ],
      })
      .catch(() => {});
    seq += 1;
  }, 100);

  await waitFor(
    () => workers.every((worker) => worker.messages.length > 0),
    30000,
    'tiga consumer mulai memproses pesan',
  );
  await sleep(1000);
  const before = workers.map((worker) => worker.messages.length);

  console.log('\n== Tambah consumer keempat');
  const fourth = spawnWorker('c4', group);
  workers.push(fourth);
  before.push(0);
  await sleep(10000);

  sending = false;
  clearInterval(interval);
  await sleep(300);
  await producer.disconnect();

  const after = workers.map((worker) => worker.messages.length);
  const deltas = after.map((count, index) => count - before[index]);

  console.log('\n== Pesan yang diproses setelah consumer keempat bergabung (10 detik)');
  workers.forEach((worker, index) => {
    const partitions = [...worker.partitions].sort();
    console.log(`  ${worker.name}: delta=${deltas[index]}, partisi dilihat=${JSON.stringify(partitions)}`);
  });

  const idle = deltas.filter((delta) => delta === 0).length;
  const active = deltas.filter((delta) => delta > 0).length;
  const total = deltas.reduce((sum, delta) => sum + delta, 0);

  await stopAll(workers);

  if (total === 0) throw new Error('tidak ada pesan yang diproses');
  if (idle < 1) throw new Error('tidak ada consumer yang idle; assignment tidak sesuai harapan');
  if (active < 2) throw new Error('terlalu sedikit consumer yang aktif');

  console.log(
    `\nPASS lab04 assignments: ${active} consumer aktif, ${idle} idle, ${total} pesan diproses setelah rebalance`,
  );
}

main().catch(async (error) => {
  console.error(`FAIL lab04 assignments: ${error.message}`);
  process.exit(1);
});
