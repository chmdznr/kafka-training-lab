'use strict';

// Lab 04 part 3: reset offsets to earliest via kafka-consumer-groups.sh,
// replay the messages, then make sure lag returns to 0.
// Run: node solution/replay.js

const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const ROOT = path.resolve(__dirname, '../../..');
const WORKER = path.join(__dirname, 'worker.js');
const TOPIC = 'lab04.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const MESSAGES = 30;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(200);
  }
  throw new Error(`timeout: ${label}`);
}

function groupCli(group, ...args) {
  return execFileSync(
    'docker',
    [
      'compose', 'exec', '-T', 'kafka-1',
      '/opt/kafka/bin/kafka-consumer-groups.sh',
      '--bootstrap-server', 'kafka-1:29092',
      '--group', group,
      ...args,
    ],
    { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
}

function totalLag(group) {
  let output;
  try {
    output = groupCli(group, '--describe');
  } catch {
    return Number.POSITIVE_INFINITY;
  }
  let sum = 0;
  for (const line of output.split('\n')) {
    const cols = line.trim().split(/\s+/);
    if (cols.length >= 6 && /^\d+$/.test(cols[2]) && /^\d+$/.test(cols[5])) {
      sum += Number(cols[5]);
    }
  }
  return sum;
}

function spawnWorker(name, group) {
  const child = spawn(process.execPath, [WORKER], {
    env: {
      ...process.env,
      LAB04_NAME: name,
      LAB04_GROUP: group,
      LAB04_PROTOCOL: 'classic',
      LAB04_FROM_BEGINNING: '1',
      LAB04_AUTO_COMMIT: '1',
      LAB04_TOPIC: TOPIC,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const state = { name, child, messages: [], stderr: '' };
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
        if (entry.event === 'msg') state.messages.push(entry);
      } catch {
        // ignore
      }
    }
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => {
    state.stderr += chunk;
  });

  return state;
}

async function stopWorker(worker) {
  worker.child.kill('SIGTERM');
  await Promise.race([
    waitFor(() => worker.child.exitCode !== null, 15000, 'worker berhenti'),
    sleep(15000),
  ]);
  if (worker.child.exitCode === null) worker.child.kill('SIGKILL');
}

async function main() {
  const runId = randomUUID();
  const group = `lab04-replay-${Date.now()}`;
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab04-replay', brokers: BROKERS, logLevel: KafkaJS.logLevel.NOTHING },
  });

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

  const producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await producer.connect();
  for (let seq = 0; seq < MESSAGES; seq++) {
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: `replay-${seq % 3}`,
          value: JSON.stringify({ runId, seq, source: 'lab04-replay' }),
        },
      ],
    });
  }
  await producer.disconnect();
  console.log(`PASS  produce ${MESSAGES} pesan penanda (runId ${runId})`);

  const first = spawnWorker('replay-1', group);
  await waitFor(
    () => first.messages.filter((message) => message.value?.runId === runId).length >= MESSAGES,
    120000,
    'consumer pertama selesai membaca pesan',
  );
  await sleep(1500);
  await stopWorker(first);

  await waitFor(() => totalLag(group) === 0, 60000, 'lag 0 setelah konsumsi pertama');
  console.log('PASS  lag 0 sebelum reset');
  console.log(groupCli(group, '--describe').trim());

  console.log('\n== Reset offset ke earliest');
  const reset = groupCli(group, '--reset-offsets', '--to-earliest', '--execute', '--topic', TOPIC);
  console.log(reset.trim());

  const second = spawnWorker('replay-2', group);
  await waitFor(
    () => second.messages.filter((message) => message.value?.runId === runId).length >= MESSAGES,
    180000,
    'consumer kedua selesai memutar ulang pesan',
  );
  const replayed = second.messages.filter((message) => message.value?.runId === runId).length;
  await sleep(1500);
  await stopWorker(second);

  await waitFor(() => totalLag(group) === 0, 60000, 'lag 0 setelah replay');
  console.log('PASS  lag 0 sesudah replay');
  console.log(groupCli(group, '--describe').trim());

  console.log(
    `PASS lab04 replay: lag 0 sebelum reset, ${replayed} pesan diputar ulang dengan offset earliest, lag 0 sesudah`,
  );
}

main().catch((error) => {
  console.error(`FAIL lab04 replay: ${error.message}`);
  process.exit(1);
});
