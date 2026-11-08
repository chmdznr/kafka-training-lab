'use strict';

// Lab 04 — titik awal orkestrasi.
// Skrip ini menyalakan 3 consumer, mengalirkan pesan, lalu berhenti.
// Lengkapi bagian LATIHAN untuk ketiga tugas lab:
//   1. tambah consumer keempat dan tunjukkan satu consumer idle;
//   2. ukur jeda rebalance classic vs consumer saat satu consumer SIGTERM;
//   3. reset offset ke earliest dan pastikan lag kembali 0.
// Versi lengkap: ../solution/assignments.js, rebalance.js, replay.js.
// Jalankan: node start/lab04.js

const { spawn } = require('node:child_process');
const path = require('node:path');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;

const WORKER = path.join(__dirname, 'worker.js');
const TOPIC = 'lab04.orders.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');
const GROUP = `lab04-start-${Date.now()}`;
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


function latihan(message) {
  throw new Error(`LATIHAN: ${message}`);
}

const spawnedChildren = [];

function spawnWorker(name, protocol = 'classic') {
  const child = spawn(process.execPath, [WORKER], {
    env: {
      ...process.env,
      LAB04_NAME: name,
      LAB04_GROUP: GROUP,
      LAB04_PROTOCOL: protocol,
      LAB04_FROM_BEGINNING: '0',
      LAB04_AUTO_COMMIT: '1',
      LAB04_TOPIC: TOPIC,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  spawnedChildren.push(child);
  const state = { name, child, messages: [], connected: false, stderr: '' };
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
        if (entry.event === 'connected') state.connected = true;
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

async function main() {
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab04-start', brokers: BROKERS, logLevel: KafkaJS.logLevel.NOTHING },
  });
  await ensureTopic(kafka);
  const producer = kafka.producer({ kafkaJS: { acks: 1 } });
  const workers = [spawnWorker('c1'), spawnWorker('c2'), spawnWorker('c3')];

  while (!workers.every((worker) => worker.connected)) await sleep(100);
  await producer.connect();

  let seq = 0;
  const interval = setInterval(() => {
    producer
      .send({
        topic: TOPIC,
        messages: [{ key: `key-${seq % 3}`, value: JSON.stringify({ seq }) }],
      })
      .catch(() => {});
    seq += 1;
  }, 100);

  await sleep(8000);
  clearInterval(interval);
  await producer.disconnect().catch(() => {});

  console.log('== Pesan yang diproses tiap consumer');
  for (const worker of workers) {
    console.log(`  ${worker.name}: ${worker.messages.length} pesan`);
  }

  // LATIHAN 1: spawn consumer keempat, tunggu 10 detik, lalu cetak selisih
  // jumlah pesan per consumer. Dengan 3 partisi, satu consumer harus idle.
  latihan('tambah consumer keempat dan tunjukkan satu consumer idle');

  // LATIHAN 2: ulangi dengan group.protocol=classic dan consumer. Kirim
  // SIGTERM ke satu consumer, catat timestamp pesan terakhir di partisinya dan
  // pesan pertama setelah rebalance, lalu bandingkan jedanya.

  // LATIHAN 3: hentikan consumer, reset offset group ke earliest dengan
  // /opt/kafka/bin/kafka-consumer-groups.sh, jalankan consumer lagi, dan
  // pastikan lag dari --describe kembali 0.

  for (const worker of workers) worker.child.kill('SIGTERM');
  await Promise.race([
    sleep(5000),
    new Promise((resolve) => {
      const timer = setInterval(() => {
        if (workers.every((worker) => worker.child.exitCode !== null)) {
          clearInterval(timer);
          resolve();
        }
      }, 100);
    }),
  ]);
  for (const worker of workers) {
    if (worker.child.exitCode === null) worker.child.kill('SIGKILL');
  }
  process.exit(0);
}

main().catch((error) => {
  console.error(`FAIL lab04 start: ${error.message}`);
  for (const child of spawnedChildren) {
    if (child.exitCode === null) child.kill('SIGKILL');
  }
  process.exit(1);
});
