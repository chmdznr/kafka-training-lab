'use strict';

// Lab 04 part 2: measure the rebalance pause when one consumer leaves, for
// group.protocol=classic and group.protocol=consumer (KIP-848).
// Run: node solution/rebalance.js

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

function spawnWorker(name, group, protocol) {
  const child = spawn(process.execPath, [WORKER], {
    env: {
      ...process.env,
      LAB04_NAME: name,
      LAB04_GROUP: group,
      LAB04_PROTOCOL: protocol,
      LAB04_FROM_BEGINNING: '0',
      LAB04_AUTO_COMMIT: '1',
      LAB04_TOPIC: TOPIC,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const state = { name, child, messages: [], entries: [], partitions: new Set(), stderr: '' };
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
        if (entry.event === 'msg') {
          state.messages.push(entry);
          state.partitions.add(entry.partition);
        }
        if (entry.event === 'connected') state.entries.push(entry);
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

async function runScenario(protocol) {
  const group = `lab04-rebalance-${protocol}-${Date.now()}`;
  const kafka = new Kafka({
    kafkaJS: {
      clientId: `lab04-rebalance-${protocol}`,
      brokers: BROKERS,
      logLevel: KafkaJS.logLevel.NOTHING,
    },
  });
  await ensureTopic(kafka);
  const producer = kafka.producer({ kafkaJS: { acks: 1 } });
  const workers = [spawnWorker('c1', group, protocol), spawnWorker('c2', group, protocol), spawnWorker('c3', group, protocol)];

  try {
    await waitFor(
      () => workers.every((worker) => worker.entries.length > 0),
      45000,
      `${protocol}: tiga consumer terhubung`,
    );
    console.log(`\n== group.protocol=${protocol}`);

    await producer.connect();
    let seq = 0;
    let sending = true;
    // Kirim ke ketiga partisi secara eksplisit supaya setiap partisi punya
    // aliran pesan yang rapat; jeda alami antar-pesan tidak menutupi jeda
    // rebalance.
    const interval = setInterval(() => {
      if (!sending) return;
      producer
        .send({
          topic: TOPIC,
          messages: [0, 1, 2].map((partition) => ({
            partition,
            key: `key-${partition}`,
            value: JSON.stringify({ group, seq, partition }),
          })),
        })
        .catch(() => {});
      seq += 1;
    }, 10);

    await waitFor(
      () => workers.every((worker) => worker.messages.length > 0),
      45000,
      `${protocol}: semua consumer sudah menerima pesan`,
    );
    for (const worker of workers) {
      console.log(`  ${worker.name}: partisi=${JSON.stringify([...worker.partitions].sort())}`);
    }
    await sleep(2000);
    const killAt = Date.now();
    console.log(`  SIGTERM ke c1 pada ${killAt}`);
    workers[0].child.kill('SIGTERM');

    await sleep(14000);
    sending = false;
    clearInterval(interval);
    await sleep(300);
    await producer.disconnect();

    const survivors = workers.slice(1);
    const movedPartition = workers[0].messages.at(-1)?.partition;
    if (movedPartition === undefined) {
      throw new Error(`${protocol}: consumer yang dihentikan tidak memproses partisi`);
    }

    const allBefore = workers.flatMap((worker) => worker.messages).filter((message) => message.ts <= killAt);
    const movedBefore = allBefore.filter((message) => message.partition === movedPartition);
    const movedAfter = survivors
      .flatMap((worker) => worker.messages)
      .filter((message) => message.ts > killAt && message.partition === movedPartition);

    if (movedBefore.length === 0 || movedAfter.length === 0) {
      throw new Error(`${protocol}: tidak cukup pesan di partisi ${movedPartition} di sekitar rebalance`);
    }

    const lastBefore = Math.max(...movedBefore.map((message) => message.ts));
    const firstAfter = Math.min(...movedAfter.map((message) => message.ts));
    const pauseMs = firstAfter - lastBefore;
    const resumedAfterKill = firstAfter - killAt;

    // Partisi yang tidak berpindah: consumer yang tidak terdampak. Jeda
    // diukur sebagai gap terbesar antar-pesan di jendela sekitar SIGTERM,
    // bukan hanya last-before/first-after, karena satu pesan bisa masih
    // sempat diproses sesaat setelah SIGTERM sebelum rebalance benar-benar
    // dimulai.
    const windowStart = killAt - 3000;
    const windowEnd = killAt + 12000;
    const maxGap = (partition) => {
      const timestamps = workers
        .flatMap((worker) => worker.messages)
        .filter((message) => message.partition === partition)
        .map((message) => message.ts)
        .filter((ts) => ts >= windowStart && ts <= windowEnd)
        .sort((a, b) => a - b);
      let best = null;
      for (let index = 1; index < timestamps.length; index += 1) {
        const gap = timestamps[index] - timestamps[index - 1];
        if (best === null || gap > best.gapMs) {
          best = { gapMs: gap, from: timestamps[index - 1], to: timestamps[index] };
        }
      }
      return best;
    };

    const allMessages = workers.flatMap((worker) => worker.messages);

    // Consumer yang menerima partisi pindahan. Consumer inilah yang sibuk
    // mengejar backlog; consumer lain adalah yang "tidak terdampak".
    const adopterCounts = new Map();
    for (const message of movedAfter) {
      const owner = survivors.find((worker) => worker.messages.includes(message))?.name;
      if (owner) adopterCounts.set(owner, (adopterCounts.get(owner) ?? 0) + 1);
    }
    const adopter = [...adopterCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
    const unaffectedWorkers = survivors.filter((worker) => worker.name !== adopter);

    // Partisi yang tetap dilayani consumer tidak terdampak. Gap dihitung dari
    // seluruh pesan partisi itu di jendela sekitar SIGTERM.
    const unaffected = [];
    for (const worker of unaffectedWorkers) {
      const partitions = new Set(
        worker.messages
          .filter((message) => message.ts > killAt && message.partition !== movedPartition)
          .map((message) => message.partition),
      );
      for (const partition of [...partitions].sort()) {
        const gap = maxGap(partition);
        if (gap) unaffected.push({ partition, worker: worker.name, ...gap });
      }
    }
    if (unaffected.length === 0) {
      throw new Error(`${protocol}: tidak ada partisi consumer tidak terdampak yang bisa diukur`);
    }
    const unaffectedPauseMs = Math.max(...unaffected.map((entry) => entry.gapMs));

    console.log(`  partisi yang berpindah: ${movedPartition} (penerima: ${adopter})`);
    console.log(`  pesan di partisi itu sebelum=${movedBefore.length}, sesudah=${movedAfter.length}`);
    console.log(`  last message before=${lastBefore} first after=${firstAfter}`);
    console.log(`  pause partisi berpindah: ${pauseMs} ms (${resumedAfterKill} ms setelah SIGTERM)`);
    for (const entry of unaffected) {
      console.log(
        `  ${entry.worker} partisi ${entry.partition} (tidak terdampak): gap maksimum=${entry.gapMs} ms (${entry.from} -> ${entry.to})`,
      );
    }
    console.log(`  gap maksimum consumer tidak terdampak: ${unaffectedPauseMs} ms`);
    for (const worker of workers) {
      console.log(`  ${worker.name}: partisi=${JSON.stringify([...worker.partitions].sort())}`);
    }

    return { protocol, pauseMs, unaffectedPauseMs, unaffected, adopter };
  } finally {
    await stopAll(workers);
    await producer.disconnect().catch(() => {});
  }
}

async function main() {
  const results = [];
  results.push(await runScenario('classic'));
  results.push(await runScenario('consumer'));

  console.log('\n== Ringkasan jeda rebalance');
  console.log('  protokol  partisi berpindah  gap maks consumer tidak terdampak');
  for (const result of results) {
    console.log(
      `  ${result.protocol.padEnd(8)} ${String(result.pauseMs).padStart(6)} ms          ${String(result.unaffectedPauseMs).padStart(6)} ms`,
    );
  }
  if (results.some((result) => !Number.isFinite(result.pauseMs) || result.pauseMs <= 0)) {
    throw new Error('jeda rebalance tidak terukur');
  }
  if (results.some((result) => !Number.isFinite(result.unaffectedPauseMs))) {
    throw new Error('jeda partisi tidak berpindah tidak terukur');
  }
  console.log(
    `PASS lab04 rebalance: classic moved=${results[0].pauseMs} ms/unaffected=${results[0].unaffectedPauseMs} ms, consumer moved=${results[1].pauseMs} ms/unaffected=${results[1].unaffectedPauseMs} ms`,
  );
}

main().catch((error) => {
  console.error(`FAIL lab04 rebalance: ${error.message}`);
  process.exit(1);
});
