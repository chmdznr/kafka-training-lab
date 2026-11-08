'use strict';

const { pool } = require('../src/db');
const { createKafka } = require('../src/kafka');
const { TOPICS, CORRELATION_PREFIX } = require('../src/config');
const { validateEnvelope } = require('../../../../shared/envelope.js');

const IDLE_MS = Number(process.env.CHECK_IDLE_MS || 8000);

function envelopeProblem(envelope) {
  try {
    const result = validateEnvelope(envelope);
    if (result === false) return 'validateEnvelope mengembalikan false';
    if (result && typeof result === 'object' && result.ok === false) {
      return JSON.stringify(result.errors ?? result);
    }
    return null;
  } catch (err) {
    return err.message;
  }
}

// Read the whole topic once, then wait until no message has arrived for
// IDLE_MS before reporting.
async function readOrderEvents() {
  const groupId = `lab07-check-${Date.now()}`;
  const consumer = createKafka('lab07-check').consumer({
    kafkaJS: {
      groupId,
      fromBeginning: true,
      readUncommitted: false,
    },
  });

  const events = [];
  let ignored = 0;
  let idleTimer;
  let resolveIdle;
  const idle = new Promise((resolve) => { resolveIdle = resolve; });
  const armIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(resolveIdle, IDLE_MS);
  };

  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.orderCreated });
  armIdle();

  const running = consumer.run({
    eachMessage: async ({ partition, message }) => {
      const raw = message.value ? message.value.toString('utf8') : '';
      let envelope = null;
      let problem = null;
      try {
        envelope = JSON.parse(raw);
      } catch (err) {
        problem = 'value bukan JSON';
      }
      // Other labs and smoke tests share this topic. Only events tagged by
      // this lab's order-service count towards the outbox check.
      const owned = envelope
        && typeof envelope.correlationId === 'string'
        && envelope.correlationId.startsWith(CORRELATION_PREFIX);
      if (!owned) {
        ignored += 1;
        return;
      }
      if (envelope) {
        problem = envelopeProblem(envelope);
      }
      events.push({
        key: message.key ? message.key.toString('utf8') : null,
        partition,
        offset: message.offset,
        envelope,
        problem,
      });
      armIdle();
    },
  });

  await idle;
  await consumer.stop().catch(() => {});
  await running.catch(() => {});
  await consumer.disconnect().catch(() => {});
  return { events, ignored };
}

async function main() {
  const { events, ignored } = await readOrderEvents();

  const { rows: orders } = await pool.query('SELECT id, created_at FROM orders ORDER BY created_at');
  const { rows: pendingRows } = await pool.query(
    'SELECT count(*)::int AS count FROM outbox WHERE published_at IS NULL',
  );
  const pending = pendingRows[0].count;

  const orderIds = new Set(orders.map((row) => row.id));
  const eventKeys = new Set();
  const eventIds = new Map();
  const invalid = [];
  const keyMismatch = [];

  for (const event of events) {
    if (event.problem) {
      invalid.push(event);
      continue;
    }
    if (!event.key) {
      invalid.push({ ...event, problem: 'key kosong, seharusnya orderId' });
      continue;
    }
    const dataOrderId = event.envelope?.data?.orderId;
    if (dataOrderId !== event.key) {
      keyMismatch.push({ key: event.key, dataOrderId });
    }
    eventKeys.add(event.key);
    const eventId = event.envelope.eventId;
    eventIds.set(eventId, (eventIds.get(eventId) ?? 0) + 1);
  }

  const ordersWithoutEvents = [...orderIds].filter((id) => !eventKeys.has(id));
  const eventsWithoutOrders = [...eventKeys].filter((key) => !orderIds.has(key));
  const duplicates = [...eventIds.entries()].filter(([, count]) => count > 1);

  console.log('');
  console.log(`order di database      : ${orderIds.size}`);
  console.log(`event lab ini          : ${events.length} (key unik: ${eventKeys.size})`);
  console.log(`event lain diabaikan   : ${ignored}`);
  console.log(`outbox belum terkirim  : ${pending}`);
  console.log(`event tidak valid      : ${invalid.length}`);
  console.log(`key != data.orderId    : ${keyMismatch.length}`);
  console.log(`eventId duplikat       : ${duplicates.length}`);
  if (ordersWithoutEvents.length > 0) {
    console.log(`order tanpa event      : ${ordersWithoutEvents.join(', ')}`);
  }
  if (eventsWithoutOrders.length > 0) {
    console.log(`event tanpa order      : ${eventsWithoutOrders.join(', ')}`);
  }
  if (invalid.length > 0) {
    console.log('detail event tidak valid:');
    for (const event of invalid) {
      console.log(`  partition ${event.partition} offset ${event.offset}: ${event.problem}`);
    }
  }

  await pool.end();

  const ok = ordersWithoutEvents.length === 0
    && eventsWithoutOrders.length === 0
    && pending === 0
    && invalid.length === 0;

  console.log('');
  console.log(ok ? 'HASIL: OK - tidak ada order tanpa event, tidak ada event tanpa order' : 'HASIL: GAGAL');
  process.exit(ok ? 0 : 1);
}

main().catch(async (err) => {
  console.error(err);
  await pool.end().catch(() => {});
  process.exit(1);
});
