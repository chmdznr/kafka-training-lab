'use strict';

const crypto = require('node:crypto');
const { createKafka } = require('./kafka');
const { TOPICS } = require('./config');
const { createEnvelope, headersFor, validateEnvelope } = require('../../../../shared/envelope.js');

const GROUP_ID = process.env.GROUP_ID || 'payment-tx';
const TRANSACTIONAL_ID = process.env.TRANSACTIONAL_ID || 'payment-tx-1';
const SKIP_FLUSH_MS = Number(process.env.SKIP_FLUSH_MS || 1000);

const kafka = createKafka('payment-tx');

const producer = kafka.producer({
  kafkaJS: {
    transactionalId: TRANSACTIONAL_ID,
    idempotent: true,
    maxInFlightRequests: 5,
    acks: -1,
  },
});

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    autoCommit: false,
    fromBeginning: true,
    // read_committed: records from aborted transactions are never delivered.
    readUncommitted: false,
  },
});

// A transactional producer can only run one transaction at a time. The skip
// flusher and eachMessage both start transactions, so serialize them.
let transactionChain = Promise.resolve();

function runTransaction(fn) {
  const run = transactionChain.then(fn, fn);
  transactionChain = run.catch(() => {});
  return run;
}

// Offsets of records we skipped, per partition: value is the next offset to
// commit. They are committed in a Kafka transaction too, so a poison record
// does not leave the group's LAG stuck above zero.
const pendingSkips = new Map();
let skippedTotal = 0;
let flushTimer = null;
let flushInFlight = false;

function addSkippedOffset(partition, offset) {
  const next = Number(offset) + 1;
  pendingSkips.set(partition, Math.max(pendingSkips.get(partition) ?? 0, next));
}

// Snapshot pending offsets, optionally including the current valid message.
function offsetsToCommit({ partition, offset } = {}) {
  const byPartition = new Map(pendingSkips);
  if (partition !== undefined) {
    const next = Number(offset) + 1;
    byPartition.set(partition, Math.max(next, byPartition.get(partition) ?? 0));
  }
  return [...byPartition.entries()]
    .sort(([a], [b]) => a - b)
    .map(([part, nextOffset]) => ({ partition: part, offset: String(nextOffset) }));
}

// Remove only the offsets covered by the transaction that just committed;
// messages processed while it was in flight may have advanced the map.
function releaseCommitted(partitions) {
  for (const { partition, offset } of partitions) {
    const committed = Number(offset);
    if ((pendingSkips.get(partition) ?? 0) <= committed) {
      pendingSkips.delete(partition);
    }
  }
}

async function commitOffsetsOnly() {
  if (pendingSkips.size === 0) return;
  const partitions = offsetsToCommit();

  await runTransaction(async () => {
    await producer.transaction();
    try {
      await producer.sendOffsets({
        consumer,
        topics: [{ topic: TOPICS.orderCreated, partitions }],
      });
      await producer.commit();
    } catch (err) {
      await producer.abort();
      throw err;
    }
  });

  releaseCommitted(partitions);
  console.log(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'offset record tidak valid di-commit agar lag kembali 0',
    partitions: partitions.length,
  }));
}

function scheduleSkipFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushSkipped().catch((err) => {
      console.error(JSON.stringify({
        at: new Date().toISOString(),
        msg: 'gagal commit offset record tidak valid, akan dicoba lagi',
        error: err.message,
      }));
      scheduleSkipFlush();
    });
  }, SKIP_FLUSH_MS);
}

async function flushSkipped() {
  if (flushInFlight) {
    scheduleSkipFlush();
    return;
  }
  flushInFlight = true;
  try {
    await commitOffsetsOnly();
  } finally {
    flushInFlight = false;
  }
}

// Other labs and smoke tests write to the same topic. Only contract-valid
// order.created.v1 events are processed here.
function parseOrderEvent(message) {
  let event;
  try {
    event = JSON.parse(message.value.toString('utf8'));
  } catch (err) {
    return null;
  }
  try {
    validateEnvelope(event);
  } catch (err) {
    return null;
  }
  if (event.eventType !== TOPICS.orderCreated || typeof event.data?.orderId !== 'string') {
    return null;
  }
  return event;
}

// Read one order event, write one payment event, and commit the consumer
// offset in the SAME Kafka transaction. Either the payment event and the
// offset both become visible, or neither does.
async function processOrder({ topic, partition, message }) {
  const orderEvent = parseOrderEvent(message);
  if (!orderEvent) {
    skippedTotal += 1;
    addSkippedOffset(partition, message.offset);
    if (skippedTotal <= 5 || skippedTotal % 1000 === 0) {
      console.warn(JSON.stringify({
        at: new Date().toISOString(),
        msg: 'lewati record yang bukan order.created.v1 valid, offset akan ikut di-commit',
        partition,
        offset: message.offset,
        totalDilewati: skippedTotal,
      }));
    }
    scheduleSkipFlush();
    return;
  }

  const paymentId = crypto.randomUUID();
  const paymentEvent = createEnvelope({
    eventType: TOPICS.paymentCompleted,
    correlationId: orderEvent.correlationId,
    causationId: orderEvent.eventId,
    data: {
      paymentId,
      orderId: orderEvent.data.orderId,
      amount: orderEvent.data.amount,
      status: 'completed',
    },
  });
  const offsets = offsetsToCommit({ partition, offset: message.offset });

  await runTransaction(async () => {
    await producer.transaction();
    try {
      await producer.send({
        topic: TOPICS.paymentCompleted,
        messages: [{
          key: orderEvent.data.orderId,
          value: JSON.stringify(paymentEvent),
          headers: headersFor(paymentEvent),
        }],
      });

      await producer.sendOffsets({
        consumer,
        topics: [{ topic, partitions: offsets }],
      });

      await producer.commit();
    } catch (err) {
      await producer.abort();
      throw err;
    }
  });

  releaseCommitted(offsets);
  console.log(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'order diproses, payment.completed.v1 dan offset ter-commit bersama',
    orderId: orderEvent.data.orderId,
    paymentId,
    partition,
    offset: message.offset,
  }));
}

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.orderCreated });

  console.log(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'payment-tx menunggu order.created.v1',
    groupId: GROUP_ID,
    transactionalId: TRANSACTIONAL_ID,
  }));

  await consumer.run({ eachMessage: processOrder });
}

async function shutdown(signal) {
  console.log(JSON.stringify({ at: new Date().toISOString(), msg: 'shutdown', signal }));
  await flushSkipped().catch(() => {});
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
