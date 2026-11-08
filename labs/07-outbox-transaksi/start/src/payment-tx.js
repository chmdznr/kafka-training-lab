'use strict';

const crypto = require('node:crypto');
const { createKafka } = require('./kafka');
const { TOPICS } = require('./config');
const { createEnvelope, headersFor, validateEnvelope } = require('../../../../shared/envelope.js');

const GROUP_ID = process.env.GROUP_ID || 'payment-tx';
const TRANSACTIONAL_ID = process.env.TRANSACTIONAL_ID || 'payment-tx-1';

const kafka = createKafka('payment-tx');

const producer = kafka.producer({
  kafkaJS: {
    // LATIHAN 3a: pakai transactionalId supaya producer transaksional.
    // Tambahkan juga idempotent, maxInFlightRequests, dan acks=-1.
  },
});

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    // LATIHAN 3b: matikan auto commit (autoCommit: false) dan pastikan consumer
    // membaca read_committed (readUncommitted: false).
    fromBeginning: true,
  },
});

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

async function processOrder({ topic, partition, message }) {
  const orderEvent = parseOrderEvent(message);
  if (!orderEvent) {
    // LATIHAN 3d: record ini bukan order.created.v1 valid. Lewati
    // pemrosesannya, tetapi offset-nya harus ikut di-commit dalam transaksi
    // (sendOffsets) supaya LAG grup tidak tertahan di atas 0.
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

  // LATIHAN 3c: bungkus langkah berikut dalam satu transaksi Kafka:
  //   await producer.transaction();
  //   ... producer.send({ topic: TOPICS.paymentCompleted, messages: [{
  //         key: orderEvent.data.orderId,
  //         value: JSON.stringify(paymentEvent),
  //         headers: headersFor(paymentEvent) }] }) ...
  //   ... producer.sendOffsets({ consumer, topics: [{ topic, partitions:
  //         [{ partition, offset: String(Number(message.offset) + 1) }] }] }) ...
  //   ... producer.commit() ...
  // dan panggil producer.abort() kalau ada error.
  //
  // Ingat: API KafkaJS-compatible di @confluentinc/kafka-javascript menerima
  // objek `consumer` pada sendOffsets, bukan consumerGroupId.
  console.log(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'LATIHAN: proses order di sini',
    orderId: orderEvent.data.orderId,
    paymentId,
    paymentEvent: paymentEvent.eventType,
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
