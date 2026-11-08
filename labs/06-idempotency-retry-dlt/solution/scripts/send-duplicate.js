#!/usr/bin/env node
// Sends the same event twice, as an at-least-once producer would after a retry.
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import pkg from '@confluentinc/kafka-javascript';

const require = createRequire(import.meta.url);
const { createEnvelope, headersFor } = require('../../../../shared/envelope.js');

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'lab06-send-duplicate', brokers, logLevel: logLevel.NOTHING },
});

const orderId = process.argv[2] ?? randomUUID();
const envelope = createEnvelope({
  eventType: 'order.created.v1',
  data: {
    orderId,
    customerId: 'cust-duplicate',
    amount: 75000,
    currency: 'IDR',
  },
});

// A fresh record per send: the client mutates the message object (it encodes
// headers in place), so reusing one object breaks the second send.
const message = () => ({
  key: orderId,
  value: JSON.stringify(envelope),
  headers: headersFor(envelope),
});

const producer = kafka.producer();
await producer.connect();
try {
  // Two separate sends of the same eventId, like a producer retry.
  await producer.send({ topic: 'order.created.v1', messages: [message()] });
  await producer.send({ topic: 'order.created.v1', messages: [message()] });
  console.log(
    JSON.stringify({
      sent: 2,
      orderId,
      eventId: envelope.eventId,
      note: 'eventId sama dikirim dua kali; payments harus tetap satu baris',
    }),
  );
} finally {
  await producer.disconnect();
}
