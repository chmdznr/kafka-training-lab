#!/usr/bin/env node
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import pkg from '@confluentinc/kafka-javascript';

const require = createRequire(import.meta.url);
const { createEnvelope, headersFor } = require('../../../shared/envelope.js');

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'proyek-send-duplicate', brokers, logLevel: logLevel.NOTHING },
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

// Fresh record per send: the client mutates headers in place.
const message = () => ({
  key: orderId,
  value: JSON.stringify(envelope),
  headers: headersFor(envelope),
});

const producer = kafka.producer();
await producer.connect();
try {
  await producer.send({ topic: 'order.created.v1', messages: [message()] });
  await producer.send({ topic: 'order.created.v1', messages: [message()] });
  console.log(JSON.stringify({ orderId, eventId: envelope.eventId, sent: 2 }));
} finally {
  await producer.disconnect();
}
