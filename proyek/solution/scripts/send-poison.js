#!/usr/bin/env node
// A valid envelope whose data can never pass validation (negative amount).
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import pkg from '@confluentinc/kafka-javascript';

const require = createRequire(import.meta.url);
const { createEnvelope, headersFor } = require('../../../shared/envelope.js');

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'proyek-send-poison', brokers, logLevel: logLevel.NOTHING },
});

const orderId = process.argv[2] ?? randomUUID();
const envelope = createEnvelope({
  eventType: 'order.created.v1',
  data: {
    orderId,
    customerId: 'cust-poison',
    amount: -5000,
    currency: 'IDR',
  },
});

const producer = kafka.producer();
await producer.connect();
try {
  await producer.send({
    topic: 'order.created.v1',
    messages: [
      {
        key: orderId,
        value: JSON.stringify(envelope),
        headers: headersFor(envelope),
      },
    ],
  });
  console.log(JSON.stringify({ orderId, eventId: envelope.eventId }));
} finally {
  await producer.disconnect();
}
