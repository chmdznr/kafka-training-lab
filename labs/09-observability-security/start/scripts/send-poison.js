#!/usr/bin/env node
// A valid envelope that always fails business validation (amount above the
// payment limit), so it travels retry -> retry -> DLT.
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import pkg from '@confluentinc/kafka-javascript';

const require = createRequire(import.meta.url);
const { createEnvelope, headersFor } = require('../../../../shared/envelope.js');

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'lab09-send-poison', brokers, logLevel: logLevel.NOTHING },
});

const orderId = process.argv[2] ?? randomUUID();
const amount = Number(process.env.POISON_AMOUNT ?? 5_000_000);

const envelope = createEnvelope({
  eventType: 'order.created.v1',
  data: {
    orderId,
    customerId: 'cust-poison',
    amount,
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
  console.log(
    JSON.stringify({
      orderId,
      eventId: envelope.eventId,
      amount,
      note: 'amount di atas batas payment_limits; tunggu retry lalu cek DLT',
    }),
  );
} finally {
  await producer.disconnect();
}
