'use strict';

// Lab 03 - use the envelope to publish an order.created.v1 event, then also send
// an intentionally broken event. The broker does not validate payloads; the
// consumer must reject it. Both go to the sandbox topic lab03.order.created.v1
// so the broken record never reaches order.created.v1, which later labs read
// from the beginning. Run: node solution/publish-order.js

const { randomUUID } = require('node:crypto');
const { KafkaJS } = require('@confluentinc/kafka-javascript');
const { Kafka } = KafkaJS;
const {
  createEnvelope,
  validateEnvelope,
  headersFor,
} = require('../../../shared/envelope.js');

const TOPIC = 'lab03.order.created.v1';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094').split(',');

async function ensureTopic(admin) {
  const topics = await admin.listTopics();
  if (topics.includes(TOPIC)) return;
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

async function main() {
  const kafka = new Kafka({
    kafkaJS: { clientId: 'lab03-publish', brokers: BROKERS, logLevel: KafkaJS.logLevel.ERROR },
  });

  const admin = kafka.admin();
  await admin.connect();
  await ensureTopic(admin);
  await admin.disconnect();

  const orderId = `order-${randomUUID()}`;
  const correlationId = `checkout-${randomUUID()}`;
  const event = createEnvelope({
    eventType: 'order.created.v1',
    version: 1,
    correlationId,
    data: {
      orderId,
      customerId: 'customer-1',
      amount: 250000,
      currency: 'IDR',
    },
  });

  // Deliberately bypass the helper: correlationId is omitted. The broker will
  // accept this record; the consumer must reject it.
  const broken = {
    eventId: randomUUID(),
    eventType: event.eventType,
    version: event.version,
    occurredAt: new Date().toISOString(),
    data: { orderId, customerId: 'customer-1', amount: 250000, currency: 'IDR' },
  };

  const producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await producer.connect();
  await producer.send({
    topic: TOPIC,
    messages: [
      { key: orderId, value: JSON.stringify(event), headers: headersFor(event) },
      { key: orderId, value: JSON.stringify(broken), headers: { 'event-type': event.eventType } },
    ],
  });
  await producer.disconnect();
  console.log(`PASS  produce 2 record ke ${TOPIC} (1 valid, 1 tanpa correlationId)`);

  const consumer = kafka.consumer({
    kafkaJS: { groupId: `lab03-publish-${randomUUID()}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });

  let validSeen = false;
  let brokenRejected = false;

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timeout menunggu event kembali')), 45000);

    consumer
      .run({
        eachMessage: async ({ message }) => {
          let payload;
          try {
            payload = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (payload?.data?.orderId !== orderId) return;

          if (payload.eventId === event.eventId) {
            try {
              validateEnvelope(payload);
              const header = message.headers?.['correlation-id'];
              if (header?.toString() !== correlationId) {
                throw new Error('header correlation-id tidak cocok dengan envelope');
              }
            } catch (error) {
              clearTimeout(timer);
              reject(error);
              return;
            }
            validSeen = true;
            console.log('PASS  event valid lolos validateEnvelope dan header cocok');
          } else {
            try {
              validateEnvelope(payload);
              clearTimeout(timer);
              reject(new Error('event tanpa correlationId diterima, seharusnya ditolak'));
              return;
            } catch (error) {
              brokenRejected = true;
              console.log(`PASS  event tanpa correlationId ditolak: ${error.message}`);
            }
          }

          if (validSeen && brokenRejected) {
            clearTimeout(timer);
            resolve();
          }
        },
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });

  await consumer.disconnect();
  console.log('PASS lab03 publish: envelope dipakai, event rusak ditolak consumer');
}

main().catch((error) => {
  console.error(`FAIL lab03 publish: ${error.message}`);
  process.exit(1);
});
