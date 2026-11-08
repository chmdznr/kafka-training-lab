import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { KafkaContainer } from '@testcontainers/kafka';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices';
import pkg from '@confluentinc/kafka-javascript';
import pg from 'pg';

const { Kafka, logLevel } = pkg.KafkaJS;
const { Pool } = pg;

const ORDER_TOPIC = 'order.created.v1';
const PAYMENT_COMPLETED = 'payment.completed.v1';
const PAYMENT_FAILED = 'payment.failed.v1';
const RETRY_TOPIC = 'order.created.v1.payment-service.retry';
const DLT_TOPIC = 'order.created.v1.payment-service.dlt';

const ORDER_GROUP = 'order-service';
const PAYMENT_GROUP = 'payment-service';
const RETRY_GROUP = 'payment-service-retry';
const NOTIFICATION_GROUP = 'notification-service';

const TOPICS = [ORDER_TOPIC, PAYMENT_COMPLETED, PAYMENT_FAILED, RETRY_TOPIC, DLT_TOPIC];

async function waitFor(check, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      last = await check();
      if (last) {
        return last;
      }
    } catch (error) {
      last = error;
    }
    await sleep(200);
  }
  throw new Error(`timeout menunggu ${label}; hasil terakhir: ${last}`);
}

async function createTopics(kafka) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: TOPICS.map((topic) => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [{ name: 'min.insync.replicas', value: '1' }],
      })),
    });
  } finally {
    await admin.disconnect();
  }
}

function microserviceOptions(groupId, brokers) {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: { clientId: `${groupId}-test`, brokers: [brokers] },
      consumer: { groupId, allowAutoTopicCreation: false },
      run: { autoCommit: false, partitionsConsumedConcurrently: 1 },
      subscribe: { fromBeginning: true },
    },
  };
}

async function bootHybrid(moduleRef, groupId, brokers) {
  const app = await NestFactory.create(moduleRef, { logger: false });
  app.connectMicroservice(microserviceOptions(groupId, brokers));
  // NestFactory.create does not run lifecycle hooks; app.init() creates the
  // tables in the service onModuleInit handlers.
  await app.init();
  await app.startAllMicroservices();
  return app;
}

test(
  'order -> payment -> notification dengan idempotency dan DLT',
  { timeout: 240_000 },
  async () => {
    const kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:8.4.0').start();
    const postgres = await new PostgreSqlContainer('postgres:17')
      .withDatabase('shop')
      .withUsername('app')
      .withPassword('app')
      .start();

    const brokers = `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`;
    process.env.KAFKA_BROKERS = brokers;
    process.env.DATABASE_URL = postgres.getConnectionUri();
    process.env.PROCESSING_DELAY_MS = '0';

    const kafka = new Kafka({
      kafkaJS: {
        clientId: 'proyek-test',
        brokers: [brokers],
        logLevel: logLevel.NOTHING,
      },
    });
    const apps = [];
    const consumers = [];
    let pool;

    try {
      await createTopics(kafka);

      // Imported after the environment points at the containers; the modules
      // read KAFKA_BROKERS when they load.
      const { AppModule: OrderModule } = await import('../dist/order/app.module.js');
      const { AppModule: PaymentModule } = await import('../dist/payment/app.module.js');
      const { RetryModule } = await import('../dist/payment/app.module.js');
      const { AppModule: NotificationModule } = await import('../dist/notification/app.module.js');

      const paymentApp = await bootHybrid(PaymentModule, PAYMENT_GROUP, brokers);
      apps.push(paymentApp);

      const retryApp = await NestFactory.createMicroservice(
        RetryModule,
        microserviceOptions(RETRY_GROUP, brokers),
      );
      await retryApp.listen();
      apps.push(retryApp);

      const notificationApp = await bootHybrid(
        NotificationModule,
        NOTIFICATION_GROUP,
        brokers,
      );
      apps.push(notificationApp);

      const orderApp = await bootHybrid(OrderModule, ORDER_GROUP, brokers);
      apps.push(orderApp);
      await orderApp.listen(0);
      const port = orderApp.getHttpServer().address().port;
      const base = `http://127.0.0.1:${port}`;

      pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 3 });

      // --- 1. happy path ---------------------------------------------------
      const createResponse = await fetch(`${base}/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          customerId: 'cust-test',
          amount: 125000,
          currency: 'IDR',
        }),
      });
      assert.equal(createResponse.status, 202);
      const created = await createResponse.json();
      assert.ok(created.orderId);
      assert.ok(created.eventId);

      const order = await waitFor(
        async () => {
          const response = await fetch(`${base}/orders/${created.orderId}`);
          if (response.status !== 200) {
            return false;
          }
          const body = await response.json();
          return body.status === 'PAID' ? body : false;
        },
        30_000,
        'order PAID',
      );
      assert.equal(order.status, 'PAID');

      const payments = await pool.query(
        'SELECT event_id, status FROM proyek_payments WHERE order_id = $1',
        [created.orderId],
      );
      assert.equal(payments.rowCount, 1);
      assert.equal(payments.rows[0].status, 'paid');

      const notifications = await pool.query(
        'SELECT message FROM proyek_notifications WHERE order_id = $1',
        [created.orderId],
      );
      assert.equal(notifications.rowCount, 1);
      assert.match(notifications.rows[0].message, /berhasil/);

      // --- 2. duplicate delivery ------------------------------------------
      const outbox = await pool.query(
        `SELECT topic, key, payload, headers FROM proyek_order_outbox
          WHERE key = $1 ORDER BY id LIMIT 1`,
        [created.orderId],
      );
      assert.equal(outbox.rowCount, 1);
      // The client mutates the message's headers in place, so each send needs
      // a fresh record.
      const record = () => ({
        key: outbox.rows[0].key,
        value: JSON.stringify(outbox.rows[0].payload),
        headers: { ...outbox.rows[0].headers },
      });

      const producer = kafka.producer();
      await producer.connect();
      try {
        await producer.send({ topic: ORDER_TOPIC, messages: [record()] });
        await producer.send({ topic: ORDER_TOPIC, messages: [record()] });
      } finally {
        await producer.disconnect();
      }

      await sleep(3000);
      const duplicatePayments = await pool.query(
        'SELECT count(*)::int AS count FROM proyek_payments WHERE order_id = $1',
        [created.orderId],
      );
      assert.equal(duplicatePayments.rows[0].count, 1, 'duplikat tidak menambah payment');
      const duplicateNotifications = await pool.query(
        'SELECT count(*)::int AS count FROM proyek_notifications WHERE order_id = $1',
        [created.orderId],
      );
      assert.equal(
        duplicateNotifications.rows[0].count,
        1,
        'duplikat tidak menambah notification',
      );
      const claimed = await pool.query(
        'SELECT count(*)::int AS count FROM proyek_processed_events WHERE event_id = $1',
        [created.eventId],
      );
      assert.equal(claimed.rows[0].count, 1);

      // --- 3. poison message ends in the DLT ------------------------------
      const dltSeen = new Set();
      const spy = kafka.consumer({
        kafkaJS: { groupId: `test-dlt-${Date.now()}` },
      });
      await spy.connect();
      await spy.subscribe({
        topics: [DLT_TOPIC],
        kafkaJS: { fromBeginning: true },
      });
      await spy.run({
        eachMessage: async ({ message }) => {
          try {
            dltSeen.add(JSON.parse(message.value.toString()).eventId);
          } catch {
            // not JSON
          }
        },
      });
      consumers.push(spy);

      const poisonOrderId = randomUUID();
      const poison = {
        eventId: randomUUID(),
        eventType: ORDER_TOPIC,
        version: 1,
        occurredAt: new Date().toISOString(),
        correlationId: randomUUID(),
        data: {
          orderId: poisonOrderId,
          customerId: 'cust-poison',
          amount: -1,
          currency: 'IDR',
        },
      };
      assert.ok(poison);

      const poisonProducer = kafka.producer();
      await poisonProducer.connect();
      try {
        await poisonProducer.send({
          topic: ORDER_TOPIC,
          messages: [
            {
              key: poisonOrderId,
              value: JSON.stringify(poison),
              headers: {
                'correlation-id': poison.correlationId,
                'event-type': ORDER_TOPIC,
              },
            },
          ],
        });
      } finally {
        await poisonProducer.disconnect();
      }

      await waitFor(
        async () => dltSeen.has(poison.eventId),
        30_000,
        'poison event masuk DLT',
      );

      const poisonPayments = await pool.query(
        'SELECT count(*)::int AS count FROM proyek_payments WHERE order_id = $1',
        [poisonOrderId],
      );
      assert.equal(poisonPayments.rows[0].count, 0);
    } finally {
      for (const consumer of consumers) {
        await consumer.disconnect().catch(() => undefined);
      }
      await pool?.end().catch(() => undefined);
      for (const app of apps) {
        await app.close().catch(() => undefined);
      }
      await kafkaContainer.stop().catch(() => undefined);
      await postgres.stop().catch(() => undefined);
    }
  },
);
