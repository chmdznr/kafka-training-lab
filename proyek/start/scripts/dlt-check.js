#!/usr/bin/env node
// Waits until the given eventId appears in the payment DLT.
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const target = process.argv[2];

if (!target) {
  console.error('pakai: node scripts/dlt-check.js <eventId>');
  process.exit(2);
}

const kafka = new Kafka({
  kafkaJS: { clientId: 'proyek-dlt-check', brokers, logLevel: logLevel.NOTHING },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: `proyek-dlt-check-${Date.now()}` },
});

const found = Promise.withResolvers();
const timer = setTimeout(() => found.resolve(false), 20_000);

await consumer.connect();
await consumer.subscribe({
  topics: ['order.created.v1.payment-service.dlt'],
  kafkaJS: { fromBeginning: true },
});

const running = consumer.run({
  eachMessage: async ({ message }) => {
    const value = message.value?.toString() ?? '';
    if (value.includes(target)) {
      clearTimeout(timer);
      found.resolve(true);
    }
  },
});

const seen = await found.promise;
await consumer.disconnect();
await running.catch(() => undefined);

if (seen) {
  console.log(`OK: ${target} ada di DLT`);
  process.exit(0);
}
console.error(`FAIL: ${target} tidak ditemukan di DLT dalam 20 detik`);
process.exit(1);
