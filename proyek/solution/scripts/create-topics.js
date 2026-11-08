#!/usr/bin/env node
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'proyek-create-topics', brokers, logLevel: logLevel.NOTHING },
});
const admin = kafka.admin();

const config = {
  numPartitions: 3,
  replicationFactor: 3,
  configEntries: [{ name: 'min.insync.replicas', value: '2' }],
};
const topics = [
  'order.created.v1',
  'payment.completed.v1',
  'payment.failed.v1',
  'order.created.v1.payment-service.retry',
  'order.created.v1.payment-service.dlt',
].map((topic) => ({ topic, ...config }));

await admin.connect();
try {
  const existing = await admin.listTopics();
  for (const topic of topics) {
    if (existing.includes(topic.topic)) {
      console.log(`topic ${topic.topic} sudah ada`);
      continue;
    }
    await admin.createTopics({ topics: [topic] });
    console.log(`topic ${topic.topic} dibuat`);
  }
} finally {
  await admin.disconnect();
}
