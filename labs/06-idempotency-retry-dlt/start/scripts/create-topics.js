#!/usr/bin/env node
// Topics are not auto-created (auto.create.topics.enable=false).
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'lab06-create-topics', brokers, logLevel: logLevel.NOTHING },
});
const admin = kafka.admin();

const topicConfig = {
  numPartitions: 3,
  replicationFactor: 3,
  configEntries: [{ name: 'min.insync.replicas', value: '2' }],
};
const topics = [
  { topic: 'order.created.v1', ...topicConfig },
  { topic: 'order.created.v1.payment-service.retry', ...topicConfig },
  { topic: 'order.created.v1.payment-service.dlt', ...topicConfig },
];

await admin.connect();
try {
  const existing = await admin.listTopics();
  for (const topic of topics) {
    if (existing.includes(topic.topic)) {
      console.log(`topic ${topic.topic} sudah ada`);
      continue;
    }
    await admin.createTopics({ topics: [topic] });
    console.log(`topic ${topic.topic} dibuat (3 partisi, RF 3, min.insync.replicas=2)`);
  }
} finally {
  await admin.disconnect();
}
