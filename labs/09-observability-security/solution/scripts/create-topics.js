#!/usr/bin/env node
// Topics are not auto-created (auto.create.topics.enable=false).
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const kafka = new Kafka({
  kafkaJS: { clientId: 'lab09-create-topics', brokers, logLevel: logLevel.NOTHING },
});
const admin = kafka.admin();

const topicConfig = {
  numPartitions: 3,
  replicationFactor: 3,
  configEntries: [{ name: 'min.insync.replicas', value: '2' }],
};
const mainTopic = process.env.ORDER_CREATED_TOPIC ?? 'order.created.v1';
const group = process.env.PAYMENT_SERVICE_GROUP ?? 'payment-service';
const topics = [
  { topic: mainTopic, ...topicConfig },
  { topic: `${mainTopic}.${group}.retry`, ...topicConfig },
  { topic: `${mainTopic}.${group}.dlt`, ...topicConfig },
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
