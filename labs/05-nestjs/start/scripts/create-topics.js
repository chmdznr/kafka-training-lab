#!/usr/bin/env node
// Topics are not auto-created (auto.create.topics.enable=false), so every lab
// creates the topics it needs explicitly.
import pkg from '@confluentinc/kafka-javascript';

const { Kafka, logLevel } = pkg.KafkaJS;

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
// In @confluentinc/kafka-javascript the KafkaJS-compatible options live under
// the `kafkaJS` block; everything outside it is a librdkafka property.
const kafka = new Kafka({
  kafkaJS: { clientId: 'lab05-create-topics', brokers, logLevel: logLevel.NOTHING },
});
const admin = kafka.admin();

const topics = [
  {
    topic: 'order.created.v1',
    numPartitions: 3,
    replicationFactor: 3,
    configEntries: [
      { name: 'min.insync.replicas', value: '2' },
      { name: 'cleanup.policy', value: 'delete' },
    ],
  },
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
    console.log(
      `topic ${topic.topic} dibuat (partisi=${topic.numPartitions}, RF=${topic.replicationFactor}, min.insync.replicas=2)`,
    );
  }
} finally {
  await admin.disconnect();
}
