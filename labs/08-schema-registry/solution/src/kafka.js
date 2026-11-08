'use strict';

const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094')
  .split(',')
  .map((broker) => broker.trim());

function createKafka(clientId) {
  return new Kafka({
    kafkaJS: {
      clientId,
      brokers: BROKERS,
      logLevel: logLevel.NOTHING,
    },
  });
}

// Lab 07 creates this topic too. Creating it here keeps Lab 08 runnable on its
// own; the cluster has auto.create.topics.enable=false.
async function ensureTopic(topic) {
  const admin = createKafka('lab08-setup').admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    if (!existing.includes(topic)) {
      await admin.createTopics({
        timeout: 15000,
        topics: [{
          topic,
          numPartitions: 3,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        }],
      });
    }
  } finally {
    await admin.disconnect();
  }
}

module.exports = { BROKERS, createKafka, ensureTopic };
