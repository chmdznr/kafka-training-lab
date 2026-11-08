'use strict';

const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;
const { BROKERS } = require('./config');

// One Kafka object per process, then producer()/consumer()/admin() from it.
function createKafka(clientId) {
  return new Kafka({
    kafkaJS: {
      clientId,
      brokers: BROKERS,
      logLevel: logLevel.NOTHING,
    },
  });
}

module.exports = { createKafka };
