'use strict';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094')
  .split(',')
  .map((broker) => broker.trim());

const TOPICS = {
  orderCreated: 'order.created.v1',
  paymentCompleted: 'payment.completed.v1',
};

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://app:app@localhost:5432/shop';

const PORT = Number(process.env.PORT || 3007);

// Tag every order created by this lab. Lab 05/06 and smoke tests also use
// order.created.v1, so the checker must be able to tell this lab's events
// apart from theirs.
const CORRELATION_PREFIX = process.env.CORRELATION_PREFIX || 'lab07-';

module.exports = { BROKERS, TOPICS, DATABASE_URL, PORT, CORRELATION_PREFIX };
