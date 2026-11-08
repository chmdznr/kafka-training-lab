'use strict';

// Lab 03 - prove the envelope helper rejects incomplete events.
// Run: node solution/envelope-check.js

const assert = require('node:assert/strict');
const {
  createEnvelope,
  validateEnvelope,
  topicFor,
  headersFor,
  EnvelopeError,
} = require('../../../shared/envelope.js');

let checks = 0;

function ok(name, fn) {
  fn();
  checks += 1;
  console.log(`PASS  ${name}`);
}

function rejected(name, fn, expectedFragment) {
  assert.throws(
    fn,
    (error) => {
      assert.ok(error instanceof EnvelopeError, `error bukan EnvelopeError: ${error}`);
      assert.ok(
        error.message.includes(expectedFragment),
        `pesan tidak memuat "${expectedFragment}": ${error.message}`,
      );
      return true;
    },
    name,
  );
  checks += 1;
  console.log(`PASS  ${name}`);
}

const order = createEnvelope({
  eventType: 'order.created.v1',
  version: 1,
  correlationId: 'checkout-123',
  data: {
    orderId: 'order-1',
    customerId: 'customer-1',
    amount: 250000,
    currency: 'IDR',
  },
});

ok('createEnvelope menghasilkan eventId UUID dan occurredAt ISO-8601', () => {
  assert.match(order.eventId, /^[0-9a-f-]{36}$/i);
  assert.ok(!Number.isNaN(Date.parse(order.occurredAt)));
  assert.equal(order.version, 1);
  assert.equal(order.data.orderId, 'order-1');
});

ok('validateEnvelope menerima envelope lengkap', () => {
  assert.equal(validateEnvelope(order), order);
});

ok('topicFor mengembalikan nama topic dari eventType', () => {
  assert.equal(topicFor(order), 'order.created.v1');
  assert.equal(topicFor({ ...order, eventType: 'payment.failed' }), 'payment.failed.v1');
});

ok('headersFor mengisi correlation-id dan event-type', () => {
  const headers = headersFor(order);
  assert.equal(headers['correlation-id'], 'checkout-123');
  assert.equal(headers['event-type'], 'order.created.v1');
  assert.deepEqual(headersFor(order, { traceparent: '00-abc-def-01' }).traceparent, '00-abc-def-01');
});

rejected('menolak event tanpa eventId', () => validateEnvelope({ ...order, eventId: undefined }), 'eventId');
rejected(
  'menolak event tanpa correlationId',
  () => validateEnvelope({ ...order, correlationId: undefined }),
  'correlationId',
);
rejected(
  'menolak eventType yang bukan pola domain.event',
  () => validateEnvelope({ ...order, eventType: 'Order Created' }),
  'eventType',
);
rejected(
  'menolak version yang tidak cocok dengan akhiran eventType',
  () => validateEnvelope({ ...order, version: 2 }),
  'tidak cocok',
);
rejected(
  'menolak occurredAt tanpa zona waktu',
  () => validateEnvelope({ ...order, occurredAt: '2026-11-08 09:15:00' }),
  'occurredAt',
);
rejected(
  'menolak data yang bukan object',
  () => validateEnvelope({ ...order, data: ['order-1'] }),
  'data',
);
rejected(
  'menolak causationId kosong',
  () => validateEnvelope({ ...order, causationId: '' }),
  'causationId',
);

const caused = createEnvelope({
  eventType: 'payment.completed.v1',
  correlationId: order.correlationId,
  causationId: order.eventId,
  data: {
    orderId: 'order-1',
    paymentId: 'payment-1',
    amount: 250000,
    currency: 'IDR',
    method: 'card',
    paidAt: new Date().toISOString(),
  },
});

ok('event turunan menerima causationId dari event sebelumnya', () => {
  assert.equal(validateEnvelope(caused).causationId, order.eventId);
});

console.log(`PASS lab03 envelope: ${checks} pemeriksaan`);
