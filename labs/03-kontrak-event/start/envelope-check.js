'use strict';

// Lab 03 — titik awal.
// Lengkapi pemeriksaan di bawah, lalu jalankan: node start/envelope-check.js
// Helper lengkap ada di ../../../shared/envelope.js.

const assert = require('node:assert/strict');

function latihan(message) {
  console.error(`FAIL lab03 envelope: LATIHAN: ${message}`);
  process.exit(1);
}
const { createEnvelope, validateEnvelope, topicFor, headersFor } = require('../../../shared/envelope.js');

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

latihan('tulis pemeriksaan eventId, occurredAt, topicFor, headersFor, dan kasus yang harus ditolak');

// LATIHAN: pastikan eventId berbentuk UUID dan occurredAt bisa di-parse.
// assert.match(order.eventId, ...);
// assert.ok(!Number.isNaN(Date.parse(order.occurredAt)));

// LATIHAN: pastikan topicFor(order) mengembalikan 'order.created.v1'.
// assert.equal(topicFor(order), ...);

// LATIHAN: pastikan headersFor(order) berisi 'correlation-id' dan 'event-type'.

// LATIHAN: pastikan validateEnvelope MENOLAK:
// - event tanpa eventId
// - event tanpa correlationId
// - data yang bukan object
// Gunakan assert.throws(() => validateEnvelope({ ...order, eventId: undefined }), ...).
