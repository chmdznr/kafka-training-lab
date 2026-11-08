'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const { pool, ensureSchema } = require('./db');
const { createEnvelope } = require('../../../../shared/envelope.js');
const { PORT, TOPICS, CORRELATION_PREFIX } = require('./config');

const MAX_BODY_BYTES = 16 * 1024;

function sendJson(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (err) {
        reject(new Error('body must be JSON'));
      }
    });
    req.on('error', reject);
  });
}

function validateOrderInput(input) {
  if (typeof input?.customerId !== 'string' || input.customerId.trim() === '') {
    return 'customerId wajib diisi';
  }
  if (typeof input?.item !== 'string' || input.item.trim() === '') {
    return 'item wajib diisi';
  }
  if (!Number.isFinite(input?.amount) || input.amount <= 0) {
    return 'amount harus angka positif';
  }
  return null;
}

async function createOrder(input) {
  const orderId = crypto.randomUUID();
  const correlationId = `${CORRELATION_PREFIX}${crypto.randomUUID()}`;
  const envelope = createEnvelope({
    eventType: TOPICS.orderCreated,
    correlationId,
    data: {
      orderId,
      customerId: input.customerId,
      item: input.item,
      amount: input.amount,
    },
  });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO orders (id, customer_id, item, amount)
       VALUES ($1, $2, $3, $4)`,
      [orderId, input.customerId, input.item, input.amount],
    );

    // LATIHAN 1: simpan juga satu baris outbox di dalam transaksi yang sama.
    // Kolom: id, aggregatetype ('order'), aggregateid (orderId), type
    // (TOPICS.orderCreated), payload (envelope). Pakai envelope.eventId sebagai
    // outbox.id supaya header `id` Debezium sama dengan eventId di payload.
    // Kalau INSERT di bawah gagal, ROLLBACK harus ikut membatalkan order.
    //
    // await client.query(
    //   `INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
    //    VALUES ($1, $2, $3, $4, $5)`,
    //   [envelope.eventId, 'order', orderId, TOPICS.orderCreated, envelope],
    // );

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  console.log(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'order tersimpan',
    orderId,
    correlationId,
    outbox: 'belum diimplementasikan',
  }));
  return { orderId, correlationId };
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'POST' && req.url === '/orders') {
      const input = await readJsonBody(req);
      const error = validateOrderInput(input);
      if (error) {
        sendJson(res, 400, { error });
        return;
      }
      const result = await createOrder(input);
      sendJson(res, 201, result);
      return;
    }
    if (req.method === 'GET' && req.url === '/health') {
      sendJson(res, 200, { ok: true });
      return;
    }
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    console.error(JSON.stringify({ at: new Date().toISOString(), msg: 'request gagal', error: err.message }));
    sendJson(res, 500, { error: err.message });
  }
});

async function main() {
  await ensureSchema();
  server.listen(PORT, () => {
    console.log(JSON.stringify({ at: new Date().toISOString(), msg: `order-service mendengarkan di :${PORT}` }));
  });
}

async function shutdown(signal) {
  console.log(JSON.stringify({ at: new Date().toISOString(), msg: 'shutdown', signal }));
  server.close(async () => {
    await pool.end();
    process.exit(0);
  });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
