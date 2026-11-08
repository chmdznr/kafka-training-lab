'use strict';

const { pool } = require('./db');
const { createKafka } = require('./kafka');
const { headersFor } = require('../../../../shared/envelope.js');

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 50);
const IDLE_MS = Number(process.env.IDLE_MS || 250);
const ERROR_BACKOFF_MS = Number(process.env.ERROR_BACKOFF_MS || 1000);
const MESSAGE_TIMEOUT_MS = Number(process.env.MESSAGE_TIMEOUT_MS || 5000);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const producer = createKafka('order-outbox-publisher').producer({
  kafkaJS: {
    // LATIHAN 2a: producer harus idempotent dan menunggu semua replika (acks=-1).
    idempotent: false,
    acks: 1,
    maxInFlightRequests: 5,
  },
  'message.timeout.ms': MESSAGE_TIMEOUT_MS,
});

async function publishBatch() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // LATIHAN 2b: ambil hanya baris yang belum terkirim, kunci dengan
    // FOR UPDATE SKIP LOCKED supaya dua publisher tidak mengirim baris yang sama.
    // Ganti query di bawah dan lengkapi loop-nya.
    //
    // SELECT id, aggregateid, type, payload
    //   FROM outbox
    //  WHERE published_at IS NULL
    //  ORDER BY created_at, id
    //  LIMIT $1
    //  FOR UPDATE SKIP LOCKED
    const { rows } = await client.query(
      'SELECT id, aggregateid, type, payload FROM outbox LIMIT $1',
      [BATCH_SIZE],
    );

    for (const row of rows) {
      const envelope = row.payload;
      // LATIHAN 2c: kirim envelope ke topic row.type, key row.aggregateid.
      // Gunakan headersFor(envelope) dari shared/envelope.js supaya header
      // 'event-type' dan 'correlation-id' sesuai kontrak.
      //
      // await producer.send({
      //   topic: row.type,
      //   messages: [{
      //     key: row.aggregateid,
      //     value: JSON.stringify(envelope),
      //     headers: headersFor(envelope),
      //   }],
      // });

      // LATIHAN 2d: tandai baris terkirim di transaksi yang sama.
      //
      // await client.query('UPDATE outbox SET published_at = now() WHERE id = $1', [row.id]);
      void envelope;
    }

    await client.query('COMMIT');
    return rows.length;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

let running = true;

async function main() {
  await producer.connect();
  console.log(JSON.stringify({ at: new Date().toISOString(), msg: 'publisher mulai' }));

  while (running) {
    try {
      const count = await publishBatch();
      if (count === 0) {
        await sleep(IDLE_MS);
      }
    } catch (err) {
      console.error(JSON.stringify({
        at: new Date().toISOString(),
        msg: 'publish gagal, baris outbox tetap pending dan akan dicoba lagi',
        error: err.message,
      }));
      await sleep(ERROR_BACKOFF_MS);
    }
  }

  await producer.disconnect();
  await pool.end();
  console.log(JSON.stringify({ at: new Date().toISOString(), msg: 'publisher berhenti' }));
}

async function shutdown(signal) {
  console.log(JSON.stringify({ at: new Date().toISOString(), msg: 'shutdown', signal }));
  running = false;
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
