'use strict';

const { Pool } = require('pg');
const { DATABASE_URL } = require('./config');

const pool = new Pool({ connectionString: DATABASE_URL, max: 10 });

// An idle client can be dropped by the server (restart, admin command,
// network blip). Without a listener that event crashes the whole process.
pool.on('error', (err) => {
  console.error(JSON.stringify({
    at: new Date().toISOString(),
    msg: 'koneksi postgres idle terputus, pool akan membuat koneksi baru',
    error: err.message,
  }));
});

// outbox.payload holds the complete event envelope, so the row and the Kafka
// record carry exactly the same bytes of information.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS orders (
  id          uuid PRIMARY KEY,
  customer_id text NOT NULL,
  item        text NOT NULL,
  amount      numeric(12, 2) NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
  id            uuid PRIMARY KEY,
  aggregatetype text NOT NULL,
  aggregateid   uuid NOT NULL,
  type          text NOT NULL,
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx
  ON outbox (created_at)
  WHERE published_at IS NULL;
`;

async function ensureSchema() {
  await pool.query(SCHEMA);
}

module.exports = { pool, ensureSchema };
