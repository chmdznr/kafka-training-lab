'use strict';

const { pool } = require('../src/db');
const { createKafka } = require('../src/kafka');
const { TOPICS } = require('../src/config');
const { ensureTopics } = require('./setup');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  await pool.query('TRUNCATE orders, outbox');
  console.log('tabel orders dan outbox dikosongkan');

  // Optional and destructive: also clears this lab's topics so the check script
  // starts from zero. Only use when every participant/process is between labs.
  if (!process.argv.includes('--topics')) {
    console.log('topik tidak dihapus; event lama masih ada dan check akan melihatnya sebagai event tanpa order');
    console.log('untuk mulai dari nol: npm run reset -- --topics');
  }

  if (process.argv.includes('--topics')) {
    const admin = createKafka('lab07-reset').admin();
    await admin.connect();
    const existing = await admin.listTopics();
    const present = Object.values(TOPICS).filter((topic) => existing.includes(topic));
    if (present.length > 0) {
      await admin.deleteTopics({ topics: present, timeout: 15000 });
      for (let attempt = 0; attempt < 30; attempt += 1) {
        const now = await admin.listTopics();
        if (present.every((topic) => !now.includes(topic))) break;
        await sleep(500);
      }
    }
    await admin.disconnect();
    await ensureTopics();
    console.log('topic dibuat ulang');
  }

  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
