'use strict';

const fs = require('node:fs');
const { createClient, SUBJECT } = require('../src/sr');

async function main() {
  const file = process.argv[2];
  if (!file) {
    console.error('pemakaian: node scripts/register.js <file.avsc>');
    process.exit(2);
  }

  const schema = fs.readFileSync(file, 'utf8');
  const client = createClient();

  try {
    const id = await client.register(SUBJECT, { schema, schemaType: 'AVRO' });
    const latest = await client.getLatestSchemaMetadata(SUBJECT);
    console.log(`subject ${SUBJECT}`);
    console.log(`versi ${latest.version}, schema id ${id}`);
    console.log('status: DITERIMA registry');
  } catch (err) {
    console.error(`subject ${SUBJECT}`);
    console.error('status: DITOLAK registry');
    console.error(err.message);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
