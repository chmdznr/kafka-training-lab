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
    // LATIHAN 1: daftarkan `schema` ke SUBJECT dengan schemaType 'AVRO'.
    //
    //   const id = await client.register(SUBJECT, { schema, schemaType: 'AVRO' });
    //   const latest = await client.getLatestSchemaMetadata(SUBJECT);
    //
    // Cetak subject, versi (latest.version), dan schema id.
    console.log('LATIHAN 1: implement client.register di scripts/register.js');
    process.exit(1);
  } catch (err) {
    // LATIHAN 2: kalau registry menolak skema (HTTP 409), cetak err.message
    // dan keluar dengan kode 1. Jangan menelan error-nya.
    throw err;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
