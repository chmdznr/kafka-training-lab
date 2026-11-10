'use strict';

const fs = require('node:fs');
const { createClient, SUBJECT } = require('../src/sr');

// Uji kompatibilitas skema terhadap versi terakhir subject TANPA mendaftarkannya.
// Hasilnya mengikuti level compatibility subject saat ini (lihat set-compat.js).
async function main() {
  const file = process.argv[2];
  if (!file) {
    console.error('pemakaian: node scripts/check-compat.js <file.avsc>');
    process.exit(2);
  }

  const schema = fs.readFileSync(file, 'utf8');
  const client = createClient();
  const level = await client.getCompatibility(SUBJECT);
  const ok = await client.testSubjectCompatibility(SUBJECT, { schema, schemaType: 'AVRO' });

  console.log(`subject ${SUBJECT}, compatibility ${level}`);
  console.log(`${file} vs versi terakhir: ${ok ? 'KOMPATIBEL' : 'TIDAK KOMPATIBEL'}`);
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
