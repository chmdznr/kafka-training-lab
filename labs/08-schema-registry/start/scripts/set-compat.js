'use strict';

const { Compatibility } = require('@confluentinc/schemaregistry');
const schemaRegistry = require('../src/sr');

async function main() {
  const raw = (process.argv[2] || 'BACKWARD').toUpperCase();
  if (!Compatibility[raw]) {
    console.error(`level tidak dikenal: ${raw}`);
    console.error(`pilihan: ${Object.keys(Compatibility).join(', ')}`);
    process.exit(2);
  }

  const client = schemaRegistry.createClient();
  const result = await client.updateCompatibility(schemaRegistry.SUBJECT, Compatibility[raw]);
  console.log(`compatibility ${schemaRegistry.SUBJECT}: ${result.compatibility ?? result}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
