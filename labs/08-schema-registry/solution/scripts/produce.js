'use strict';

const crypto = require('node:crypto');
const { AvroSerializer, SerdeType } = require('@confluentinc/schemaregistry');
const { createEnvelope, headersFor } = require('../../../../shared/envelope.js');
const { createClient, SUBJECT, TOPIC } = require('../src/sr');
const { createKafka, ensureTopic } = require('../src/kafka');

function argValue(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}

async function main() {
  const version = Number(argValue('--version', '0'));
  if (![1, 2].includes(version)) {
    console.error('pemakaian: node scripts/produce.js --version 1|2 [--count N]');
    process.exit(2);
  }
  const count = Number(argValue('--count', '1'));

  await ensureTopic(TOPIC);

  const registry = createClient();
  const metadata = await registry.getSchemaMetadata(SUBJECT, version, false);

  // useSchemaId pins the writer schema to this registered version, so we can
  // still produce v1 events after v2 exists.
  const serializer = new AvroSerializer(registry, SerdeType.VALUE, {
    useSchemaId: metadata.id,
  });

  const producer = createKafka('lab08-producer').producer({
    kafkaJS: { idempotent: true, acks: -1, maxInFlightRequests: 5 },
  });
  await producer.connect();

  for (let i = 0; i < count; i += 1) {
    const orderId = crypto.randomUUID();
    const data = {
      orderId,
      customerId: 'c-lab08',
      item: 'kopi',
      amount: 25000,
    };
    if (version >= 2) {
      data.channel = 'web';
    }

    const event = createEnvelope({
      eventType: TOPIC,
      correlationId: crypto.randomUUID(),
      data,
    });

    const value = await serializer.serialize(TOPIC, event);
    await producer.send({
      topic: TOPIC,
      messages: [{
        key: orderId,
        value,
        headers: headersFor(event),
      }],
    });

    console.log(`terkirim: schema v${version} id=${metadata.id} orderId=${orderId} bytes=${value.length}`);
  }

  await producer.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
