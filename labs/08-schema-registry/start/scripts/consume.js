'use strict';

const avro = require('avsc');
const { AvroDeserializer, SerdeType } = require('@confluentinc/schemaregistry');
const { createClient, readSchema, SUBJECT, TOPIC } = require('../src/sr');
const { createKafka } = require('../src/kafka');

const IDLE_MS = Number(process.env.IDLE_MS || 6000);
const OLD_READER = process.argv.includes('--old-reader');

// Wire format from the serializer: magic byte 0x00 + 4-byte schema id + Avro body.
const MAGIC_BYTE = 0;
const HEADER_BYTES = 5;

function decodeSchemaId(buffer) {
  return buffer.readUInt32BE(1);
}

async function main() {
  const registry = createClient();
  const deserializer = OLD_READER
    ? null
    : new AvroDeserializer(registry, SerdeType.VALUE, {});
  const oldReaderType = OLD_READER
    ? avro.Type.forSchema(JSON.parse(readSchema('order-created-v1.avsc')))
    : null;
  const writerTypes = new Map();

  async function writerTypeFor(schemaId) {
    if (!writerTypes.has(schemaId)) {
      const info = await registry.getBySubjectAndId(SUBJECT, schemaId);
      writerTypes.set(schemaId, avro.Type.forSchema(JSON.parse(info.schema)));
    }
    return writerTypes.get(schemaId);
  }

  const groupId = `lab08-consume-${Date.now()}`;
  const consumer = createKafka('lab08-consumer').consumer({
    kafkaJS: { groupId, fromBeginning: true, readUncommitted: false },
  });

  let read = 0;
  let skipped = 0;
  let idleTimer;
  let resolveIdle;
  const idle = new Promise((resolve) => { resolveIdle = resolve; });
  const armIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(resolveIdle, IDLE_MS);
  };

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });
  armIdle();

  const running = consumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value;
      if (!value || value.length <= HEADER_BYTES || value[0] !== MAGIC_BYTE) {
        skipped += 1;
        return;
      }

      if (!OLD_READER) {
        const event = await deserializer.deserialize(TOPIC, value);
        const hasChannel = event.data?.channel !== undefined;
        console.log(`[consumer] orderId=${event.data.orderId} amount=${event.data.amount} channel=${event.data.channel ?? '(tidak ada)'} source=${hasChannel ? 'v2+' : 'v1'}`);
        console.log(JSON.stringify(event));
      } else {
        const schemaId = decodeSchemaId(value);
        const writerType = await writerTypeFor(schemaId);
        const projected = oldReaderType.fromBuffer(
          value.subarray(HEADER_BYTES),
          oldReaderType.createResolver(writerType),
        );
        console.log(`[consumer lama, skema v1] eventId=${projected.eventId} orderId=${projected.data.orderId} amount=${projected.data.amount} channel=(tidak ada di skema v1)`);
      }

      read += 1;
      armIdle();
    },
  });

  await idle;
  await consumer.stop().catch(() => {});
  await running.catch(() => {});
  await consumer.disconnect().catch(() => {});

  console.log('');
  console.log(`dibaca: ${read} event Avro`);
  console.log(`dilewati: ${skipped} record yang bukan Avro Schema Registry (misalnya event JSON dari lab lain)`);
  if (!OLD_READER && read === 0) {
    console.log('tidak ada event Avro. Jalankan scripts/produce.js lebih dulu.');
  }
  if (!OLD_READER && read > 0) {
    console.log('consumer memakai writer schema dari registry, termasuk field baru bila ada.');
  }
  if (OLD_READER && read > 0) {
    console.log('consumer lama (reader schema v1) tetap membaca semua event di atas.');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
