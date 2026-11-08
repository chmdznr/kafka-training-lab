'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { SchemaRegistryClient } = require('@confluentinc/schemaregistry');

const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const TOPIC = 'lab08.order.created.v1';
// Default subject name strategy for a value schema: <topic>-value.
const SUBJECT = `${TOPIC}-value`;

function createClient() {
  return SchemaRegistryClient.newClient({ baseURLs: [SCHEMA_REGISTRY_URL] });
}

function readSchema(relativePath) {
  return fs.readFileSync(path.join(__dirname, '..', 'avro', relativePath), 'utf8');
}

module.exports = { SCHEMA_REGISTRY_URL, TOPIC, SUBJECT, createClient, readSchema };
