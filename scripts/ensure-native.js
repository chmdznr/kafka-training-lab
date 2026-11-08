'use strict';

// Fallback installer for the @confluentinc/kafka-javascript native binary.
//
// The package uses node-pre-gyp, which downloads from
// github.com/.../releases/download. On some networks that endpoint returns
// 504 while the GitHub API endpoint still works. This script runs only when
// build/Release/*.node is missing: fetch the matching release asset through
// the API, extract it, done. On a normal network the package's own install
// already produced the binary and this script is a no-op.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const https = require('node:https');
const { execFileSync } = require('node:child_process');

const LAB_ROOT = path.resolve(__dirname, '..');
const PKG_DIR = path.join(LAB_ROOT, 'node_modules', '@confluentinc', 'kafka-javascript');
const BINARY = path.join(PKG_DIR, 'build', 'Release', 'confluent-kafka-javascript.node');

function log(message) {
  console.log(`[ensure-native] ${message}`);
}

function get(url, headers = {}, redirects = 5) {
  return new Promise((resolve, reject) => {
    const request = https.get(
      url,
      { headers: { 'User-Agent': 'kafka-training-lab', ...headers } },
      (response) => {
        if (
          response.statusCode >= 300 &&
          response.statusCode < 400 &&
          response.headers.location &&
          redirects > 0
        ) {
          response.resume();
          resolve(get(new URL(response.headers.location, url).toString(), headers, redirects - 1));
          return;
        }
        if (response.statusCode !== 200) {
          response.resume();
          reject(new Error(`HTTP ${response.statusCode} dari ${url}`));
          return;
        }
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => resolve(Buffer.concat(chunks)));
      },
    );
    request.on('error', reject);
    request.setTimeout(60000, () => request.destroy(new Error(`timeout: ${url}`)));
  });
}

async function main() {
  if (fs.existsSync(BINARY)) {
    log('binary native sudah ada');
    return;
  }
  if (!fs.existsSync(path.join(PKG_DIR, 'package.json'))) {
    throw new Error('paket belum terpasang; jalankan npm ci lebih dulu');
  }
  if (!['darwin', 'linux'].includes(process.platform)) {
    throw new Error(`platform ${process.platform} belum didukung prebuilt; build dari source`);
  }

  const pkg = JSON.parse(fs.readFileSync(path.join(PKG_DIR, 'package.json'), 'utf8'));
  const libc =
    process.platform === 'linux'
      ? process.report?.getReport?.().header?.glibcVersionRuntime
        ? 'glibc'
        : 'musl'
      : 'unknown';
  const assetName = `confluent-kafka-javascript-v${pkg.version}-node-v${process.versions.modules}-${process.platform}-${libc}-${process.arch}.tar.gz`;

  log(`mencari ${assetName}`);
  const release = JSON.parse(
    await get(`https://api.github.com/repos/confluentinc/confluent-kafka-javascript/releases/tags/v${pkg.version}`, {
      Accept: 'application/vnd.github+json',
    }),
  );
  const asset = (release.assets || []).find((candidate) => candidate.name === assetName);
  if (!asset) {
    throw new Error(`asset ${assetName} tidak ditemukan di rilis v${pkg.version}`);
  }

  log(`mengunduh asset ${asset.id} (${asset.size} bytes) lewat GitHub API`);
  const archive = await get(
    `https://api.github.com/repos/confluentinc/confluent-kafka-javascript/releases/assets/${asset.id}`,
    { Accept: 'application/octet-stream' },
  );

  const tempFile = path.join(os.tmpdir(), `confluent-kafka-js-${process.pid}.tar.gz`);
  fs.writeFileSync(tempFile, archive);
  try {
    fs.mkdirSync(path.join(PKG_DIR, 'build'), { recursive: true });
    execFileSync('tar', [
      '-xzf',
      tempFile,
      '-C',
      path.join(PKG_DIR, 'build'),
      'Release/confluent-kafka-javascript.node',
    ]);
  } finally {
    fs.rmSync(tempFile, { force: true });
  }

  if (!fs.existsSync(BINARY)) {
    throw new Error('binary tidak muncul setelah ekstrak');
  }
  log(`selesai: ${BINARY}`);
}

main().catch((error) => {
  console.error(`[ensure-native] ${error.message}`);
  console.error('[ensure-native] jalankan: npm rebuild @confluentinc/kafka-javascript --build-from-source');
  process.exit(1);
});
