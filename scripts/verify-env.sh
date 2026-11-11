#!/usr/bin/env bash
# H-2 environment check. Brings up the whole stack, then produces/consumes
# through PLAINTEXT and SASL. The short PASS/FAIL summary can be sent to the
# trainer.
set -uo pipefail

# Git Bash on Windows rewrites arguments like /opt/kafka/bin/... into Windows
# paths before docker sees them. Harmless everywhere else.
export MSYS_NO_PATHCONV=1

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || { echo "FAIL: tidak bisa masuk $ROOT_DIR"; exit 1; }

PASS_COUNT=0
FAIL_COUNT=0
FAILED_NAMES=()

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf '  PASS  %s\n' "$1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAILED_NAMES+=("$1")
  printf '  FAIL  %s\n' "$1"
}

step() {
  printf '\n== %s\n' "$1"
}

echo "Pemeriksaan environment lab Kafka"
echo "OS: $(uname -srm)"
echo "Direktori: $ROOT_DIR"

DOCKER_OK=0
CLUSTER_OK=0

# ---------------------------------------------------------------- Docker
step "Docker"
if command -v docker >/dev/null 2>&1; then
  if DOCKER_VERSION="$(docker version --format '{{.Server.Version}}' 2>/dev/null)"; then
    pass "docker $DOCKER_VERSION"
    DOCKER_OK=1
  else
    fail "docker terpasang tetapi daemon tidak merespons (buka Docker Desktop / start service)"
  fi

  if COMPOSE_VERSION="$(docker compose version --short 2>/dev/null)"; then
    pass "docker compose $COMPOSE_VERSION"
  else
    fail "plugin 'docker compose' tidak tersedia (butuh Compose v2)"
    DOCKER_OK=0
  fi
else
  fail "docker tidak ditemukan di PATH"
fi

# ---------------------------------------------------------------- Node and npm
step "Node dan dependency"
if command -v node >/dev/null 2>&1; then
  NODE_VERSION="$(node -v)"
  NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null)"
  WANT_MAJOR="$(tr -d 'v \n' < .nvmrc 2>/dev/null | cut -d. -f1)"
  if [ -n "$WANT_MAJOR" ] && [ "$NODE_MAJOR" = "$WANT_MAJOR" ]; then
    pass "node $NODE_VERSION (sesuai .nvmrc $WANT_MAJOR)"
  else
    fail "node $NODE_VERSION tidak sesuai .nvmrc (butuh $WANT_MAJOR); jalankan 'nvm use'"
  fi
else
  fail "node tidak ditemukan di PATH (butuh Node 24; jalankan 'nvm use')"
fi

if command -v npm >/dev/null 2>&1; then
  if npm ci --no-audit --no-fund >/tmp/verify-env-npm.log 2>&1; then
    pass "npm ci"
  else
    fail "npm ci gagal (lihat /tmp/verify-env-npm.log)"
    tail -n 5 /tmp/verify-env-npm.log
  fi
else
  fail "npm tidak ditemukan"
fi

if npm run setup:native >/tmp/verify-env-native.log 2>&1; then
  pass "binary native @confluentinc/kafka-javascript terpasang"
else
  fail "binary native gagal dipasang (lihat /tmp/verify-env-native.log)"
  tail -n 5 /tmp/verify-env-native.log
fi

if node -e "const { KafkaJS } = require('@confluentinc/kafka-javascript'); process.exit(typeof KafkaJS?.Kafka === 'function' ? 0 : 1)" >/dev/null 2>&1; then
  pass "native @confluentinc/kafka-javascript termuat"
else
  fail "native @confluentinc/kafka-javascript gagal dimuat"
fi

# ---------------------------------------------------------------- Compose
if [ "$DOCKER_OK" = 1 ]; then
  step "Image dan stack"
  if docker compose pull >/tmp/verify-env-pull.log 2>&1; then
    pass "image docker tersedia"
  else
    fail "docker compose pull gagal (cek jaringan/proxy; lihat /tmp/verify-env-pull.log)"
    tail -n 5 /tmp/verify-env-pull.log
  fi

  # The mini-project integration test (Testcontainers) needs these images on
  # training day; pull them now while the network can still be fixed.
  TEST_IMAGES="confluentinc/cp-kafka:8.4.1 testcontainers/ryuk:0.14.0"
  test_pull_ok=1
  for image in $TEST_IMAGES; do
    docker pull "$image" >>/tmp/verify-env-pull.log 2>&1 || test_pull_ok=0
  done
  if [ "$test_pull_ok" = 1 ]; then
    pass "image Testcontainers tersedia (cp-kafka, ryuk)"
  else
    fail "pull image Testcontainers gagal (lihat /tmp/verify-env-pull.log)"
  fi

  if docker compose up -d >/tmp/verify-env-up.log 2>&1; then
    pass "docker compose up -d"
  else
    fail "docker compose up gagal (cek port 9092-9094, 9192-9194, 8080, 8081, 5432)"
    tail -n 8 /tmp/verify-env-up.log
  fi

  step "Cluster Kafka"
  for _ in $(seq 1 90); do
    if docker compose exec -T kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server kafka-1:29092 >/dev/null 2>&1; then
      CLUSTER_OK=1
      break
    fi
    sleep 2
  done

  if [ "$CLUSTER_OK" = 1 ]; then
    pass "broker menerima koneksi"

    RUNNING_BROKERS="$(docker compose ps --status running --services 2>/dev/null | grep -cE '^kafka-[123]$' || true)"
    if [ "$RUNNING_BROKERS" = 3 ]; then
      pass "3 broker berjalan"
    else
      fail "hanya $RUNNING_BROKERS dari 3 broker berjalan"
    fi

    QUORUM="$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server kafka-1:29092 describe --status 2>/dev/null || true)"
    VOTERS="$(printf '%s\n' "$QUORUM" | grep -o '"id": [0-9]*' | wc -l | tr -d ' ')"
    if [ "$VOTERS" = 3 ]; then
      pass "KRaft quorum 3 voter"
    else
      fail "KRaft quorum tidak lengkap (voter terbaca: $VOTERS)"
    fi
  else
    fail "cluster tidak siap dalam 180 detik (lihat 'docker compose logs kafka-1')"
  fi

  step "Produce dan consume"
  if [ "$CLUSTER_OK" = 1 ]; then
    if node scripts/smoke.js; then
      pass "smoke PLAINTEXT dan SASL"
    else
      fail "smoke produce/consume gagal"
    fi
  else
    fail "smoke dilewati karena cluster belum siap"
  fi
fi

# ---------------------------------------------------------------- Ringkasan
echo
echo "----------------------------------------"
if [ "$FAIL_COUNT" -eq 0 ]; then
  echo "PASS: $PASS_COUNT  FAIL: 0"
  echo "Lingkungan siap dipakai untuk training."
  exit 0
fi

echo "PASS: $PASS_COUNT  FAIL: $FAIL_COUNT"
for name in "${FAILED_NAMES[@]}"; do
  echo "  - $name"
done
echo "Kirim seluruh output ini ke trainer."
exit 1
