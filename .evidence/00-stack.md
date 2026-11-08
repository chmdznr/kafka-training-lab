# Evidence stack Docker

Tanggal: 2026-11-08
Host: macOS arm64, Docker 29.8.2, Compose 5.5.0

## Service

`docker compose ps` (semua sehat):

```
kafka-1             Up (healthy)   0.0.0.0:9092->9092/tcp, 0.0.0.0:9192->9192/tcp
kafka-2             Up (healthy)   0.0.0.0:9093->9092/tcp, 0.0.0.0:9193->9192/tcp
kafka-3             Up (healthy)   0.0.0.0:9094->9092/tcp, 0.0.0.0:9194->9192/tcp
kafka-ui            Up             http://localhost:8080
schema-registry     Up             http://localhost:8081
postgres            Up (healthy)   localhost:5432
```

`kafka-init` selesai dengan exit 0:

```
Completed updating config for user app.
SCRAM user 'app' siap
```

## KRaft quorum

```
ClusterId:              5L6g3nShT-eMCtK--X86sw
LeaderId:               1
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
```

## Topic replicated

```
$ docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:29092 --describe --topic order.created.v1
Topic: order.created.v1  PartitionCount: 3  ReplicationFactor: 3
  Configs: min.insync.replicas=2
  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

## Klien PLAINTEXT dan SASL

`node scripts/smoke.js` (dari `lab/`):

```
PASS plaintext: produced + consumed lab.verify.v1 (RF3, 3 partitions)
PASS sasl: produced + consumed lab.verify.v1 (RF3, 3 partitions)
PASS smoke: PLAINTEXT dan SASL berfungsi
```

SASL memakai `localhost:9192-9194`, mekanisme SCRAM-SHA-512, user `app`.

## UI, Schema Registry, Postgres

```
curl -o /dev/null -w '%{http_code}' http://localhost:8080/          -> 200
curl -o /dev/null -w '%{http_code}' http://localhost:8081/subjects  -> 200
psql -h localhost -U app -d shop -tAc 'select version()'            -> PostgreSQL 17.7
```

Jaeger (profile tracing) juga diuji: UI `http://localhost:16686` -> 200,
OTLP HTTP `http://localhost:4318/v1/traces` -> 200, lalu container dihentikan.

## Image multi-arch

Diperiksa dengan `docker manifest inspect`; semua menyediakan `amd64` dan
`arm64`:

- apache/kafka:4.4.0
- kafbat/kafka-ui:v1.5.0
- confluentinc/cp-schema-registry:8.4.0
- postgres:17
- quay.io/debezium/connect:3.7.1.Final
- jaegertracing/jaeger:2.0.0
