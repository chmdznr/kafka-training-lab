import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import {
  KAFKA_BROKERS,
  PAYMENT_SERVICE_GROUP,
} from '../common/kafka.js';
import { AppModule } from './app.module.js';

// Hybrid app: an HTTP listener for health checks plus one Kafka consumer.
const app = await NestFactory.create(AppModule);

app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.KAFKA,
  options: {
    postfixId: '',
    client: { clientId: 'payment-service', brokers: KAFKA_BROKERS },
    consumer: {
      groupId: PAYMENT_SERVICE_GROUP,
      allowAutoTopicCreation: false,
    },
    // Manual commit: the offset advances only after the handler commits it.
    run: { autoCommit: false, partitionsConsumedConcurrently: 1 },
    subscribe: { fromBeginning: true },
  },
});

// SIGTERM/SIGINT run the shutdown hooks: Nest closes the consumer, which waits
// for the handler that is running before disconnecting.
app.enableShutdownHooks();

await app.startAllMicroservices();

const port = Number(process.env.HTTP_PORT ?? 3001);
await app.listen(port);

console.log(
  JSON.stringify({
    level: 'info',
    service: 'payment-service',
    event: 'ready',
    group: PAYMENT_SERVICE_GROUP,
    port,
  }),
);
