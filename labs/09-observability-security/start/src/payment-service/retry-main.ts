import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import {
  KAFKA_BROKERS,
  PAYMENT_RETRY_GROUP,
  RETRY_TOPIC,
} from '../common/kafka.js';
import { RetryModule } from './app.module.js';

const app = await NestFactory.createMicroservice<MicroserviceOptions>(
  RetryModule,
  {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: { clientId: 'payment-service-retry', brokers: KAFKA_BROKERS },
      consumer: {
        groupId: PAYMENT_RETRY_GROUP,
        allowAutoTopicCreation: false,
      },
      run: { autoCommit: false, partitionsConsumedConcurrently: 1 },
      subscribe: { fromBeginning: true },
    },
  },
);

app.enableShutdownHooks();
await app.listen();

console.log(
  JSON.stringify({
    level: 'info',
    service: 'payment-service-retry',
    event: 'ready',
    group: PAYMENT_RETRY_GROUP,
    topic: RETRY_TOPIC,
  }),
);
