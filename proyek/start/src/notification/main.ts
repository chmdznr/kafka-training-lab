import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import {
  KAFKA_BROKERS,
  NOTIFICATION_GROUP,
} from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';
import { AppModule } from './app.module.js';

const app = await NestFactory.create(AppModule);
app.useLogger(new JsonLogger('notification-service'));

app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.KAFKA,
  options: {
    postfixId: '',
    client: { clientId: 'notification-service', brokers: KAFKA_BROKERS },
    consumer: {
      groupId: NOTIFICATION_GROUP,
      allowAutoTopicCreation: false,
    },
    run: { autoCommit: false },
    subscribe: { fromBeginning: true },
  },
});

app.enableShutdownHooks();
await app.startAllMicroservices();

const port = Number(process.env.HTTP_PORT ?? 3002);
await app.listen(port);

console.log(
  JSON.stringify({
    level: 'info',
    service: 'notification-service',
    event: 'ready',
    port,
    group: NOTIFICATION_GROUP,
  }),
);
