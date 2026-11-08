import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KAFKA_BROKERS, ORDER_GROUP } from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';
import { AppModule } from './app.module.js';

const app = await NestFactory.create(AppModule);
app.useLogger(new JsonLogger('order-service'));

// The order-service consumes payment events to update the order status.
app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.KAFKA,
  options: {
    postfixId: '',
    client: { clientId: 'order-service', brokers: KAFKA_BROKERS },
    consumer: {
      groupId: ORDER_GROUP,
      allowAutoTopicCreation: false,
    },
    run: { autoCommit: false },
    // The transport subscribes to every @EventPattern topic in the module.
    subscribe: { fromBeginning: true },
  },
});

app.enableShutdownHooks();
await app.startAllMicroservices();

const port = Number(process.env.HTTP_PORT ?? 3000);
await app.listen(port);

console.log(
  JSON.stringify({
    level: 'info',
    service: 'order-service',
    event: 'ready',
    port,
    group: ORDER_GROUP,
  }),
);
