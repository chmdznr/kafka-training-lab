import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';

const app = await NestFactory.create(AppModule);
app.enableShutdownHooks();

const port = Number(process.env.HTTP_PORT ?? 3000);
await app.listen(port);

console.log(
  JSON.stringify({
    level: 'info',
    service: 'order-service',
    event: 'listening',
    port,
  }),
);
