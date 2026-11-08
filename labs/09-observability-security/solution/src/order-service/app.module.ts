import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KAFKA_BROKERS, KAFKA_CLIENT } from '../common/kafka.js';
import { OrdersController } from './orders.controller.js';
import { OrdersService } from './orders.service.js';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: {
          // Without this Nest appends "-client" to clientId and groupId.
          postfixId: '',
          client: { clientId: 'order-service', brokers: KAFKA_BROKERS },
          // The HTTP side only produces events; it does not join a consumer group.
          producerOnlyMode: true,
          // acks=all for every record sent through this client.
          send: { acks: -1 },
        },
      },
    ]),
  ],
  controllers: [OrdersController],
  providers: [OrdersService],
})
export class AppModule {}
