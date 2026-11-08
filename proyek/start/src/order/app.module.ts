import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { DbModule } from '../common/db.module.js';
import { KAFKA_BROKERS, KAFKA_CLIENT, OUTBOX_TABLE } from '../common/kafka.js';
import { OutboxPoller } from '../common/outbox-poller.js';
import { OrdersController } from './orders.controller.js';
import { OrdersService } from './orders.service.js';
import { PaymentEventsController } from './payment-events.controller.js';

@Module({
  imports: [
    DbModule,
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: {
          postfixId: '',
          client: { clientId: 'order-service-producer', brokers: KAFKA_BROKERS },
          producerOnlyMode: true,
          send: { acks: -1 },
        },
      },
    ]),
  ],
  controllers: [OrdersController, PaymentEventsController],
  providers: [
    OrdersService,
    ConsumerDrain,
    OutboxPoller,
    { provide: OUTBOX_TABLE, useValue: 'proyek_order_outbox' },
  ],
})
export class AppModule {}
