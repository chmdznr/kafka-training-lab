import { Module, type DynamicModule } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { DbModule } from '../common/db.module.js';
import { KAFKA_BROKERS, KAFKA_CLIENT, OUTBOX_TABLE } from '../common/kafka.js';
import { OutboxPoller } from '../common/outbox-poller.js';
import { PaymentController } from './payment.controller.js';
import { PaymentService } from './payment.service.js';
import { RetryController } from './retry.controller.js';
import { RetryPolicy } from './retry-policy.js';

function producerClient(clientId: string): DynamicModule {
  return ClientsModule.register([
    {
      name: KAFKA_CLIENT,
      transport: Transport.KAFKA,
      options: {
        postfixId: '',
        client: { clientId, brokers: KAFKA_BROKERS },
        producerOnlyMode: true,
        send: { acks: -1 },
      },
    },
  ]);
}

const paymentProviders = [
  PaymentService,
  RetryPolicy,
  ConsumerDrain,
  OutboxPoller,
  { provide: OUTBOX_TABLE, useValue: 'proyek_payment_outbox' },
];

@Module({
  imports: [DbModule, producerClient('payment-service-producer')],
  controllers: [PaymentController],
  providers: paymentProviders,
})
export class AppModule {}

// The retry worker is its own process and consumer group. It also runs the
// outbox poller because it can write payment events while retrying.
@Module({
  imports: [DbModule, producerClient('payment-retry-producer')],
  controllers: [RetryController],
  providers: paymentProviders,
})
export class RetryModule {}
