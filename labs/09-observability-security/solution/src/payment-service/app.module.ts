import { Module } from '@nestjs/common';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { DbModule } from '../common/db.module.js';
import { PaymentController } from './payment.controller.js';
import { PaymentService } from './payment.service.js';
import { RetryController } from './retry.controller.js';
import { RetryPolicy } from './retry-policy.js';

const paymentProviders = [PaymentService, RetryPolicy, ConsumerDrain];

@Module({
  imports: [DbModule],
  controllers: [PaymentController],
  providers: paymentProviders,
})
export class AppModule {}

// The retry worker runs as its own process with its own consumer group, so a
// backoff sleep never blocks the main topic. Its module only exposes the retry
// handler, so its consumer subscribes to the retry topic alone.
@Module({
  imports: [DbModule],
  controllers: [RetryController],
  providers: paymentProviders,
})
export class RetryModule {}
