import { Module } from '@nestjs/common';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { DbModule } from '../common/db.module.js';
import { PaymentController } from './payment.controller.js';
import { PaymentService } from './payment.service.js';

@Module({
  imports: [DbModule],
  controllers: [PaymentController],
  providers: [PaymentService, ConsumerDrain],
})
export class AppModule {}
