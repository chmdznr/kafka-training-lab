import { Module } from '@nestjs/common';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { DbModule } from '../common/db.module.js';
import { NotificationController } from './notification.controller.js';
import { NotificationService } from './notification.service.js';

@Module({
  imports: [DbModule],
  controllers: [NotificationController],
  providers: [NotificationService, ConsumerDrain],
})
export class AppModule {}
