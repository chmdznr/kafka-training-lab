import { Global, Module } from '@nestjs/common';
import { DbService } from './db.js';

@Global()
@Module({
  providers: [DbService],
  exports: [DbService],
})
export class DbModule {}
