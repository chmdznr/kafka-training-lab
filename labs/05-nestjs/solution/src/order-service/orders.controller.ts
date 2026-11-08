import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Post,
} from '@nestjs/common';
import { OrdersService, type OrderInput } from './orders.service.js';

@Controller()
export class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  @Post('orders')
  @HttpCode(202)
  create(
    @Body() body: Partial<OrderInput>,
    @Headers('x-correlation-id') correlationId?: string,
  ) {
    return this.orders.accept(body, correlationId);
  }
}
