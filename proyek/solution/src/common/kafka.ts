export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

export const KAFKA_CLIENT = 'KAFKA_CLIENT';
export const OUTBOX_TABLE = 'OUTBOX_TABLE';

export const ORDER_CREATED = 'order.created.v1';
export const PAYMENT_COMPLETED = 'payment.completed.v1';
export const PAYMENT_FAILED = 'payment.failed.v1';

export const ORDER_GROUP = 'order-service';
export const PAYMENT_GROUP = 'payment-service';
export const PAYMENT_RETRY_GROUP = 'payment-service-retry';
export const NOTIFICATION_GROUP = 'notification-service';

export const PAYMENT_RETRY_TOPIC = `${ORDER_CREATED}.${PAYMENT_GROUP}.retry`;
export const PAYMENT_DLT_TOPIC = `${ORDER_CREATED}.${PAYMENT_GROUP}.dlt`;

export const MAX_RETRIES = 2;

export function retryDelayMs(retryCount: number): number {
  return 1000 * 2 ** (retryCount - 1);
}

export type KafkaHeaders = Record<string, unknown>;

export function headerValue(
  headers: KafkaHeaders | undefined,
  name: string,
): string | undefined {
  const value = headers?.[name];
  if (Buffer.isBuffer(value)) {
    return value.toString();
  }
  return typeof value === 'string' ? value : undefined;
}
