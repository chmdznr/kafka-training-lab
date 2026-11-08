export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

// Overridable so a shared cluster can run an isolated copy of this lab.
export const ORDER_CREATED_TOPIC =
  process.env.ORDER_CREATED_TOPIC ?? 'order.created.v1';
export const ORDER_CREATED_PARTITIONS = 3;

export const PAYMENT_SERVICE_GROUP =
  process.env.PAYMENT_SERVICE_GROUP ?? 'payment-service';
export const PAYMENT_RETRY_GROUP =
  process.env.PAYMENT_SERVICE_RETRY_GROUP ?? 'payment-service-retry';

// Contract: <topic>.<consumer-group>.retry and <topic>.<consumer-group>.dlt
export const RETRY_TOPIC = `${ORDER_CREATED_TOPIC}.${PAYMENT_SERVICE_GROUP}.retry`;
export const DLT_TOPIC = `${ORDER_CREATED_TOPIC}.${PAYMENT_SERVICE_GROUP}.dlt`;

// Attempts on the retry topic before the message goes to the DLT.
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
