export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

export const ORDER_CREATED_TOPIC = 'order.created.v1';
export const ORDER_CREATED_PARTITIONS = 3;

export const PAYMENT_SERVICE_GROUP = 'payment-service';
