export interface OrderCreatedData {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
}

export interface PaymentEventData {
  orderId: string;
  amount: number;
  currency: string;
  reason?: string;
}
