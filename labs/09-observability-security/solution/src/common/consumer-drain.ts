import { Injectable, type OnModuleDestroy } from '@nestjs/common';
import type { Consumer } from 'kafkajs';

/**
 * Graceful shutdown helper.
 *
 * `enableShutdownHooks()` closes the consumer when SIGTERM arrives. kafkajs
 * sets its runner to "not running" at that moment, and a manual commit from a
 * handler still in flight is then dropped without an error. This provider runs
 * before the transport closes (onModuleDestroy runs first), pauses fetching,
 * and waits for the running handlers so their commits land. Only after that
 * does Nest disconnect the consumer.
 */
@Injectable()
export class ConsumerDrain implements OnModuleDestroy {
  #consumer: Consumer | undefined;
  #topics = new Set<string>();
  #inFlight = 0;
  #idleWaiters: Array<() => void> = [];

  attach(consumer: Consumer, topic: string): void {
    this.#consumer ??= consumer;
    this.#topics.add(topic);
  }

  enter(): void {
    this.#inFlight += 1;
  }

  leave(): void {
    this.#inFlight -= 1;
    if (this.#inFlight === 0) {
      for (const resolve of this.#idleWaiters.splice(0)) {
        resolve();
      }
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.#consumer) {
      return;
    }
    try {
      this.#consumer.pause([...this.#topics].map((topic) => ({ topic })));
    } catch {
      // The consumer never joined a group; nothing is in flight.
      return;
    }
    if (this.#inFlight === 0) {
      return;
    }
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#idleWaiters.push(resolve);
    await promise;
  }
}
