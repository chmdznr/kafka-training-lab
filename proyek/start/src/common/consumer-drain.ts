import { Injectable, type OnModuleDestroy } from '@nestjs/common';
import type { Consumer } from 'kafkajs';

/**
 * Graceful shutdown helper.
 *
 * kafkajs stops its runner before Nest destroys providers; a manual commit
 * from a handler still in flight is then dropped without an error. This
 * provider runs before the transport closes, pauses fetching, and waits for
 * running handlers so their commits land.
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
