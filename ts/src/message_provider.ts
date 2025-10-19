import type { BlockHeight, StreamerMessage } from './types.js';
import type { MessageStreamer } from './indexer.js';

export interface MessageProvider<E = unknown> {
  getMessage(blockHeight: BlockHeight): Promise<StreamerMessage | null>;
}

// Simple async queue to bridge producer/consumer with backpressure
class AsyncQueue<T> {
  private values: (T | undefined)[] = [];
  private resolvers: ((value: IteratorResult<T>) => void)[] = [];
  private done = false;

  push(v: T) {
    if (this.done) return;
    if (this.resolvers.length) {
      const r = this.resolvers.shift()!;
      r({ value: v, done: false });
    } else {
      this.values.push(v);
    }
  }

  close() {
    this.done = true;
    for (const r of this.resolvers) r({ value: undefined as any, done: true });
    this.resolvers = [];
  }

  async *iterator(): AsyncGenerator<T> {
    while (true) {
      if (this.values.length) {
        const v = this.values.shift()!;
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        yield v!;
        continue;
      }
      if (this.done) return;
      const v = await new Promise<IteratorResult<T>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (v.done) return;
      yield v.value as T;
    }
  }
}

export class ProviderStreamer<P extends MessageProvider> implements MessageStreamer {
  private provider: P;
  private bufferSize: number;
  constructor(provider: P, bufferSize = 100) {
    this.provider = provider;
    this.bufferSize = bufferSize;
  }

  async stream(first: BlockHeight, lastExclusive?: BlockHeight) {
    const queue = new AsyncQueue<StreamerMessage>();
    const end = lastExclusive ?? Number.MAX_SAFE_INTEGER;

    const handle = (async () => {
      for (let h = first; h < end; h++) {
        let retries = 0;
        // retry loop
        // eslint-disable-next-line no-constant-condition
        while (true) {
          try {
            const msg = await this.provider.getMessage(h);
            if (msg) queue.push(msg);
            break; // success or fork skip
          } catch (err) {
            retries++;
            // Backoff 1s
            await new Promise((r) => setTimeout(r, 1000));
          }
        }
      }
      queue.close();
    })();

    return { handle, receiver: queue.iterator() };
  }
}

export class ParallelProviderStreamer<P extends MessageProvider> implements MessageStreamer {
  private provider: P;
  private workers: number;
  constructor(provider: P, workers = 4) {
    this.provider = provider;
    this.workers = workers;
  }

  async stream(first: BlockHeight, lastExclusive?: BlockHeight) {
    const queue = new AsyncQueue<StreamerMessage>();
    const end = lastExclusive ?? Number.MAX_SAFE_INTEGER;

    const handle = (async () => {
      let nextToEmit = first;
      const results = new Map<BlockHeight, StreamerMessage | null>();
      let active = 0;
      let scheduled = first;

      const scheduleOne = async (h: BlockHeight) => {
        active++;
        try {
          let retries = 0;
          // eslint-disable-next-line no-constant-condition
          while (true) {
            try {
              const msg = await this.provider.getMessage(h);
              results.set(h, msg);
              break;
            } catch {
              retries++;
              await new Promise((r) => setTimeout(r, 1000));
            }
          }
        } finally {
          active--;
          // attempt to drain in order
          while (true) {
            const have = results.has(nextToEmit);
            if (!have) break;
            const msg = results.get(nextToEmit)!;
            results.delete(nextToEmit);
            if (msg) queue.push(msg);
            nextToEmit++;
          }
          // Schedule next if room
          if (scheduled < end) {
            const h2 = scheduled++;
            // fire and forget
            scheduleOne(h2);
          } else if (active === 0 && results.size === 0) {
            queue.close();
          }
        }
      };

      // prime workers
      for (let i = 0; i < this.workers && scheduled < end; i++) {
        const h = scheduled++;
        // fire and forget
        scheduleOne(h);
      }
    })();

    return { handle, receiver: queue.iterator() };
  }
}
