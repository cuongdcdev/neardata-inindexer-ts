import type {
  Indexer,
  CompleteTransaction,
  TransactionReceipt,
  IncompleteTransaction,
} from './indexer.js';
import type { StreamerMessage, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome } from './types.js';

export class MultiIndexer<E = unknown> implements Indexer<E> {
  private indexers: Indexer<E>[];
  constructor(indexers: Indexer<E>[]) {
    this.indexers = indexers;
  }

  getIndexers() {
    return this.indexers;
  }

  async processBlock(block: StreamerMessage) {
    for (const i of this.indexers) if (i.processBlock) await i.processBlock(block);
  }
  async processTransaction(tx: IndexerTransactionWithOutcome, block: StreamerMessage) {
    for (const i of this.indexers)
      if (i.processTransaction) await i.processTransaction(tx, block);
  }
  async processReceipt(r: IndexerExecutionOutcomeWithReceipt, block: StreamerMessage) {
    for (const i of this.indexers) if (i.processReceipt) await i.processReceipt(r, block);
  }
  async onTransaction(tx: CompleteTransaction, block: StreamerMessage) {
    for (const i of this.indexers) if (i.onTransaction) await i.onTransaction(tx, block);
  }
  async onReceipt(r: TransactionReceipt, itx: IncompleteTransaction, block: StreamerMessage) {
    for (const i of this.indexers) if (i.onReceipt) await i.onReceipt(r, itx, block);
  }
  async processBlockEnd(block: StreamerMessage) {
    for (const i of this.indexers) if (i.processBlockEnd) await i.processBlockEnd(block);
  }
  async finalize() {
    for (const i of this.indexers) if (i.finalize) await i.finalize();
  }
}

export function chainIndexers<E = unknown>(a: Indexer<E>, b: Indexer<E>): MultiIndexer<E> {
  if (a instanceof MultiIndexer) return new MultiIndexer([...a.getIndexers(), b]);
  return new MultiIndexer([a, b]);
}

export class MapErrorIndexer<E, E2> implements Indexer<E2> {
  private indexer: Indexer<E>;
  private map: (e: E) => E2;
  constructor(indexer: Indexer<E>, map: (e: E) => E2) {
    this.indexer = indexer;
    this.map = map;
  }

  async processBlock(block: StreamerMessage) {
    if (!this.indexer.processBlock) return;
    try {
      await this.indexer.processBlock(block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async processTransaction(tx: IndexerTransactionWithOutcome, block: StreamerMessage) {
    if (!this.indexer.processTransaction) return;
    try {
      await this.indexer.processTransaction(tx, block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async processReceipt(r: IndexerExecutionOutcomeWithReceipt, block: StreamerMessage) {
    if (!this.indexer.processReceipt) return;
    try {
      await this.indexer.processReceipt(r, block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async onTransaction(tx: CompleteTransaction, block: StreamerMessage) {
    if (!this.indexer.onTransaction) return;
    try {
      await this.indexer.onTransaction(tx, block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async onReceipt(r: TransactionReceipt, itx: IncompleteTransaction, block: StreamerMessage) {
    if (!this.indexer.onReceipt) return;
    try {
      await this.indexer.onReceipt(r, itx, block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async processBlockEnd(block: StreamerMessage) {
    if (!this.indexer.processBlockEnd) return;
    try {
      await this.indexer.processBlockEnd(block);
    } catch (e) {
      throw this.map(e as E);
    }
  }
  async finalize() {
    if (!this.indexer.finalize) return;
    try {
      await this.indexer.finalize();
    } catch (e) {
      throw this.map(e as E);
    }
  }
}

export class ParallelMultiIndexer<E = unknown> implements Indexer<E> {
  private indexers: Indexer<E>[];
  constructor(indexers: Indexer<E>[]) {
    this.indexers = indexers;
  }

  async processBlock(block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.processBlock ? i.processBlock(block) : undefined)),
    );
  }
  async processTransaction(tx: IndexerTransactionWithOutcome, block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.processTransaction ? i.processTransaction(tx, block) : undefined)),
    );
  }
  async processReceipt(r: IndexerExecutionOutcomeWithReceipt, block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.processReceipt ? i.processReceipt(r, block) : undefined)),
    );
  }
  async onTransaction(tx: CompleteTransaction, block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.onTransaction ? i.onTransaction(tx, block) : undefined)),
    );
  }
  async onReceipt(r: TransactionReceipt, itx: IncompleteTransaction, block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.onReceipt ? i.onReceipt(r, itx, block) : undefined)),
    );
  }
  async processBlockEnd(block: StreamerMessage) {
    await Promise.all(
      this.indexers.map((i) => (i.processBlockEnd ? i.processBlockEnd(block) : undefined)),
    );
  }
  async finalize() {
    await Promise.all(this.indexers.map((i) => (i.finalize ? i.finalize() : undefined)));
  }
}
