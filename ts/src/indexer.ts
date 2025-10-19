import { MAINNET_GENESIS_BLOCK_HEIGHT, isReceiptSuccessful } from './near_utils.ts';
import type {
  BlockHeight,
  BlockHeightDelta,
  IndexerExecutionOutcomeWithReceipt,
  IndexerTransactionWithOutcome,
  StreamerMessage,
} from './types.ts';
// Internal state machine (inlined to avoid circular imports)

export interface Indexer<Err = unknown> {
  processBlock?(block: StreamerMessage): Promise<void>;
  processTransaction?(
    transaction: IndexerTransactionWithOutcome,
    block: StreamerMessage,
  ): Promise<void>;
  processReceipt?(
    receipt: IndexerExecutionOutcomeWithReceipt,
    block: StreamerMessage,
  ): Promise<void>;
  onTransaction?(transaction: CompleteTransaction, block: StreamerMessage): Promise<void>;
  onReceipt?(
    receipt: TransactionReceipt,
    tx: IncompleteTransaction,
    block: StreamerMessage,
  ): Promise<void>;
  processBlockEnd?(block: StreamerMessage): Promise<void>;
  finalize?(): Promise<void>;
}

export interface MessageStreamer<Err = unknown> {
  stream(
    first_block_inclusive: BlockHeight,
    last_block_exclusive?: BlockHeight,
  ): Promise<{
    handle: Promise<void>;
    receiver: AsyncGenerator<StreamerMessage>;
  }>;
}

export interface CompleteTransaction {
  transaction: IndexerTransactionWithOutcome;
  receipts: TransactionReceipt[];
  allReceiptsSuccessful(): boolean;
}

export interface IncompleteTransaction {
  transaction: IndexerTransactionWithOutcome;
  receipts: Map<string, TransactionReceipt | null>;
}

export interface TransactionReceipt {
  receipt: IndexerExecutionOutcomeWithReceipt;
  block_height: BlockHeight;
  block_timestamp_nanosec: string; // serialized string
  isSuccessful(ifUnknown: boolean): boolean;
}

export type BlockRange =
  | { kind: 'Range'; start_inclusive: BlockHeight; end_exclusive?: BlockHeight }
  | { kind: 'AutoContinue'; auto: AutoContinue };

export interface PreprocessTransactionsSettings {
  prefetch_blocks: number; // default 100
  postfetch_blocks: number; // default 100
}

export interface IndexerOptions {
  stop_on_error: boolean;
  range: BlockRange;
  preprocess_transactions?: PreprocessTransactionsSettings;
  genesis_block_height: BlockHeight;
  ctrl_c_handler: boolean;
}

export interface SaveLocation {
  load(): Promise<BlockHeight | undefined>;
  save(height: BlockHeight): Promise<void>;
}

export type AutoContinueEnd =
  | { kind: 'Height'; height: BlockHeight }
  | { kind: 'Count'; count: BlockHeightDelta }
  | { kind: 'Infinite' };

export class AutoContinue {
  public save_location: SaveLocation;
  public start_height_if_does_not_exist: BlockHeight;
  public end: AutoContinueEnd;
  constructor(
    save_location: SaveLocation,
    start_height_if_does_not_exist: BlockHeight = MAINNET_GENESIS_BLOCK_HEIGHT,
    end: AutoContinueEnd = { kind: 'Infinite' },
  ) {
    this.save_location = save_location;
    this.start_height_if_does_not_exist = start_height_if_does_not_exist;
    this.end = end;
  }

  async getStartBlock(): Promise<BlockHeight> {
    return (await this.save_location.load()) ?? this.start_height_if_does_not_exist;
  }

  async range(): Promise<{ start: BlockHeight; end: BlockHeight }> {
    const start = await this.getStartBlock();
    const end =
      this.end.kind === 'Height'
        ? this.end.height
        : this.end.kind === 'Count'
        ? start + this.end.count
        : Number.MAX_SAFE_INTEGER;
    return { start, end };
  }

  // Acts as a PostProcessor: save the next block height after successfully processing a block
  async afterBlock(
    block: StreamerMessage,
    _options: BlockProcessingOptions,
    stopping: boolean,
  ): Promise<void> {
    // Avoid saving while stopping due to postfetch window
    if (!stopping) {
      await this.save_location.save(block.block.header.height + 1);
    }
  }
}

export interface PostProcessor {
  afterBlock(
    block: StreamerMessage,
    options: BlockProcessingOptions,
    stopping: boolean,
  ): Promise<void>;
}

export interface BlockProcessingOptions {
  height: BlockHeight;
  preprocess: boolean;
  preprocess_new_transactions: boolean;
  handle_raw_events: boolean;
  handle_preprocessed_transactions_by_indexer: boolean;
}

export class DefaultTransactionReceipt implements TransactionReceipt {
  public receipt: IndexerExecutionOutcomeWithReceipt;
  public block_height: BlockHeight;
  public block_timestamp_nanosec: string;
  constructor(
    receipt: IndexerExecutionOutcomeWithReceipt,
    block_height: BlockHeight,
    block_timestamp_nanosec: string,
  ) {
    this.receipt = receipt;
    this.block_height = block_height;
    this.block_timestamp_nanosec = block_timestamp_nanosec;
  }
  isSuccessful(ifUnknown: boolean) {
    const r = isReceiptSuccessful(this.receipt);
    return r === undefined ? ifUnknown : r;
  }
}

export class DefaultCompleteTransaction implements CompleteTransaction {
  public transaction: IndexerTransactionWithOutcome;
  public receipts: TransactionReceipt[];
  constructor(
    transaction: IndexerTransactionWithOutcome,
    receipts: TransactionReceipt[],
  ) {
    this.transaction = transaction;
    this.receipts = receipts;
  }
  allReceiptsSuccessful(): boolean {
    return this.receipts.every((r) => r.isSuccessful(true));
  }
}

export async function runIndexer<I extends Indexer, S extends MessageStreamer>(
  indexer: I,
  streamer: S,
  options: IndexerOptions,
): Promise<void> {
  const indexerState = new InternalIndexerState();

  const { start_block_height, end_block_height, post_processor } = await (async () => {
    if (options.range.kind === 'Range') {
      return {
        start_block_height: options.range.start_inclusive,
        end_block_height: options.range.end_exclusive,
        post_processor: undefined as PostProcessor | undefined,
      };
    } else {
      const r = await options.range.auto.range();
      return {
        start_block_height: r.start,
        end_block_height: r.end === Number.MAX_SAFE_INTEGER ? undefined : r.end,
        post_processor: (options.range.auto as unknown as PostProcessor) as PostProcessor,
      };
    }
  })();

  const preprocess = options.preprocess_transactions;
  const prefetch_blocks = preprocess?.prefetch_blocks ?? 0;
  const postfetch_blocks = preprocess?.postfetch_blocks ?? 0;

  const prefetchRangeStart = Math.max(
    start_block_height - prefetch_blocks,
    options.genesis_block_height,
  );
  const prefetchRange = { start: prefetchRangeStart, end: start_block_height };
  const postfetchRange = {
    start: end_block_height ?? Number.MAX_SAFE_INTEGER,
    end: end_block_height ? end_block_height + postfetch_blocks : Number.MAX_SAFE_INTEGER,
  };

  const startWithPrefetch = Math.max(options.genesis_block_height, start_block_height - prefetch_blocks);
  const endWithPostfetch = end_block_height ? end_block_height + postfetch_blocks : undefined;

  const { handle, receiver } = await streamer.stream(startWithPrefetch, endWithPostfetch);

  // no-op placeholder for start hook parity

  let hasSentPostfetchMessage = false;
  try {
    for await (const message of receiver) {
      const inPrefetch = message.block.header.height >= prefetchRange.start &&
        message.block.header.height < prefetchRange.end;
      const inPostfetch = message.block.header.height >= postfetchRange.start &&
        message.block.header.height < postfetchRange.end;

      if (inPostfetch && !hasSentPostfetchMessage) {
        hasSentPostfetchMessage = true;
        // logging hook; user can log externally
      }

      const processingOptions: BlockProcessingOptions = {
        height: message.block.header.height,
        preprocess: !!preprocess,
        preprocess_new_transactions: !inPostfetch,
        handle_raw_events: !inPostfetch && !inPrefetch,
        handle_preprocessed_transactions_by_indexer: !inPrefetch,
      };

      try {
        await indexerState.processBlock(indexer, message, processingOptions);
      } catch (e) {
        if (options.stop_on_error) throw e;
      }

      if (!inPrefetch && post_processor) {
        await post_processor.afterBlock(message, processingOptions, inPostfetch);
      }
    }
  } finally {
    await handle; // wait join
    if (indexer.finalize) await indexer.finalize();
  }
}

export const DefaultPreprocessSettings: PreprocessTransactionsSettings = {
  prefetch_blocks: 100,
  postfetch_blocks: 100,
};

export function defaultOptions(range: BlockRange): IndexerOptions {
  return {
    stop_on_error: false,
    range,
    preprocess_transactions: undefined,
    genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
    ctrl_c_handler: true,
  };
}

// Implementation of the state machine, ported from Rust's IndexerState
class InternalIndexerState {
  private pendingTransactions = new Map<string, IncompleteTransaction>();
  private receiptToTx = new Map<string, string>();

  async processBlock(
    indexer: Indexer,
    message: StreamerMessage,
    options: BlockProcessingOptions,
  ): Promise<void> {
    if (indexer.processBlock && options.handle_raw_events) {
      await indexer.processBlock(message);
    }

    // Process transactions
    for (const shard of message.shards) {
      const chunk = shard.chunk;
      if (!chunk) continue;
      for (const tx of chunk.transactions) {
        if (options.preprocess) {
          for (const r of tx.outcome.execution_outcome.outcome.receipt_ids) {
            this.receiptToTx.set(r, tx.transaction.hash);
          }
          if (options.preprocess_new_transactions) {
            const receipts = new Map<string, TransactionReceipt | null>();
            for (const r of tx.outcome.execution_outcome.outcome.receipt_ids) {
              receipts.set(r, null);
            }
            this.pendingTransactions.set(tx.transaction.hash, {
              transaction: tx,
              receipts,
            });
          }
        }
        if (indexer.processTransaction && options.handle_raw_events) {
          await indexer.processTransaction(tx, message);
        }
      }
    }

    // Process receipts
    for (const shard of message.shards) {
      for (const receipt of shard.receipt_execution_outcomes) {
        const txId = this.receiptToTx.get(receipt.receipt.receipt_id);
        if (txId && options.preprocess) {
          const incomplete = this.pendingTransactions.get(txId);
          if (incomplete) {
            const processed: TransactionReceipt = {
              receipt,
              block_height: message.block.header.height,
              block_timestamp_nanosec: String(message.block.header.timestamp_nanosec),
              isSuccessful(ifUnknown: boolean) {
                const status = this.receipt.execution_outcome.outcome.status;
                switch (status.kind) {
                  case 'Failure':
                    return false;
                  case 'SuccessValue':
                  case 'SuccessReceiptId':
                    return true;
                  case 'Unknown':
                    return ifUnknown;
                  default:
                    return ifUnknown as never;
                }
              },
            };
            if (indexer.onReceipt && options.handle_preprocessed_transactions_by_indexer) {
              await indexer.onReceipt(processed, incomplete, message);
            }
            incomplete.receipts.set(receipt.receipt.receipt_id, processed);
            for (const newReceiptId of receipt.execution_outcome.outcome.receipt_ids) {
              this.receiptToTx.set(newReceiptId, txId);
              if (!incomplete.receipts.has(newReceiptId)) {
                incomplete.receipts.set(newReceiptId, null);
              }
            }

            // Try to complete
            const all = Array.from(incomplete.receipts.values());
            if (all.every((r) => r !== null)) {
              this.pendingTransactions.delete(txId);
              const complete: CompleteTransaction = {
                transaction: incomplete.transaction,
                receipts: all as TransactionReceipt[],
                allReceiptsSuccessful() {
                  return this.receipts.every((r) => r.isSuccessful(true));
                },
              };
              if (indexer.onTransaction && options.handle_preprocessed_transactions_by_indexer) {
                await indexer.onTransaction(complete, message);
              }
            }
          }
        }

        if (indexer.processReceipt && options.handle_raw_events) {
          await indexer.processReceipt(receipt, message);
        }
      }
    }

    if (indexer.processBlockEnd) {
      await indexer.processBlockEnd(message);
    }
  }
}
