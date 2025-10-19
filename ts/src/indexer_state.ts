import type { StreamerMessage } from './types.js';
import type {
  BlockProcessingOptions,
  CompleteTransaction,
  Indexer,
  IncompleteTransaction,
  TransactionReceipt,
} from './indexer.js';

// Internal state machine used by runIndexer
export class IndexerState {
  private pendingTransactions = new Map<string, IncompleteTransaction>();
  private receiptToTx = new Map<string, string>();
  private blocksReceived = 0;

  async processBlock(
    indexer: Indexer,
    message: StreamerMessage,
    options: BlockProcessingOptions,
  ): Promise<void> {
    this.blocksReceived += 1;

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
              block_timestamp_nanosec: String(
                message.block.header.timestamp_nanosec,
              ),
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
                  return this.receipts.every((r) => r!.isSuccessful(true));
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
