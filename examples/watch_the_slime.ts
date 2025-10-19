import { runIndexer, defaultOptions } from '../src/index.ts';
import type { Indexer } from '../src/indexer.ts';
import type { StreamerMessage, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome } from '../src/types.ts';
import { NeardataProvider, type Fetcher, type FetcherConfig } from '../src/neardata.ts';

class WatcherIndexer implements Indexer<string> {
  constructor(private trackedAccount: string) {}

  async onTransaction(
    transaction: { transaction: IndexerTransactionWithOutcome } & { receipts: any[]; allReceiptsSuccessful(): boolean },
    _block: StreamerMessage,
  ): Promise<void> {
    if (transaction.transaction.transaction.signer_id === this.trackedAccount) {
      console.info(
        `Found transaction: https://pikespeak.ai/transaction-viewer/${transaction.transaction.transaction.hash}`,
      );
    }
  }
}

const exampleFetcher: Fetcher = async (_cfg: FetcherConfig, _height: number) => {
  return null; // placeholder
};

async function main() {
  const range = { kind: 'Range' as const, start_inclusive: 112_037_807, end_exclusive: 112_037_810 };
  await runIndexer(
    new WatcherIndexer('slimedragon.near'),
    new NeardataProvider({ chain_id: 'mainnet' }, exampleFetcher),
    defaultOptions(range),
  );
}

main().catch((e) => console.error(e));
