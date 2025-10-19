import { runIndexer, defaultOptions } from '../src/index.ts';
import type { Indexer } from '../src/indexer.ts';
import type { StreamerMessage } from '../src/types.ts';
import { NeardataProvider, type Fetcher, type FetcherConfig } from '../src/neardata.ts';

class DownloadIndexer implements Indexer<string> {
  async processBlock(block: StreamerMessage): Promise<void> {
    const height = block.block.header.height;
    // Save somewhere: here only console
    console.log(`Got block ${height}`);
  }
}

const exampleFetcher: Fetcher = async (_cfg: FetcherConfig, _height: number) => {
  return null; // placeholder
};

async function main() {
  const range = { kind: 'Range' as const, start_inclusive: 100, end_exclusive: 110 };
  await runIndexer(new DownloadIndexer(), new NeardataProvider({ chain_id: 'mainnet' }, exampleFetcher), defaultOptions(range));
}

main().catch((e) => {
  console.error(e);
});
