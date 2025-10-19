import {
  runIndexer,
  defaultOptions,
  AutoContinue,
  NEP141_EVENT_STANDARD_STRING,
  deserializeEventLog,
  validateFtEvent,
  deserializeTknFarmTransferLog,
} from '../src/index.ts';
import type { Indexer } from '../src/indexer.ts';
import type { StreamerMessage, IndexerExecutionOutcomeWithReceipt } from '../src/types.ts';
import { NeardataProvider, type Fetcher, fetchFirstBlockHeight, makeNeardataHttpFetcherFromChain } from '../src/neardata.ts';
import { MAINNET_GENESIS_BLOCK_HEIGHT } from '../src/near_utils.ts';
import { promises as fs } from 'node:fs';
import path from 'node:path';

class FileSaveLocation {
  private file: string;
  constructor(file: string) {
    this.file = file;
  }
  async load() {
    try {
      const s = await fs.readFile(this.file, 'utf8');
      return Number(s.trim());
    } catch {
      return undefined;
    }
  }
  async save(h: number) {
    await fs.writeFile(this.file, String(h), 'utf8');
  }
}

class FtTransferIndexer implements Indexer<string> {
  async processReceipt(receipt: IndexerExecutionOutcomeWithReceipt, _block: StreamerMessage) {
    const status = receipt.execution_outcome.outcome.status;
    if (status.kind === 'Failure') return;
    const token_id = receipt.receipt.receiver_id;
    const logs = receipt.execution_outcome.outcome.logs ?? [];
    const transfers: { old_owner_id: string; new_owner_id: string; amount: string }[] = [];
    for (const log of logs) {
      try {
        const evt = deserializeEventLog<any>(log);
        if (validateFtEvent(evt, 'ft_transfer')) {
          transfers.push(...evt.data);
          continue;
        }
      } catch {}
      try {
        const arr = deserializeTknFarmTransferLog(log);
        transfers.push(...arr);
      } catch {}
    }
    for (const t of transfers) {
      console.info(
        `${t.old_owner_id} --> ${t.new_owner_id}: ${t.amount} ${token_id}, https://nearblocks.io?query=${receipt.receipt.receipt_id}`,
      );
    }
  }
}

// Choose ONE: mock fetcher for local demo, or real HTTP fetcher.
// 1) Mock fetcher (runs instantly with synthetic data):
// const exampleFetcher: Fetcher = makeMockFetcher(139_770_436);
// 2) Real HTTP fetcher hitting neardata.xyz official /v0 endpoints:
const exampleFetcher: Fetcher = makeNeardataHttpFetcherFromChain('mainnet');
// For testnet, use: makeNeardataHttpFetcherFromChain('testnet')
// const exampleFetcher: Fetcher = makeMockFetcher(139_770_436);

async function main() {
  const savePath = path.resolve('example_ft_transfers_last_block.txt');
  const save = new FileSaveLocation(savePath);
  let start = await save.load();
  if (!Number.isFinite(start)) {
    // If no saved state, ask the server for the first available block height (fast and correct for the chosen chain)
    start = await fetchFirstBlockHeight('mainnet');
  }
  const ac = new AutoContinue(save, start!);
  const range = { kind: 'AutoContinue' as const, auto: ac };

  const indexer = new FtTransferIndexer();
  const streamer = new NeardataProvider({ chain_id: 'mainnet', finality: 'Final' }, exampleFetcher);
  const options = {
    ...defaultOptions(range),
    genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
  };

  await runIndexer(indexer, streamer, options);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
