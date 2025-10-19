import {
    runIndexer,
    defaultOptions,
    AutoContinue,
    deserializeEventLog,
    validateFtEvent,
} from '../src/index.ts';
import type {
    Indexer,
} from '../src/indexer.ts';
import type {
    StreamerMessage,
    IndexerExecutionOutcomeWithReceipt,
    IndexerTransactionWithOutcome,
} from '../src/types.ts';
import {
    NeardataProvider,
    makeNeardataHttpFetcherFromChain,
    fetchLastBlockHeight,
} from '../src/neardata.ts';
import { TESTNET_GENESIS_BLOCK_HEIGHT } from '../src/near_utils.ts';
import { promises as fs } from 'node:fs';
import path from 'node:path';

// Contracts to watch on testnet
const TOKEN_CONTRACT = 'token.cuongdcdev.testnet';
const COINFLIP_CONTRACT = 'coinflip.cuongdcdev.testnet';

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

function safeBase64Json<T = any>(b64: string): T | undefined {
    try {
        const text = Buffer.from(b64, 'base64').toString('utf8');
        return JSON.parse(text);
    } catch {
        return undefined;
    }
}

class WatchBetIndexer implements Indexer<string> {
    // Detect events by reading receipt execution outcome logs (NEAR events live here)
    async processReceipt(receipt: IndexerExecutionOutcomeWithReceipt, _block: StreamerMessage) {
        const contract = receipt.receipt.receiver_id;
        const logs = receipt.execution_outcome.outcome.logs ?? [];

        for (const log of logs) {
            // Try NEP-297 style first
            try {
                const evt = deserializeEventLog<any>(log);

                // FT transfers on TOKEN_CONTRACT
                if (contract === TOKEN_CONTRACT && validateFtEvent(evt, 'ft_transfer')) {
                    for (const t of evt.data as Array<{ old_owner_id: string; new_owner_id: string; amount: string }>) {
                        console.log(`[ft_transfer] ${t.old_owner_id} -> ${t.new_owner_id}: ${t.amount} ${contract}, rid=${receipt.receipt.receipt_id}`);
                    }
                    continue;
                }

                // Coinflip events on COINFLIP_CONTRACT
                if ((evt.event === 'BetPlaced' || evt.event === 'BetSettled')) {
                    console.log(`[${evt.event}]`, JSON.stringify(evt.data));
                    continue;
                }
            } catch {
                // console.error('Raw log:', deserializeEventLog(log));
                console.error('Nothing found, parsed NEP-297 event:', deserializeEventLog(log));
            }

            // Fallback: raw substring match for Coinflip logs (non-NEP-297)
            if ( (log.includes('BetPlaced') || log.includes('BetSettled'))) {
                console.log(`[CoinflipLog] ${log}`);
            }
        }
    }
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Wrap a fetcher with a polite rate limit and 429 backoff to stay under FASTNEAR limits
function withRateLimitFetcher(
    fetcher: ReturnType<typeof makeNeardataHttpFetcherFromChain>,
    opts: { minIntervalMs?: number; nullDelayMs?: number; on429DelayMs?: number; maxRetries?: number } = {},
) {
    const minIntervalMs = opts.minIntervalMs ?? 400; // ~150 req/min < 180 limit
    const nullDelayMs = opts.nullDelayMs ?? 200; // small pause on nulls to avoid hammering
    const on429DelayMs = opts.on429DelayMs ?? 5000;
    const maxRetries = opts.maxRetries ?? 3;
    let lastCall = 0;
    return async (cfg: Parameters<typeof fetcher>[0], height: number) => {
        const now = Date.now();
        const delta = now - lastCall;
        if (delta < minIntervalMs) await sleep(minIntervalMs - delta);
        lastCall = Date.now();

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                const msg = await fetcher(cfg as any, height);
                if (msg === null) {
                    await sleep(nullDelayMs);
                }
                return msg;
            } catch (e: any) {
                const msg = String(e?.message ?? e ?? '');
                if (msg.includes('429') || msg.toLowerCase().includes('rate-limit')) {
                    // Back off then retry
                    await sleep(on429DelayMs);
                    continue;
                }
                throw e;
            }
        }
        // Last try
        return fetcher(cfg as any, height);
    };
}

async function main() {
    const savePath = path.resolve('example_watch_bet_last_block.txt');
    const save = new FileSaveLocation(savePath);
    let start = await save.load();
    if (!Number.isFinite(start)) {
        // Start from the latest finalized block to "tail" the chain
        start = await fetchLastBlockHeight('testnet', 'Final');
    }
    const ac = new AutoContinue(save, start!);
    const range = { kind: 'AutoContinue' as const, auto: ac };

    const indexer = new WatchBetIndexer();
    const baseFetcher = makeNeardataHttpFetcherFromChain('testnet');
    const fetcher = withRateLimitFetcher(baseFetcher, {
        minIntervalMs:10,
        nullDelayMs: 20,
        on429DelayMs: 5000,
        maxRetries: 3,

        //         minIntervalMs: 400,
        // nullDelayMs: 200,
        // on429DelayMs: 5000,
        // maxRetries: 3,
    });
    const streamer = new NeardataProvider({ chain_id: 'testnet', finality: 'Final' }, fetcher as any);
    const options = {
        ...defaultOptions(range),
        genesis_block_height: TESTNET_GENESIS_BLOCK_HEIGHT,
    };

    await runIndexer(indexer, streamer, options);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
