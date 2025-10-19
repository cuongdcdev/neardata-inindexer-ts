import type { BlockHeight, StreamerMessage } from './types.js';
import type { MessageStreamer } from './indexer.js';

export type Finality = 'Final' | 'Optimistic';
export type ChainId = 'mainnet' | 'testnet';

export interface FetcherConfig {
  chain_id: ChainId;
  finality?: Finality;
  user_agent?: string;
}

export type Fetcher = (
  cfg: FetcherConfig,
  height: BlockHeight,
) => Promise<StreamerMessage | null>;

export class NeardataProvider implements MessageStreamer {
  private cfg: FetcherConfig;
  private fetcher: Fetcher;
  constructor(cfg: FetcherConfig, fetcher: Fetcher) {
    this.cfg = cfg;
    this.fetcher = fetcher;
  }
  static mainnet(fetcher: Fetcher) {
    return new NeardataProvider({ chain_id: 'mainnet', finality: 'Final' }, fetcher);
  }
  static testnet(fetcher: Fetcher) {
    return new NeardataProvider({ chain_id: 'testnet', finality: 'Final' }, fetcher);
  }

  async stream(first: BlockHeight, lastExclusive?: BlockHeight) {
    // Wrap a fetcher into the generic MessageStreamer contract
    async function* gen(this: NeardataProvider) {
      const end = lastExclusive ?? Number.MAX_SAFE_INTEGER;
      for (let h = first; h < end; h++) {
        const msg = await this.fetcher(this.cfg, h);
        if (msg) yield msg;
      }
    }
    const receiver = gen.call(this);
    const handle = Promise.resolve();
    return { handle, receiver };
  }
}

// --- HTTP fetcher helpers ---

export function baseUrlForChain(chain: ChainId): string {
  return chain === 'mainnet'
    ? 'https://mainnet.neardata.xyz/'
    : 'https://testnet.neardata.xyz/';
}

export interface HttpFetcherOptions {
  baseUrl?: string; // defaults to baseUrlForChain(cfg.chain_id)
  headers?: Record<string, string>;
  // Build the path segment given a height. Common variants include:
  // - (h, finality) => `v0/block/${h}` (Final) or `v0/block_opt/${h}` (Optimistic)
  pathBuilder?: (height: BlockHeight) => string;
  // If the endpoint JSON shape differs from StreamerMessage,
  // provide a mapper to convert JSON -> StreamerMessage
  map?: (json: any) => StreamerMessage;
}

export function makeNeardataHttpFetcher(opts: HttpFetcherOptions = {}): Fetcher {
  const {
    baseUrl,
    headers,
    pathBuilder,
    map,
  } = opts;

  return async (cfg: FetcherConfig, height: BlockHeight) => {
    const base = (baseUrl ?? baseUrlForChain(cfg.chain_id)).replace(/\/$/, '');
    const defaultPath = cfg.finality === 'Optimistic' ? `v0/block_opt/${height}` : `v0/block/${height}`;
    const url = `${base}/${(pathBuilder ? pathBuilder(height) : defaultPath)}`;
    const fetchFn: any = (globalThis as any).fetch;
    if (!fetchFn) throw new Error('global fetch not available (Node 18+ required)');
    const hdrs = { ...(headers ?? {}), ...(cfg.user_agent ? { 'user-agent': cfg.user_agent } : {}) } as any;
    const res = await fetchFn(url, { headers: hdrs });
    if (res.status === 404) return null;
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`Neardata fetch failed ${res.status}: ${text}`);
    }
    const json = await res.json();
    return map ? map(json) : (json as StreamerMessage);
  };
}

export function makeNeardataHttpFetcherFromChain(
  chain: ChainId,
  options: Omit<HttpFetcherOptions, 'baseUrl'> = {},
): Fetcher {
  return makeNeardataHttpFetcher({ baseUrl: baseUrlForChain(chain), ...options });
}

// Try multiple path patterns (e.g., ['streamer/block', 'block', 'blocks']) and return the first success
export function makeNeardataHttpFetcherMulti(
  chain: ChainId,
  pathPrefixes: string[] = ['streamer/block', 'block', 'blocks'],
  options: Omit<HttpFetcherOptions, 'baseUrl' | 'pathBuilder'> = {},
): Fetcher {
  const base = baseUrlForChain(chain).replace(/\/$/, '');
  const { headers, map } = options;
  return async (_cfg: FetcherConfig, height: BlockHeight) => {
    const fetchFn: any = (globalThis as any).fetch;
    if (!fetchFn) throw new Error('global fetch not available (Node 18+ required)');
    let lastErr: any;
    for (const prefix of pathPrefixes) {
      const url = `${base}/${prefix.replace(/\/$/, '')}/${height}`;
      try {
        const res = await fetchFn(url, { headers });
        if (res.status === 404) {
          continue; // try next pattern
        }
        if (!res.ok) {
          const text = await res.text().catch(() => '');
          throw new Error(`Neardata fetch failed ${res.status}: ${text}`);
        }
        const json = await res.json();
        return map ? map(json) : (json as StreamerMessage);
      } catch (e) {
        lastErr = e;
      }
    }
    if (lastErr) {
      // If all attempts error (but not 404), surface the last error
      throw lastErr;
    }
    return null;
  };
}

// Helpers to query starting points according to server docs
export async function fetchFirstBlockHeight(chain: ChainId, headers?: Record<string, string>): Promise<number> {
  const base = baseUrlForChain(chain).replace(/\/$/, '');
  const fetchFn: any = (globalThis as any).fetch;
  if (!fetchFn) throw new Error('global fetch not available (Node 18+ required)');
  const res = await fetchFn(`${base}/v0/first_block`, { headers, redirect: 'follow' as any });
  if (!res.ok) throw new Error(`first_block failed ${res.status}`);
  const json = await res.json();
  // Expect json.block.header.height
  return json?.block?.header?.height ?? Number(json.height ?? json);
}

export async function fetchLastBlockHeight(
  chain: ChainId,
  finality: Finality = 'Final',
  headers?: Record<string, string>,
): Promise<number> {
  const base = baseUrlForChain(chain).replace(/\/$/, '');
  const path = finality === 'Optimistic' ? 'v0/last_block/optimistic' : 'v0/last_block/final';
  const fetchFn: any = (globalThis as any).fetch;
  if (!fetchFn) throw new Error('global fetch not available (Node 18+ required)');
  const res = await fetchFn(`${base}/${path}`, { headers, redirect: 'follow' as any });
  if (!res.ok) throw new Error(`last_block failed ${res.status}`);
  const json = await res.json();
  return json?.block?.header?.height ?? Number(json.height ?? json);
}
