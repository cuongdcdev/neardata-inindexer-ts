# inindexer-ts (TypeScript port)

TypeScript port of the `inindexer` Rust library. It preserves the Indexer interface, streaming abstraction, preprocessing, and NEP-297 helpers.

## Quick start

1. Install dev deps

```bash
npm install
```

2. Try examples with the mock fetcher first. Edit the example to import the mock fetcher:

```ts
import { makeMockFetcher } from './examples/mock_fetcher.js';
```

3. Run an example

```bash
npm run example:ft
```

Replace the mock fetcher with a real data source (FastNEAR/NEAR Lake/RPC) returning `StreamerMessage`.

## API surface
- Indexer interface with hooks (processBlock/Transaction/Receipt, onTransaction/onReceipt, finalize)
- runIndexer(indexer, streamer, options)
- MessageStreamer and Provider streamers (serial and parallel)
- MultiIndexer, ParallelMultiIndexer, MapErrorIndexer
- near_utils: NEP-297 helpers, FT/NFT event validators, constants

See `src/index.ts` for exports.
