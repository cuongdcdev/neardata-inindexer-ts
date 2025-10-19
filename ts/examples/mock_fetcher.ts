import type { Fetcher, FetcherConfig } from '../src/neardata.ts';
import type { StreamerMessage, ExecutionStatusView } from '../src/types.ts';

// A tiny in-memory mock that returns 3 consecutive blocks with one FT transfer log
export const makeMockFetcher = (startHeight = 100): Fetcher => {
  return async (_cfg: FetcherConfig, height: number): Promise<StreamerMessage | null> => {
    if (height < startHeight || height >= startHeight + 3) return null;

    const status: ExecutionStatusView = { kind: 'SuccessValue', value: '' };
    const receipt_id = `rid-${height}`;

    const ftLog =
      'EVENT_JSON:' +
      JSON.stringify({
        standard: 'nep141',
        version: '1.0.0',
        event: 'ft_transfer',
        data: [
          {
            old_owner_id: 'alice.near',
            new_owner_id: 'bob.near',
            amount: '1000000000000000000000',
          },
        ],
      });

    return {
      block: {
        header: {
          height,
          timestamp_nanosec: String(1_700_000_000_000_000_000n + BigInt(height)),
        },
      },
      shards: [
        {
          shard_id: 0,
          chunk: {
            transactions: [
              {
                transaction: {
                  hash: `tx-${height}`,
                  signer_id: 'signer.near',
                  receiver_id: 'token.near',
                },
                outcome: {
                  execution_outcome: {
                    outcome: {
                      receipt_ids: [receipt_id],
                    },
                  },
                },
              },
            ],
          },
          receipt_execution_outcomes: [
            {
              receipt: { receipt_id, receiver_id: 'token.near' },
              execution_outcome: {
                outcome: { status, receipt_ids: [], logs: [ftLog] },
              },
            },
          ],
        },
      ],
    };
  };
};
