// Minimal public types used by the orchestrator. Extend as needed.

export type BlockHeight = number;
export type BlockHeightDelta = number;

export type ExecutionStatusView =
  | { kind: 'Failure'; error?: unknown }
  | { kind: 'SuccessValue'; value?: string }
  | { kind: 'SuccessReceiptId'; id: string }
  | { kind: 'Unknown' };

export interface IndexerExecutionOutcomeWithReceipt {
  execution_outcome: {
    outcome: {
      status: ExecutionStatusView;
      receipt_ids: string[];
      logs: string[];
    };
  };
  receipt: {
    receipt_id: string;
    receiver_id: string;
  };
}

export interface IndexerTransactionWithOutcome {
  transaction: {
    hash: string;
    signer_id: string;
    receiver_id: string;
  };
  outcome: {
    execution_outcome: {
      outcome: {
        receipt_ids: string[];
      };
    };
  };
}

export interface IndexerShard {
  shard_id: number;
  chunk?: {
    transactions: IndexerTransactionWithOutcome[];
  };
  receipt_execution_outcomes: IndexerExecutionOutcomeWithReceipt[];
  state_changes?: unknown;
}

export interface StreamerMessage {
  block: {
    header: {
      height: BlockHeight;
      timestamp_nanosec: string | number;
    };
  };
  shards: IndexerShard[];
}
