import type {
  ExecutionStatusView,
  IndexerExecutionOutcomeWithReceipt,
} from './types.js';

export const MAINNET_GENESIS_BLOCK_HEIGHT = 9_820_210;
export const TESTNET_GENESIS_BLOCK_HEIGHT = 100_000_000;

export function isReceiptSuccessful(
  receipt: IndexerExecutionOutcomeWithReceipt,
): boolean | undefined {
  const status = receipt.execution_outcome.outcome.status;
  switch (status.kind) {
    case 'Failure':
      return false;
    case 'SuccessValue':
    case 'SuccessReceiptId':
      return true;
    case 'Unknown':
      return undefined;
    default:
      return undefined as never;
  }
}

// NEP-297 log container
export interface EventLogData<T> {
  standard: string;
  version: string;
  event: string;
  data: T;
}

export function deserializeEventLog<T>(log: string): EventLogData<T> {
  const prefix = 'EVENT_JSON:';
  if (!log.startsWith(prefix)) throw new Error("No 'EVENT_JSON:' prefix");
  return JSON.parse(log.slice(prefix.length));
}

export const NEP141_EVENT_STANDARD_STRING = 'nep141';
export const NEP171_EVENT_STANDARD_STRING = 'nep171';

// FT events
export interface FtMintEvent {
  owner_id: string;
  amount: string; // as decimal string
  memo?: string | null;
}
export type FtMintLog = FtMintEvent[];

export interface FtBurnEvent {
  owner_id: string;
  amount: string;
  memo?: string | null;
}
export type FtBurnLog = FtBurnEvent[];

export interface FtTransferEvent {
  old_owner_id: string;
  new_owner_id: string;
  amount: string;
  memo?: string | null;
}
export type FtTransferLog = FtTransferEvent[];

export function validateFtEvent(
  evt: EventLogData<FtMintLog | FtBurnLog | FtTransferLog>,
  eventName: 'ft_mint' | 'ft_burn' | 'ft_transfer',
): boolean {
  try {
    const [majorStr] = evt.version.split('.');
    const major = Number(majorStr);
    return (
      evt.standard === NEP141_EVENT_STANDARD_STRING &&
      evt.event === eventName &&
      major === 1
    );
  } catch {
    return false;
  }
}

export function deserializeTknFarmTransferLog(log: string): FtTransferLog {
  // "Transfer <amount> from <old> to <new>"
  if (!log.startsWith('Transfer ')) throw new Error("Log doesn't start with 'Transfer '");
  const tail = log.slice('Transfer '.length);
  const parts1 = tail.split(' from ');
  if (parts1.length !== 2) throw new Error("Log doesn't contain ' from '");
  const amount = parts1[0];
  const parts2 = parts1[1].split(' to ');
  if (parts2.length !== 2) throw new Error("Log doesn't contain ' to '");
  const old_owner_id = parts2[0];
  const new_owner_id = parts2[1];
  return [{ old_owner_id, new_owner_id, amount }];
}

// NFT events
export interface NftMintEvent {
  owner_id: string;
  token_ids: string[];
  memo?: string | null;
}
export type NftMintLog = NftMintEvent[];

export interface NftBurnEvent {
  owner_id: string;
  authorized_id?: string | null;
  token_ids: string[];
  memo?: string | null;
}
export type NftBurnLog = NftBurnEvent[];

export interface NftTransferEvent {
  authorized_id?: string | null;
  old_owner_id: string;
  new_owner_id: string;
  token_ids: string[];
  memo?: string | null;
}
export type NftTransferLog = NftTransferEvent[];

export interface NftContractMetadataUpdateEvent {
  memo?: string | null;
}
export type NftContractMetadataUpdateLog = NftContractMetadataUpdateEvent[];

export function validateNftEvent(
  evt: EventLogData<
    NftMintLog | NftBurnLog | NftTransferLog | NftContractMetadataUpdateLog
  >,
  eventName: 'nft_mint' | 'nft_burn' | 'nft_transfer' | 'contract_metadata_update',
): boolean {
  try {
    const [majorStr] = evt.version.split('.');
    const major = Number(majorStr);
    return (
      evt.standard === NEP171_EVENT_STANDARD_STRING &&
      evt.event === eventName &&
      major === 1
    );
  } catch {
    return false;
  }
}
