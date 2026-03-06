/**
 * Read-only MultiversX chain client. Uses the public API (no signing, no sending tx).
 * Access from handler context as context.client.
 * Wraps with a DB-backed cache so reindexes avoid repeated API calls.
 */

import { eq } from "drizzle-orm";
import { MULTIVERSX_API_URL } from "../constants";
import { multiverseChainCache } from "../schema/internal";
import type { IndexerDb } from "./db";

function normalizeUrl(url: string): string {
  return url.replace(/\/$/, "");
}

function cacheKey(sourceId: string, method: string, ...parts: string[]): string {
  return `${method}:${sourceId}:${parts.join(":")}`;
}

async function cacheGet(db: IndexerDb, key: string): Promise<string | null> {
  const rows = await db
    .select({ value: multiverseChainCache.value })
    .from(multiverseChainCache)
    .where(eq(multiverseChainCache.key, key))
    .limit(1);
  return rows[0]?.value ?? null;
}

async function cacheSet(db: IndexerDb, key: string, value: string, method: string): Promise<void> {
  await db.insert(multiverseChainCache).values({ key, value, method }).onConflictDoUpdate({
    target: multiverseChainCache.key,
    set: { value, method },
  });
}

export interface AccountOnChain {
  address: string;
  balance: string;
  nonce: number;
  code?: string;
  codeHash?: string | null;
  rootHash?: string | null;
  [key: string]: unknown;
}

export interface TransactionOnChain {
  txHash: string;
  sender: string;
  receiver: string;
  value: string;
  data?: string | null;
  blockInfo?: { hash?: string; nonce?: number; round?: number };
  [key: string]: unknown;
}

export interface ContractQueryResult {
  returnData: string[];
  returnCode: string;
  returnMessage?: string;
  gasUsed?: number;
  [key: string]: unknown;
}

/**
 * Read-only chain reader: account, transaction, and contract VM query.
 */
export interface ChainReader {
  /** Get account state (balance, nonce, code, etc.). */
  getAccount(address: string): Promise<AccountOnChain | null>;
  /** Get transaction by hash. */
  getTransaction(txHash: string): Promise<TransactionOnChain | null>;
  /**
   * Query a smart contract view/pure function (read-only).
   * @param contractAddress - Contract bech32 address
   * @param functionName - Function name (e.g. "getRewards")
   * @param args - Optional hex-encoded arguments (without 0x prefix)
   */
  queryContract(
    contractAddress: string,
    functionName: string,
    args?: string[],
  ): Promise<ContractQueryResult>;
}

export function createChainClient(apiBaseUrl: string = MULTIVERSX_API_URL): ChainReader {
  const base = normalizeUrl(apiBaseUrl);

  return {
    async getAccount(address: string): Promise<AccountOnChain | null> {
      try {
        const res = await fetch(`${base}/accounts/${encodeURIComponent(address)}`);
        if (!res.ok) return null;
        return (await res.json()) as AccountOnChain;
      } catch {
        return null;
      }
    },

    async getTransaction(txHash: string): Promise<TransactionOnChain | null> {
      try {
        const res = await fetch(`${base}/transactions/${encodeURIComponent(txHash)}`);
        if (!res.ok) return null;
        return (await res.json()) as TransactionOnChain;
      } catch {
        return null;
      }
    },

    async queryContract(
      contractAddress: string,
      functionName: string,
      args: string[] = [],
    ): Promise<ContractQueryResult> {
      const res = await fetch(`${base}/vm-values/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          scAddress: contractAddress,
          funcName: functionName,
          args: args.map((a) => (a.startsWith("0x") ? a.slice(2) : a)),
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`VM query failed ${res.status}: ${text}`);
      }
      return (await res.json()) as ContractQueryResult;
    },
  };
}

/**
 * Wraps a ChainReader with a DB-backed cache. On reindex, cached reads avoid API calls.
 */
export function createCachedChainClient(
  inner: ChainReader,
  db: IndexerDb,
  sourceId: string,
): ChainReader {
  return {
    async getAccount(address: string): Promise<AccountOnChain | null> {
      const key = cacheKey(sourceId, "account", address);
      const cached = await cacheGet(db, key);
      if (cached !== null) return JSON.parse(cached) as AccountOnChain | null;
      const result = await inner.getAccount(address);
      await cacheSet(db, key, JSON.stringify(result), "account");
      return result;
    },

    async getTransaction(txHash: string): Promise<TransactionOnChain | null> {
      const key = cacheKey(sourceId, "transaction", txHash);
      const cached = await cacheGet(db, key);
      if (cached !== null) return JSON.parse(cached) as TransactionOnChain | null;
      const result = await inner.getTransaction(txHash);
      await cacheSet(db, key, JSON.stringify(result), "transaction");
      return result;
    },

    async queryContract(
      contractAddress: string,
      functionName: string,
      args: string[] = [],
    ): Promise<ContractQueryResult> {
      const argsKey = JSON.stringify(args);
      const key = cacheKey(sourceId, "query", contractAddress, functionName, argsKey);
      const cached = await cacheGet(db, key);
      if (cached !== null) return JSON.parse(cached) as ContractQueryResult;
      const result = await inner.queryContract(contractAddress, functionName, args);
      await cacheSet(db, key, JSON.stringify(result), "query");
      return result;
    },
  };
}

/**
 * Create a map of sourceId -> ChainReader for all Kepler sources.
 * Each client is wrapped with a DB-backed cache so reindexes avoid repeated API calls.
 */
export function createChainClientsForConfig(
  sources: Array<{ id: string; type: string; multiversxApiUrl?: string }>,
  db: IndexerDb,
): Map<string, ChainReader> {
  const map = new Map<string, ChainReader>();
  for (const source of sources) {
    if (source.type !== "kepler") continue;
    const apiUrl = source.multiversxApiUrl ?? MULTIVERSX_API_URL;
    const inner = createChainClient(apiUrl);
    map.set(source.id, createCachedChainClient(inner, db, source.id));
  }
  return map;
}
