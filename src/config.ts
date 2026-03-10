import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DEFAULT_SCHEMA_NAME, KEPLER_ES_URL, KEPLER_WS_URL } from "./constants";
import type { ChainReader } from "./runtime/chain-client";
import type { IndexerDb } from "./runtime/db";
import type { MultiversXEvent } from "./schema/types";

export function getDefaultSchemaName(): string {
  try {
    const path = join(process.cwd(), "package.json");
    const raw = readFileSync(path, "utf-8");
    const pkg = JSON.parse(raw) as { name?: string };
    const name = pkg?.name;
    if (typeof name === "string" && name.length > 0) {
      return name.replace(/-/g, "_");
    }
  } catch {
    // ignore
  }
  return DEFAULT_SCHEMA_NAME;
}

export interface KeplerSourceConfig {
  id: string;
  type: "kepler";
  esUrl?: string;
  wsUrl?: string;
  apiKey: string;
  batchSize?: number;
  requestDelayMs?: number;
  /** Base URL for MultiversX API (used to fetch deployment block when fromBlock is not set). Default: https://api.multiversx.com. Use https://testnet-api.multiversx.com for testnet. */
  multiversxApiUrl?: string;
}

export type SourceConfig = KeplerSourceConfig;

export interface ContractConfig {
  sourceId: string;
  address: string;
  eventIdentifiers: string[];
  /** Optional topics (base64) for WebSocket subscription. If set, used instead of eventIdentifiers. Plain text is base64-encoded. */
  eventTopics?: string[];
  /** Start backfill from this block (used when no checkpoint). If not set, the deployment block is fetched from the MultiversX API. ES index must have a "block" field for block filtering. */
  fromBlock?: number;
  /** End backfill at this block (optional). */
  toBlock?: number;
  startTimestamp?: number;
  endTimestamp?: number;
}

export interface HandlerContext {
  db: IndexerDb;
  sourceId: string;
  /** User Drizzle schema (tables) — use for type-safe inserts, e.g. context.schema.rewardEvents */
  schema: Record<string, unknown>;
  /** Read-only chain client for the current source (contract queries, account, tx). Null if source has no API. */
  client: ChainReader | null;
}

export type EventHandler = (event: MultiversXEvent, context: HandlerContext) => Promise<void>;

export interface IndexerConfig {
  database: {
    url?: string;
    dataDir?: string;
    /** Log every SQL query to console (noisy, useful for debugging). Default: false. */
    verbose?: boolean;
  };
  /** Schema for processed (user) tables. Default: project name from package.json (e.g. tao_rewards_tracker). */
  schemaName?: string;
  /**
   * When true, clears raw_events and checkpoint tables before starting.
   * Use for a full re-index from scratch. Default: false.
   */
  resetInternalTables?: boolean;
  sources: SourceConfig[];
  contracts: ContractConfig[];
  handlers: Record<string, EventHandler>;
  schema?: Record<string, unknown>;
  onSetup?: (context: { db: IndexerDb; schema: Record<string, unknown> }) => Promise<void>;
  healthPort?: number;
}

export function defineConfig(config: IndexerConfig): IndexerConfig {
  if (!config.sources.length) {
    throw new Error("At least one source is required");
  }
  if (!config.contracts.length) {
    throw new Error("At least one contract is required");
  }

  for (const source of config.sources) {
    if (!source.apiKey) {
      throw new Error(`Source "${source.id}" requires an apiKey`);
    }
    if (source.type === "kepler") {
      source.esUrl ??= KEPLER_ES_URL;
      source.wsUrl ??= KEPLER_WS_URL;
    }
  }

  for (const contract of config.contracts) {
    const source = config.sources.find((s) => s.id === contract.sourceId);
    if (!source) {
      throw new Error(
        `Contract "${contract.address}" references unknown source "${contract.sourceId}"`,
      );
    }
  }

  if (config.schemaName === undefined || config.schemaName === "") {
    config.schemaName = getDefaultSchemaName() || DEFAULT_SCHEMA_NAME;
  }

  return config;
}
