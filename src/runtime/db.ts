import { sql } from "drizzle-orm";
import type { Logger } from "drizzle-orm/logger";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";

export const INTERNAL_SCHEMA = "multiverse_internal";

function log(msg: string) {
  console.log(msg);
}

export class IndexerLogger implements Logger {
  logQuery(query: string, params: unknown[]): void {
    const short = query.length > 120 ? `${query.slice(0, 120)}…` : query;
    const clean = short.replace(/\s+/g, " ").trim();
    if (params.length > 0) {
      console.log(`[sql] ${clean}  -- params: ${JSON.stringify(params)}`);
    } else {
      console.log(`[sql] ${clean}`);
    }
  }
}

export type IndexerDb = PgDatabase<PgQueryResultHKT, Record<string, never>>;

export interface IndexerDatabase {
  db: IndexerDb;
  close(): Promise<void>;
}

export async function createDatabase(opts: {
  url?: string;
  dataDir?: string;
  verbose?: boolean;
}): Promise<IndexerDatabase> {
  const logger = opts.verbose ? new IndexerLogger() : false;

  const onnotice = opts.verbose
    ? (notice: { message?: string; severity?: string }) => {
        log(`[pg] ${notice.severity ?? "NOTICE"}: ${notice.message ?? ""}`);
      }
    : () => {};

  if (opts.url) {
    const pgModule = await import("postgres");
    const pgDrizzle = await import("drizzle-orm/postgres-js");
    const client = pgModule.default(opts.url, { onnotice });
    const db = pgDrizzle.drizzle(client, { logger });
    return {
      db: db as unknown as IndexerDb,
      close: async () => {
        await client.end();
      },
    };
  }

  const { PGlite } = await import("@electric-sql/pglite");
  const pgliteDrizzle = await import("drizzle-orm/pglite");
  const dataDir = opts.dataDir ?? "./indexer-data";
  const client = new PGlite(dataDir);
  const db = pgliteDrizzle.drizzle(client, { logger });
  return {
    db: db as unknown as IndexerDb,
    close: async () => {
      await client.close();
    },
  };
}

export async function bootstrapInternalSchema(db: IndexerDb): Promise<void> {
  const s = INTERNAL_SCHEMA;
  log(`Bootstrapping internal schema "${s}"...`);

  await db.execute(sql.raw(`CREATE SCHEMA IF NOT EXISTS "${s}"`));
  await db.execute(
    sql.raw(`
    CREATE TABLE IF NOT EXISTS "${s}"."_multiverse_raw_events" (
      id TEXT PRIMARY KEY,
      source_id TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      block_timestamp INTEGER NOT NULL,
      contract_address TEXT NOT NULL,
      event_identifier TEXT NOT NULL,
      topics TEXT NOT NULL,
      data TEXT,
      additional_data TEXT,
      event_index INTEGER NOT NULL,
      raw_payload TEXT
    )
  `),
  );
  await db.execute(
    sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_raw_source_timestamp
      ON "${s}"."_multiverse_raw_events"(source_id, block_timestamp)
  `),
  );
  await db.execute(
    sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_raw_source_tx
      ON "${s}"."_multiverse_raw_events"(source_id, tx_hash)
  `),
  );
  await db.execute(
    sql.raw(`
    CREATE INDEX IF NOT EXISTS idx_raw_order
      ON "${s}"."_multiverse_raw_events"(block_timestamp, tx_hash, event_index)
  `),
  );
  await db.execute(
    sql.raw(`
    CREATE TABLE IF NOT EXISTS "${s}"."_multiverse_checkpoint" (
      source_id TEXT PRIMARY KEY,
      contract_address TEXT NOT NULL,
      last_tx_hash TEXT,
      last_timestamp INTEGER,
      last_from_index INTEGER,
      updated_at INTEGER
    )
  `),
  );
  await db.execute(
    sql.raw(`
    CREATE TABLE IF NOT EXISTS "${s}"."_multiverse_chain_cache" (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      method TEXT NOT NULL
    )
  `),
  );

  log(
    `  Internal schema ready (tables: _multiverse_raw_events, _multiverse_checkpoint, _multiverse_chain_cache)`,
  );
}
