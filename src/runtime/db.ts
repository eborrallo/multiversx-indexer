import { sql } from "drizzle-orm";
import type { Logger } from "drizzle-orm/logger";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import { DEFAULT_DATA_DIR } from "../constants";

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

/** Redacts password from a postgres URL for safe logging. */
export function maskDbUrl(url: string): string {
  return url.replace(/^(postgres(?:ql)?:\/\/[^:]+:)([^@]+)(@)/, "$1***$3");
}

/** Builds credential-safe connection info for error messages (password hidden). */
function formatCredentialsForError(
  url: string,
  socketOpts: { host: string; database: string; username: string; port?: number } | null,
): string {
  if (socketOpts) {
    const parts = [
      `username: ${socketOpts.username}`,
      `database: ${socketOpts.database}`,
      `host: ${socketOpts.host}`,
      "password: ***",
    ];
    if (socketOpts.port != null) parts.splice(3, 0, `port: ${socketOpts.port}`);
    return parts.join(", ");
  }
  return maskDbUrl(url);
}

/** Removes potential credential leaks from error messages. */
function sanitizeErrorMessage(msg: string): string {
  return msg
    .replace(/password[=:]\s*['"]?[^'"\s]+/gi, "password=***")
    .replace(/postgres(?:ql)?:\/\/[^:]+:[^@]+@/g, (m) => m.replace(/:[^@]+@/, ":***@"));
}

export interface IndexerDatabase {
  db: IndexerDb;
  close(): Promise<void>;
}

/**
 * Parses PostgreSQL URLs that use host= in query params for Unix sockets.
 * The postgres package fails to parse these (ERR_INVALID_URL) because the host
 * is empty in the URL and the socket path is in ?host=/path/to/socket
 */
export function parseUnixSocketUrl(url: string): {
  host: string;
  database: string;
  username: string;
  password: string;
  port?: number;
} | null {
  try {
    // Trim URL — Secret Manager / env vars often add trailing newlines
    url = url.trim();
    // Primary: strict regex for user:password@/database?host=/path (password optional for Cloud SQL IAM etc.)
    const match = url.match(/^postgres(?:ql)?:\/\/([^:@]+)(?::([^@]*))?@\/([^?]+)(?:\?(.+))?$/);
    if (match) {
      const [, user, password, database, query] = match;
      if (user && database) {
        const hostMatch = query?.match(/host=([^&]+)/);
        const hostRaw = hostMatch?.[1];
        const host = hostRaw
          ? decodeURIComponent(hostRaw)
              .replace(/^["']|["']$/g, "")
              .trim()
          : null;
        if (host?.startsWith("/")) {
          const portMatch = query?.match(/port=(\d+)/);
          const portRaw = portMatch?.[1];
          const port = portRaw ? parseInt(portRaw, 10) : undefined;
          const pass = password ?? "";
          return {
            host,
            database: decodeURIComponent(database),
            username: decodeURIComponent(user),
            password: pass ? decodeURIComponent(pass) : "",
            port,
          };
        }
      }
    }

    // Fallback: URL has @/ (empty host) and host= with socket path — postgres would fail on this
    if (url.includes("@/") && /host=\/[^&\s]+/.test(url)) {
      const authEnd = url.indexOf("@/");
      const authPart = url.slice(url.indexOf("://") + 3, authEnd);
      const colonIdx = authPart.indexOf(":");
      const username =
        colonIdx > 0
          ? decodeURIComponent(authPart.slice(0, colonIdx))
          : decodeURIComponent(authPart);
      const password = colonIdx > 0 ? decodeURIComponent(authPart.slice(colonIdx + 1)) : "";
      if (username) {
        const afterSlash = url.slice(authEnd + 2);
        const qIdx = afterSlash.indexOf("?");
        const database = decodeURIComponent(qIdx >= 0 ? afterSlash.slice(0, qIdx) : afterSlash);
        const query = qIdx >= 0 ? afterSlash.slice(qIdx + 1) : "";
        const hostMatch = query.match(/host=([^&]+)/);
        const hostRaw = hostMatch?.[1];
        const host = hostRaw
          ? decodeURIComponent(hostRaw)
              .replace(/^["']|["']$/g, "")
              .trim()
          : null;
        if (host?.startsWith("/")) {
          const portMatch = query.match(/port=(\d+)/);
          const portRaw = portMatch?.[1];
          const port = portRaw ? parseInt(portRaw, 10) : undefined;
          return { host, database, username, password, port };
        }
      }
    }
    return null;
  } catch {
    return null;
  }
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
    const url = opts.url.trim();
    const pgModule = await import("postgres");
    const pgDrizzle = await import("drizzle-orm/postgres-js");
    const pgOptions = { onnotice };
    const socketOpts = parseUnixSocketUrl(url);
    const credentialsLabel = formatCredentialsForError(url, socketOpts);

    try {
      const client = socketOpts
        ? pgModule.default({
            host: socketOpts.host,
            database: socketOpts.database,
            username: socketOpts.username,
            password: socketOpts.password,
            port: socketOpts.port,
            ...pgOptions,
          })
        : pgModule.default(url, pgOptions);
      const db = pgDrizzle.drizzle(client, { logger });

      // Validate connection before returning
      await db.execute(sql.raw("SELECT 1"));

      return {
        db: db as unknown as IndexerDb,
        close: async () => {
          await client.end();
        },
      };
    } catch (err) {
      const cause = err instanceof Error ? err.message : String(err);
      const sanitized = sanitizeErrorMessage(cause);
      throw new Error(
        `Database connection failed. Credentials: ${credentialsLabel}. ${sanitized}`,
        { cause: err instanceof Error ? err : undefined },
      );
    }
  }

  const { PGlite } = await import("@electric-sql/pglite");
  const pgliteDrizzle = await import("drizzle-orm/pglite");
  const dataDir = opts.dataDir ?? DEFAULT_DATA_DIR;
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
