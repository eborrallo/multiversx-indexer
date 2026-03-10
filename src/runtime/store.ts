import { and, asc, count, eq, inArray, sql } from "drizzle-orm";
import { RAW_BATCH_SIZE } from "../constants";
import { multiverseCheckpoint, multiverseRawEvents } from "../schema/internal";
import type { MultiversXEvent } from "../schema/types";
import type { IndexerDb } from "./db";

export function eventToRow(event: MultiversXEvent) {
  return {
    id: event.id,
    sourceId: event.sourceId,
    txHash: event.txHash,
    blockTimestamp: event.timestamp,
    contractAddress: event.address,
    eventIdentifier: event.identifier,
    topics: JSON.stringify(event.topics),
    data: event.data,
    additionalData: JSON.stringify(event.additionalData),
    eventIndex: event.eventIndex,
    rawPayload: null,
  };
}

export function rowToEvent(row: typeof multiverseRawEvents.$inferSelect): MultiversXEvent {
  return {
    id: row.id,
    sourceId: row.sourceId,
    txHash: row.txHash,
    timestamp: row.blockTimestamp,
    address: row.contractAddress,
    identifier: row.eventIdentifier,
    topics: JSON.parse(row.topics),
    data: row.data,
    additionalData: row.additionalData ? JSON.parse(row.additionalData) : [],
    eventIndex: row.eventIndex,
  };
}

export async function batchInsertRawEvents(
  db: IndexerDb,
  events: MultiversXEvent[],
): Promise<number> {
  if (events.length === 0) return 0;

  let inserted = 0;
  for (let i = 0; i < events.length; i += RAW_BATCH_SIZE) {
    const batch = events.slice(i, i + RAW_BATCH_SIZE);
    const rows = batch.map(eventToRow);
    await db.insert(multiverseRawEvents).values(rows).onConflictDoNothing();
    inserted += batch.length;
  }
  return inserted;
}

/**
 * Get the minimum lastTimestamp across all checkpoints for (sourceId, contractAddress, eventIdentifier).
 * Used to determine afterTimestamp when fetching - we must not miss events from any topic.
 */
export async function getCheckpointForContract(
  db: IndexerDb,
  sourceId: string,
  contractAddress: string,
  eventIdentifiers: string[],
): Promise<{
  lastTxHash: string | null;
  lastTimestamp: number | null;
  lastFromIndex: number | null;
} | null> {
  if (eventIdentifiers.length === 0) return null;

  const rows = await db
    .select()
    .from(multiverseCheckpoint)
    .where(
      and(
        eq(multiverseCheckpoint.sourceId, sourceId),
        eq(multiverseCheckpoint.contractAddress, contractAddress),
        inArray(multiverseCheckpoint.eventIdentifier, eventIdentifiers),
      ),
    );

  if (rows.length === 0) return null;

  const first = rows[0];
  if (!first) return null;
  const minRow = rows.slice(1).reduce((acc, r) => {
    const ts = r.lastTimestamp ?? 0;
    const accTs = acc.lastTimestamp ?? 0;
    return ts < accTs ? r : acc;
  }, first);

  return {
    lastTxHash: minRow.lastTxHash,
    lastTimestamp: minRow.lastTimestamp,
    lastFromIndex: minRow.lastFromIndex,
  };
}

/** @deprecated Use getCheckpointForContract. Kept for backwards compatibility in tests. */
export async function getCheckpoint(db: IndexerDb, sourceId: string) {
  const rows = await db
    .select()
    .from(multiverseCheckpoint)
    .where(eq(multiverseCheckpoint.sourceId, sourceId));
  return rows[0] ?? null;
}

/**
 * Update checkpoint for each event identifier. When we process a batch of events
 * (sorted by timestamp), we've seen all events up to the last one - so we update
 * all event identifiers for this contract to that position.
 */
export async function updateCheckpoint(
  db: IndexerDb,
  sourceId: string,
  contractAddress: string,
  eventIdentifiers: string[],
  lastTxHash: string | null,
  lastTimestamp: number | null,
  lastFromIndex: number | null,
) {
  const updatedAt = Math.floor(Date.now() / 1000);
  for (const eventIdentifier of eventIdentifiers) {
    await db
      .insert(multiverseCheckpoint)
      .values({
        sourceId,
        contractAddress,
        eventIdentifier,
        lastTxHash,
        lastTimestamp,
        lastFromIndex,
        updatedAt,
      })
      .onConflictDoUpdate({
        target: [
          multiverseCheckpoint.sourceId,
          multiverseCheckpoint.contractAddress,
          multiverseCheckpoint.eventIdentifier,
        ],
        set: {
          lastTxHash,
          lastTimestamp,
          lastFromIndex,
          updatedAt,
        },
      });
  }
}

export async function countRawEvents(db: IndexerDb, sourceId?: string): Promise<number> {
  const result = sourceId
    ? await db
        .select({ total: count() })
        .from(multiverseRawEvents)
        .where(eq(multiverseRawEvents.sourceId, sourceId))
    : await db.select({ total: count() }).from(multiverseRawEvents);
  return result[0]?.total ?? 0;
}

export async function countRawEventsForContract(
  db: IndexerDb,
  contractAddress: string,
): Promise<number> {
  const result = await db
    .select({ total: count() })
    .from(multiverseRawEvents)
    .where(eq(multiverseRawEvents.contractAddress, contractAddress));
  return result[0]?.total ?? 0;
}

export async function countRawEventsForContractAndEvent(
  db: IndexerDb,
  contractAddress: string,
  eventIdentifier: string,
): Promise<number> {
  const result = await db
    .select({ total: count() })
    .from(multiverseRawEvents)
    .where(
      and(
        eq(multiverseRawEvents.contractAddress, contractAddress),
        eq(multiverseRawEvents.eventIdentifier, eventIdentifier),
      ),
    );
  return result[0]?.total ?? 0;
}

/**
 * Cursor for keyset pagination. Uses (blockTimestamp, txHash, eventIndex) to avoid OFFSET.
 */
type Cursor = { blockTimestamp: number; txHash: string; eventIndex: number };

/**
 * Read raw events in order, paginated for memory efficiency.
 * Uses keyset pagination (cursor-based) instead of OFFSET for O(1) per-page cost.
 * Returns an async generator yielding batches.
 */
export async function* readRawEventsChunked(
  db: IndexerDb,
  chunkSize: number = 10_000,
  sourceId?: string,
): AsyncGenerator<MultiversXEvent[]> {
  let cursor: Cursor | null = null;
  const ordering = [
    asc(multiverseRawEvents.blockTimestamp),
    asc(multiverseRawEvents.txHash),
    asc(multiverseRawEvents.eventIndex),
  ] as const;

  while (true) {
    const base = db.select().from(multiverseRawEvents);
    let query = base.orderBy(...ordering).limit(chunkSize);

    const conditions = [];
    if (sourceId) conditions.push(eq(multiverseRawEvents.sourceId, sourceId));
    if (cursor) {
      conditions.push(
        sql`(${multiverseRawEvents.blockTimestamp}, ${multiverseRawEvents.txHash}, ${multiverseRawEvents.eventIndex}) > (${cursor.blockTimestamp}, ${cursor.txHash}, ${cursor.eventIndex})`,
      );
    }
    if (conditions.length > 0) {
      query = query.where(and(...conditions)) as typeof query;
    }

    const rows = await query;

    if (rows.length === 0) break;
    yield rows.map(rowToEvent);
    const last = rows.at(-1);
    if (!last) break;
    cursor = {
      blockTimestamp: last.blockTimestamp,
      txHash: last.txHash,
      eventIndex: last.eventIndex,
    };
    if (rows.length < chunkSize) break;
  }
}

export async function eventExistsInRaw(db: IndexerDb, eventId: string): Promise<boolean> {
  const result = await db
    .select({ id: multiverseRawEvents.id })
    .from(multiverseRawEvents)
    .where(eq(multiverseRawEvents.id, eventId))
    .limit(1);
  return result.length > 0;
}

/** Key for allowed (sourceId, contractAddress, eventIdentifier) from config. */
export type AllowedEventKey = {
  sourceId: string;
  contractAddress: string;
  eventIdentifier: string;
};

/**
 * Build allowed keys from config contracts. Each (sourceId, contractAddress, eventIdentifier)
 * that appears in config is kept; all others are considered orphaned.
 */
export function buildAllowedEventKeys(
  contracts: Array<{ sourceId: string; address: string; eventIdentifiers: string[] }>,
): AllowedEventKey[] {
  const keys: AllowedEventKey[] = [];
  for (const c of contracts) {
    for (const evId of c.eventIdentifiers) {
      keys.push({ sourceId: c.sourceId, contractAddress: c.address, eventIdentifier: evId });
    }
  }
  return keys;
}

/**
 * Purge raw events and checkpoints that are not in the allowed set (config).
 * Call when config changes (e.g. contract or event identifier removed).
 */
export async function purgeOrphanedData(
  db: IndexerDb,
  allowedKeys: AllowedEventKey[],
): Promise<{ rawEventsDeleted: number; checkpointsDeleted: number }> {
  const allowedSet = new Set(
    allowedKeys.map((k) => `${k.sourceId}|${k.contractAddress}|${k.eventIdentifier}`),
  );

  const keyStr = (s: string, c: string, e: string) => `${s}|${c}|${e}`;

  let checkpointsDeleted = 0;
  const checkpointRows = await db.select().from(multiverseCheckpoint);
  for (const row of checkpointRows) {
    if (!allowedSet.has(keyStr(row.sourceId, row.contractAddress, row.eventIdentifier))) {
      await db
        .delete(multiverseCheckpoint)
        .where(
          and(
            eq(multiverseCheckpoint.sourceId, row.sourceId),
            eq(multiverseCheckpoint.contractAddress, row.contractAddress),
            eq(multiverseCheckpoint.eventIdentifier, row.eventIdentifier),
          ),
        );
      checkpointsDeleted++;
    }
  }

  const distinctRaw = await db
    .selectDistinct({
      sourceId: multiverseRawEvents.sourceId,
      contractAddress: multiverseRawEvents.contractAddress,
      eventIdentifier: multiverseRawEvents.eventIdentifier,
    })
    .from(multiverseRawEvents);

  let rawEventsDeleted = 0;
  for (const row of distinctRaw) {
    if (!allowedSet.has(keyStr(row.sourceId, row.contractAddress, row.eventIdentifier))) {
      const countResult = await db
        .select({ n: count() })
        .from(multiverseRawEvents)
        .where(
          and(
            eq(multiverseRawEvents.sourceId, row.sourceId),
            eq(multiverseRawEvents.contractAddress, row.contractAddress),
            eq(multiverseRawEvents.eventIdentifier, row.eventIdentifier),
          ),
        );
      const n = countResult[0]?.n ?? 0;
      await db
        .delete(multiverseRawEvents)
        .where(
          and(
            eq(multiverseRawEvents.sourceId, row.sourceId),
            eq(multiverseRawEvents.contractAddress, row.contractAddress),
            eq(multiverseRawEvents.eventIdentifier, row.eventIdentifier),
          ),
        );
      rawEventsDeleted += n;
    }
  }

  return { rawEventsDeleted, checkpointsDeleted };
}
