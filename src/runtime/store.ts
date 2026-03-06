import { and, asc, count, eq, sql } from "drizzle-orm";
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

export async function getCheckpoint(db: IndexerDb, sourceId: string) {
  const rows = await db
    .select()
    .from(multiverseCheckpoint)
    .where(eq(multiverseCheckpoint.sourceId, sourceId));
  return rows[0] ?? null;
}

export async function updateCheckpoint(
  db: IndexerDb,
  sourceId: string,
  contractAddress: string,
  lastTxHash: string | null,
  lastTimestamp: number | null,
  lastFromIndex: number | null,
) {
  await db
    .insert(multiverseCheckpoint)
    .values({
      sourceId,
      contractAddress,
      lastTxHash,
      lastTimestamp,
      lastFromIndex,
      updatedAt: Math.floor(Date.now() / 1000),
    })
    .onConflictDoUpdate({
      target: multiverseCheckpoint.sourceId,
      set: {
        lastTxHash,
        lastTimestamp,
        lastFromIndex,
        updatedAt: Math.floor(Date.now() / 1000),
      },
    });
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
