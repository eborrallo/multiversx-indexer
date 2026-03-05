/**
 * Transparent write buffering for handlers. Intercepts db.insert() and batches rows
 * per table, then flushes with bulk inserts. No need for separate batch handlers.
 *
 * IMPORTANT: Before any select/update/delete/execute, we flush the buffer so buffered
 * inserts are visible. This ensures handlers that do "insert then select" or
 * "insert then update" see the persisted data.
 */
import type { IndexerDb } from "./db";

type Table = Record<string, unknown>;
type BufferedInsert = { rows: Record<string, unknown>[]; strategy: "doNothing" };

const INSERT_BUFFER = Symbol("insertBuffer");

export interface BatchedDb extends IndexerDb {
  [INSERT_BUFFER]: Map<Table, BufferedInsert>;
}

function wrapWithFlush<T extends { then?: unknown }>(
  realDb: IndexerDb,
  buffer: Map<Table, BufferedInsert>,
  value: T,
): T {
  if (value == null || typeof (value as PromiseLike<unknown>).then !== "function") return value;
  return new Proxy(value, {
    get(target, prop) {
      const v = Reflect.get(target, prop);
      if (prop === "then") {
        return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) => {
          return flushInsertBuffer(realDb, buffer).then(() =>
            (v as (f?: (x: unknown) => unknown, r?: (e: unknown) => unknown) => Promise<unknown>)(
              onFulfilled,
              onRejected,
            ),
          );
        };
      }
      if (typeof v === "function") {
        return (...args: unknown[]) => {
          const result = (v as (...a: unknown[]) => unknown).apply(target, args);
          return wrapWithFlush(realDb, buffer, result as T);
        };
      }
      return v;
    },
  }) as T;
}

/**
 * Create a db wrapper that buffers insert().values().onConflictDoNothing() calls.
 * Before any select/update/delete/execute, flushes the buffer so buffered data is visible.
 */
export function createBatchedDb(
  realDb: IndexerDb,
  buffer: Map<Table, BufferedInsert> = new Map(),
): BatchedDb {
  const readMethods = new Set(["select", "update", "delete", "execute"]);
  const batchedDb = new Proxy(realDb, {
    get(target, prop) {
      if (prop === "insert") {
        return (table: Table) => ({
          values: (rowOrRows: Record<string, unknown> | Record<string, unknown>[]) => ({
            onConflictDoNothing: () => {
              const rows = Array.isArray(rowOrRows) ? rowOrRows : [rowOrRows];
              const entry = buffer.get(table);
              if (entry) {
                entry.rows.push(...rows);
              } else {
                buffer.set(table, { rows: [...rows], strategy: "doNothing" });
              }
              return Promise.resolve(undefined);
            },
            onConflictDoUpdate: (opts: { target: unknown; set: Record<string, unknown> }) => {
              return (realDb as IndexerDb)
                .insert(table as never)
                .values(rowOrRows as never)
                .onConflictDoUpdate(opts as never);
            },
          }),
        });
      }
      if (prop === INSERT_BUFFER) return buffer;
      const value = Reflect.get(target, prop);
      if (readMethods.has(prop as string) && typeof value === "function") {
        return (...args: unknown[]) => {
          if (prop === "execute") {
            return flushInsertBuffer(realDb, buffer).then(() =>
              (value as (...a: unknown[]) => unknown).apply(target, args),
            );
          }
          const result = (value as (...a: unknown[]) => unknown).apply(target, args);
          return wrapWithFlush(realDb, buffer, result as { then?: unknown });
        };
      }
      return value;
    },
  }) as BatchedDb;
  (batchedDb as BatchedDb)[INSERT_BUFFER] = buffer;
  return batchedDb;
}

/**
 * Flush buffered inserts to the real db. Call after processing each chunk.
 */
export async function flushInsertBuffer(
  realDb: IndexerDb,
  buffer: Map<Table, BufferedInsert>,
): Promise<void> {
  for (const [table, { rows, strategy }] of buffer) {
    if (rows.length === 0) continue;
    if (strategy === "doNothing") {
      await (realDb as IndexerDb)
        .insert(table as never)
        .values(rows as never)
        .onConflictDoNothing();
    }
  }
  buffer.clear();
}
