import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { sql } from "drizzle-orm";
import { pgSchema, text } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/pglite";
import { createBatchedDb, flushInsertBuffer } from "./batched-db";
import { bootstrapInternalSchema, type IndexerDb } from "./db";

const TEST_SCHEMA = "batched_db_test";
const schema = pgSchema(TEST_SCHEMA);
const testTable = schema.table("test_output", {
  id: text("id").primaryKey(),
  value: text("value").notNull(),
});

let realDb: IndexerDb;
let client: PGlite;

beforeEach(async () => {
  client = new PGlite();
  realDb = drizzle(client) as unknown as IndexerDb;
  await bootstrapInternalSchema(realDb);
  await realDb.execute(sql.raw(`CREATE SCHEMA IF NOT EXISTS "${TEST_SCHEMA}"`));
  await realDb.execute(
    sql.raw(`
      CREATE TABLE IF NOT EXISTS "${TEST_SCHEMA}"."test_output" (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `),
  );
});

afterEach(async () => {
  await client.close();
});

async function countRows(db: IndexerDb): Promise<number> {
  const result = (await db.execute(
    sql.raw(`SELECT COUNT(*) as n FROM "${TEST_SCHEMA}"."test_output"`),
  )) as { rows: { n: string }[] };
  return parseInt(result.rows[0]?.n ?? "0", 10);
}

describe("createBatchedDb", () => {
  describe("plain insert (no onConflict)", () => {
    test("persists row when awaited", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" });

      expect(await countRows(realDb)).toBe(1);
      expect(buffer.size).toBe(0);
    });

    test("persists multiple plain inserts", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" });
      await batchedDb.insert(testTable).values({ id: "b", value: "v2" });
      await batchedDb.insert(testTable).values({ id: "c", value: "v3" });

      expect(await countRows(realDb)).toBe(3);
    });

    test("throws on duplicate primary key (no onConflict)", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" });
      const plainInsert = batchedDb.insert(testTable).values({ id: "a", value: "v2" });
      await expect(Promise.resolve(plainInsert)).rejects.toThrow();
    });
  });

  describe("onConflictDoNothing", () => {
    test("buffers rows until flush", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" }).onConflictDoNothing();
      await batchedDb.insert(testTable).values({ id: "b", value: "v2" }).onConflictDoNothing();

      expect(await countRows(realDb)).toBe(0);
      const entry = buffer.get(testTable);
      expect(entry?.rows).toHaveLength(2);
    });

    test("flushInsertBuffer persists buffered rows", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" }).onConflictDoNothing();
      await batchedDb.insert(testTable).values({ id: "b", value: "v2" }).onConflictDoNothing();

      await flushInsertBuffer(realDb, buffer);

      expect(await countRows(realDb)).toBe(2);
      expect(buffer.size).toBe(0);
    });

    test("select flushes buffer before reading", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" }).onConflictDoNothing();

      const result = (await batchedDb.execute(
        sql.raw(`SELECT id, value FROM "${TEST_SCHEMA}"."test_output"`),
      )) as { rows: { id: string; value: string }[] };

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({ id: "a", value: "v1" });
    });

    test("skips duplicates on conflict", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" });
      await batchedDb.insert(testTable).values({ id: "a", value: "v2" }).onConflictDoNothing();

      await flushInsertBuffer(realDb, buffer);

      const result = (await realDb.execute(
        sql.raw(`SELECT value FROM "${TEST_SCHEMA}"."test_output" WHERE id = 'a'`),
      )) as { rows: { value: string }[] };
      expect(result.rows[0]?.value).toBe("v1");
    });
  });

  describe("onConflictDoUpdate", () => {
    test("forwards to real db immediately", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" });
      await batchedDb
        .insert(testTable)
        .values({ id: "a", value: "v2" })
        .onConflictDoUpdate({
          target: testTable.id,
          set: { value: "v2" },
        });

      expect(await countRows(realDb)).toBe(1);
      const result = (await realDb.execute(
        sql.raw(`SELECT value FROM "${TEST_SCHEMA}"."test_output" WHERE id = 'a'`),
      )) as { rows: { value: string }[] };
      expect(result.rows[0]?.value).toBe("v2");
    });
  });

  describe("mixed plain and buffered inserts", () => {
    test("both plain and onConflictDoNothing persist correctly", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      // Plain insert (like handleAccrueRewardsEvent)
      await batchedDb.insert(testTable).values({ id: "accrue:1", value: "tx1" });
      await batchedDb.insert(testTable).values({ id: "accrue:2", value: "tx2" });

      // Buffered insert (like handleAddRewardsEvent)
      await batchedDb.insert(testTable).values({ id: "add:1", value: "tx1" }).onConflictDoNothing();
      await batchedDb.insert(testTable).values({ id: "add:2", value: "tx2" }).onConflictDoNothing();

      await flushInsertBuffer(realDb, buffer);

      expect(await countRows(realDb)).toBe(4);
      const result = (await realDb.execute(
        sql.raw(`SELECT id, value FROM "${TEST_SCHEMA}"."test_output" ORDER BY id`),
      )) as { rows: { id: string; value: string }[] };
      expect(result.rows.map((r) => r.id)).toEqual(["accrue:1", "accrue:2", "add:1", "add:2"]);
    });

    test("simulates accrue (plain) + add_rewards (buffered) handler scenario", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      const accrueEvents = [
        { id: "kepler:tx1:0", value: "tx1" },
        { id: "kepler:tx1:1", value: "tx1" },
        { id: "kepler:tx2:0", value: "tx2" },
      ];
      const addEvents = [
        { id: "kepler:tx1:0", value: "tx1" },
        { id: "kepler:tx3:0", value: "tx3" },
      ];

      for (const ev of accrueEvents) {
        await batchedDb.insert(testTable).values(ev);
      }
      for (const ev of addEvents) {
        await batchedDb.insert(testTable).values(ev).onConflictDoNothing();
      }

      await flushInsertBuffer(realDb, buffer);

      expect(await countRows(realDb)).toBe(4);
      const ids = (
        (await realDb.execute(
          sql.raw(`SELECT id FROM "${TEST_SCHEMA}"."test_output" ORDER BY id`),
        )) as { rows: { id: string }[] }
      ).rows.map((r) => r.id);
      expect(ids).toContain("kepler:tx1:0");
      expect(ids).toContain("kepler:tx1:1");
      expect(ids).toContain("kepler:tx2:0");
      expect(ids).toContain("kepler:tx3:0");
    });
  });

  describe("flushInsertBuffer", () => {
    test("clears buffer after flush", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      await batchedDb.insert(testTable).values({ id: "a", value: "v1" }).onConflictDoNothing();

      await flushInsertBuffer(realDb, buffer);
      expect(buffer.size).toBe(0);

      await flushInsertBuffer(realDb, buffer);
      expect(await countRows(realDb)).toBe(1);
    });

    test("handles empty buffer", async () => {
      const buffer = new Map();
      await flushInsertBuffer(realDb, buffer);
      expect(await countRows(realDb)).toBe(0);
    });

    test("batches multiple rows per table", async () => {
      const buffer = new Map();
      const batchedDb = createBatchedDb(realDb, buffer);

      for (let i = 0; i < 10; i++) {
        await batchedDb
          .insert(testTable)
          .values({ id: `id-${i}`, value: `v${i}` })
          .onConflictDoNothing();
      }

      await flushInsertBuffer(realDb, buffer);
      expect(await countRows(realDb)).toBe(10);
    });
  });
});
