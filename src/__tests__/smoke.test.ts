import { afterEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { sql } from "drizzle-orm";
import { pgSchema, text } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/pglite";
import { defineConfig } from "../config";
import { KEPLER_ES_URL, KEPLER_WS_URL } from "../constants";
import { bootstrapInternalSchema, INTERNAL_SCHEMA, type IndexerDb } from "../runtime/db";
import { reindex } from "../runtime/pipeline";
import {
  batchInsertRawEvents,
  countRawEvents,
  eventExistsInRaw,
  eventToRow,
  getCheckpoint,
  readRawEventsChunked,
  rowToEvent,
  updateCheckpoint,
} from "../runtime/store";
import type { MultiversXEvent } from "../schema/types";
import { base64ToBigInt, base64ToUtf8, makeEventId, utf8ToBase64 } from "../schema/types";

let openClients: PGlite[] = [];

function makeDb(): IndexerDb {
  const client = new PGlite();
  openClients.push(client);
  return drizzle(client) as unknown as IndexerDb;
}

function makeEvent(overrides: Partial<MultiversXEvent> = {}): MultiversXEvent {
  return {
    id: overrides.id ?? makeEventId("test_src", "tx1", 0),
    sourceId: "test_src",
    txHash: "tx1",
    timestamp: 1000,
    address: "erd1abc",
    identifier: "transfer",
    topics: [utf8ToBase64("transfer"), utf8ToBase64("100")],
    data: null,
    additionalData: [],
    eventIndex: 0,
    ...overrides,
  };
}

afterEach(async () => {
  for (const c of openClients) {
    try {
      await c.close();
    } catch {}
  }
  openClients = [];
});

describe("Database bootstrap", () => {
  test("creates internal tables", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const result = (await db.execute(
      sql.raw(
        `SELECT tablename FROM pg_tables WHERE schemaname = '${INTERNAL_SCHEMA}' ORDER BY tablename`,
      ),
    )) as { rows: { tablename: string }[] };
    const tables = result.rows.map((r) => r.tablename);
    expect(tables).toContain("_multiverse_raw_events");
    expect(tables).toContain("_multiverse_checkpoint");
  });
});

describe("Event utilities", () => {
  test("makeEventId produces stable IDs", () => {
    expect(makeEventId("src", "hash", 0)).toBe("src:hash:0");
    expect(makeEventId("src", "hash", 5)).toBe("src:hash:5");
  });

  test("base64 round-trip", () => {
    const original = "accrue_rewards_event";
    const b64 = utf8ToBase64(original);
    expect(base64ToUtf8(b64)).toBe(original);
  });

  test("base64ToBigInt decodes correctly", () => {
    const buf = Buffer.from([0x01, 0x00]);
    const b64 = buf.toString("base64");
    expect(base64ToBigInt(b64)).toBe(256n);
  });

  test("base64ToBigInt handles empty", () => {
    expect(base64ToBigInt("")).toBe(0n);
  });
});

describe("Raw event store", () => {
  test("insert and count", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const events = [
      makeEvent({
        id: "test_src:tx1:0",
        txHash: "tx1",
        eventIndex: 0,
        timestamp: 1000,
      }),
      makeEvent({
        id: "test_src:tx1:1",
        txHash: "tx1",
        eventIndex: 1,
        timestamp: 1000,
      }),
      makeEvent({
        id: "test_src:tx2:0",
        txHash: "tx2",
        eventIndex: 0,
        timestamp: 2000,
      }),
    ];

    await batchInsertRawEvents(db, events);
    expect(await countRawEvents(db)).toBe(3);
    expect(await countRawEvents(db, "test_src")).toBe(3);
    expect(await countRawEvents(db, "other_src")).toBe(0);
  });

  test("idempotent insert (onConflictDoNothing)", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const ev = makeEvent();
    await batchInsertRawEvents(db, [ev]);
    await batchInsertRawEvents(db, [ev]);
    expect(await countRawEvents(db)).toBe(1);
  });

  test("eventExistsInRaw", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const ev = makeEvent();
    expect(await eventExistsInRaw(db, ev.id)).toBe(false);
    await batchInsertRawEvents(db, [ev]);
    expect(await eventExistsInRaw(db, ev.id)).toBe(true);
  });

  test("readRawEventsChunked returns events in order", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const events = [
      makeEvent({
        id: "s:tx3:0",
        txHash: "tx3",
        timestamp: 3000,
        eventIndex: 0,
      }),
      makeEvent({
        id: "s:tx1:0",
        txHash: "tx1",
        timestamp: 1000,
        eventIndex: 0,
      }),
      makeEvent({
        id: "s:tx2:0",
        txHash: "tx2",
        timestamp: 2000,
        eventIndex: 0,
      }),
    ];
    await batchInsertRawEvents(db, events);

    const all: MultiversXEvent[] = [];
    for await (const chunk of readRawEventsChunked(db, 10)) {
      all.push(...chunk);
    }
    expect(all.length).toBe(3);
    expect(all[0]?.timestamp).toBe(1000);
    expect(all[1]?.timestamp).toBe(2000);
    expect(all[2]?.timestamp).toBe(3000);
  });

  test("eventToRow <-> rowToEvent round-trip", () => {
    const ev = makeEvent();
    const row = eventToRow(ev);
    const back = rowToEvent(row as Parameters<typeof rowToEvent>[0]);
    expect(back.id).toBe(ev.id);
    expect(back.sourceId).toBe(ev.sourceId);
    expect(back.txHash).toBe(ev.txHash);
    expect(back.timestamp).toBe(ev.timestamp);
    expect(back.topics).toEqual(ev.topics);
    expect(back.additionalData).toEqual(ev.additionalData);
  });
});

describe("Checkpoint", () => {
  test("get returns null when empty", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    const cp = await getCheckpoint(db, "src1");
    expect(cp).toBeNull();
  });

  test("update then get", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    await updateCheckpoint(db, "src1", "erd1abc", ["transfer"], "txABC", 5000, 42);
    const cp = await getCheckpoint(db, "src1");
    expect(cp).not.toBeNull();
    expect(cp?.lastTxHash).toBe("txABC");
    expect(cp?.lastTimestamp).toBe(5000);
    expect(cp?.lastFromIndex).toBe(42);
  });

  test("upsert overwrites previous", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);

    await updateCheckpoint(db, "src1", "erd1abc", ["ev"], "tx1", 1000, null);
    await updateCheckpoint(db, "src1", "erd1abc", ["ev"], "tx2", 2000, null);

    const cp = await getCheckpoint(db, "src1");
    expect(cp?.lastTxHash).toBe("tx2");
    expect(cp?.lastTimestamp).toBe(2000);
  });

  test("multiple events from same contract have independent checkpoints", async () => {
    const db = makeDb();
    await bootstrapInternalSchema(db);
    const { getCheckpointForContract } = await import("../runtime/store");

    await updateCheckpoint(db, "src1", "erd1abc", ["EventA"], "txA", 1000, null);
    await updateCheckpoint(db, "src1", "erd1abc", ["EventB"], "txB", 2000, null);

    const cpBoth = await getCheckpointForContract(db, "src1", "erd1abc", ["EventA", "EventB"]);
    expect(cpBoth?.lastTimestamp).toBe(1000);
    expect(cpBoth?.lastTxHash).toBe("txA");

    const cpA = await getCheckpointForContract(db, "src1", "erd1abc", ["EventA"]);
    const cpB = await getCheckpointForContract(db, "src1", "erd1abc", ["EventB"]);
    expect(cpA?.lastTimestamp).toBe(1000);
    expect(cpB?.lastTimestamp).toBe(2000);
  });
});

describe("Config validation", () => {
  test("rejects empty sources", () => {
    expect(() =>
      defineConfig({
        database: {},
        sources: [],
        contracts: [{ sourceId: "x", address: "y", eventIdentifiers: ["z"] }],
        handlers: {},
      }),
    ).toThrow("At least one source");
  });

  test("rejects empty contracts", () => {
    expect(() =>
      defineConfig({
        database: {},
        sources: [{ id: "s", type: "kepler", apiKey: "k" }],
        contracts: [],
        handlers: {},
      }),
    ).toThrow("At least one contract");
  });

  test("rejects missing apiKey", () => {
    expect(() =>
      defineConfig({
        database: {},
        sources: [{ id: "s", type: "kepler", apiKey: "" }],
        contracts: [{ sourceId: "s", address: "a", eventIdentifiers: ["e"] }],
        handlers: {},
      }),
    ).toThrow("requires an apiKey");
  });

  test("rejects unknown sourceId", () => {
    expect(() =>
      defineConfig({
        database: {},
        sources: [{ id: "s", type: "kepler", apiKey: "k" }],
        contracts: [{ sourceId: "unknown", address: "a", eventIdentifiers: ["e"] }],
        handlers: {},
      }),
    ).toThrow('unknown source "unknown"');
  });

  test("sets default Kepler URLs", () => {
    const config = defineConfig({
      database: {},
      sources: [{ id: "s", type: "kepler", apiKey: "k" }],
      contracts: [{ sourceId: "s", address: "a", eventIdentifiers: ["e"] }],
      handlers: {},
    });
    const src = config.sources[0];
    expect(src?.type === "kepler" && src?.esUrl).toBe(KEPLER_ES_URL);
    expect(src?.type === "kepler" && src?.wsUrl).toBe(KEPLER_WS_URL);
  });
});

describe("Reindex (pipeline phase 2)", () => {
  const testSchemaName = "tao_rewards_tracker";
  const testSchema = pgSchema(testSchemaName);
  const testTable = testSchema.table("test_output", {
    id: text("id").primaryKey(),
    value: text("value").notNull(),
  });

  test("replays handlers from raw events into user tables", async () => {
    const client = new PGlite("./test-reindex-data");
    openClients.push(client);
    const db = drizzle(client) as unknown as IndexerDb;
    await bootstrapInternalSchema(db);

    const events = [
      makeEvent({
        id: "s:tx1:0",
        txHash: "tx1",
        timestamp: 1000,
        identifier: "ev",
        address: "c1",
        eventIndex: 0,
      }),
      makeEvent({
        id: "s:tx2:0",
        txHash: "tx2",
        timestamp: 2000,
        identifier: "ev",
        address: "c1",
        eventIndex: 0,
      }),
    ];
    await batchInsertRawEvents(db, events);
    await client.close();

    const handlerCalls: string[] = [];
    const config = defineConfig({
      database: { dataDir: "./test-reindex-data" },
      schemaName: testSchemaName,
      sources: [{ id: "test_src", type: "kepler", apiKey: "test" }],
      contracts: [{ sourceId: "test_src", address: "c1", eventIdentifiers: ["ev"] }],
      schema: { testTable },
      handlers: {
        "c1:ev": async (event, ctx) => {
          handlerCalls.push(event.id);
          const table = ctx.schema.testTable as typeof testTable;
          await ctx.db
            .insert(table)
            .values({ id: event.id, value: event.txHash })
            .onConflictDoNothing();
        },
      },
    });

    await reindex(config);

    expect(handlerCalls).toEqual(["s:tx1:0", "s:tx2:0"]);

    const client2 = new PGlite("./test-reindex-data");
    openClients.push(client2);
    const db2 = drizzle(client2) as unknown as IndexerDb;
    const result = (await db2.execute(
      sql.raw(`SELECT id, value FROM "${testSchemaName}"."test_output" ORDER BY id`),
    )) as { rows: { id: string; value: string }[] };
    expect(result.rows.length).toBe(2);
    expect(result.rows[0]?.id).toBe("s:tx1:0");
    expect(result.rows[1]?.id).toBe("s:tx2:0");
    await client2.close();

    // Clean up PGlite data directory
    const { rmSync } = await import("node:fs");
    try {
      rmSync("./test-reindex-data", { recursive: true, force: true });
    } catch {}
  });
});
