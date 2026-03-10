import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { sql } from "drizzle-orm";
import type { MultiversXEvent } from "../schema/types";
import { makeEventId, utf8ToBase64 } from "../schema/types";
import {
  bootstrapInternalSchema,
  createDatabase,
  INTERNAL_SCHEMA,
  IndexerLogger,
  maskDbUrl,
  parseUnixSocketUrl,
} from "./db";
import {
  batchInsertRawEvents,
  buildAllowedEventKeys,
  countRawEvents,
  countRawEventsForContract,
  eventExistsInRaw,
  getCheckpoint,
  getCheckpointForContract,
  purgeOrphanedData,
  readRawEventsChunked,
  updateCheckpoint,
} from "./store";

const MEMORY_DATA_DIR = "memory://";

function rand(): string {
  return Math.random().toString(36).slice(2, 10);
}

function makeEvent(overrides: Partial<MultiversXEvent> = {}): MultiversXEvent {
  return {
    id: makeEventId("test_src", "tx1", 0),
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

describe("maskDbUrl", () => {
  test("redacts password from standard postgres URL", () => {
    const user = rand();
    const pass = rand();
    const host = `host-${rand()}`;
    const db = rand();
    const url = `postgresql://${user}:${pass}@${host}:5432/${db}`;
    expect(maskDbUrl(url)).toBe(`postgresql://${user}:***@${host}:5432/${db}`);
  });

  test("redacts password from postgres:// URL", () => {
    const user = rand();
    const pass = rand();
    const host = rand();
    const db = rand();
    const url = `postgres://${user}:${pass}@${host}/${db}`;
    expect(maskDbUrl(url)).toBe(`postgres://${user}:***@${host}/${db}`);
  });

  test("handles Cloud SQL style URL", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const project = rand();
    const region = rand();
    const instance = rand();
    const url = `postgresql://${user}:${pass}@/${db}?host=/cloudsql/${project}:${region}:${instance}`;
    expect(maskDbUrl(url)).toBe(
      `postgresql://${user}:***@/${db}?host=/cloudsql/${project}:${region}:${instance}`,
    );
  });

  test("leaves URL unchanged when no password match", () => {
    const user = rand();
    const host = rand();
    const db = rand();
    const url = `postgresql://${user}@${host}/${db}`;
    expect(maskDbUrl(url)).toBe(url);
  });
});

describe("parseUnixSocketUrl", () => {
  test("parses Cloud SQL style URL", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const project = rand();
    const region = rand();
    const instance = rand();
    const socketPath = `/cloudsql/${project}:${region}:${instance}`;
    const url = `postgresql://${user}:${pass}@/${db}?host=${encodeURIComponent(socketPath)}`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: pass,
      port: undefined,
    });
  });

  test("trims trailing newline from URL (Secret Manager / env vars)", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const socketPath = `/cloudsql/${rand()}:${rand()}:${rand()}`;
    const url = `postgresql://${user}:${pass}@/${db}?host=${encodeURIComponent(socketPath)}\n`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: pass,
      port: undefined,
    });
  });

  test("parses URL with empty password (Cloud SQL IAM etc.)", () => {
    const user = rand();
    const db = rand();
    const socketPath = `/cloudsql/${rand()}:${rand()}:${rand()}`;
    const url = `postgresql://${user}:@/${db}?host=${encodeURIComponent(socketPath)}`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: "",
      port: undefined,
    });
  });

  test("parses URL with port in query", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const socketPath = `/var/run/postgres-${rand()}`;
    const port = 5432 + Math.floor(Math.random() * 100);
    const url = `postgresql://${user}:${pass}@/${db}?host=${encodeURIComponent(socketPath)}&port=${port}`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: pass,
      port,
    });
  });

  test("returns null for standard TCP URL", () => {
    const user = rand();
    const pass = rand();
    const host = rand();
    const db = rand();
    const url = `postgresql://${user}:${pass}@${host}:5432/${db}`;
    expect(parseUnixSocketUrl(url)).toBeNull();
  });

  test("returns null when host param is not a socket path", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const url = `postgresql://${user}:${pass}@/${db}?host=${rand()}`;
    expect(parseUnixSocketUrl(url)).toBeNull();
  });

  test("returns null for malformed URL", () => {
    expect(parseUnixSocketUrl("not-a-url")).toBeNull();
    expect(parseUnixSocketUrl(`postgresql://${rand()}:${rand()}@${rand()}/${rand()}`)).toBeNull();
  });

  test("parses host with quoted path", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const socketPath = `/var/run/postgres-${rand()}`;
    const url = `postgresql://${user}:${pass}@/${db}?host=${encodeURIComponent(socketPath)}`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: pass,
      port: undefined,
    });
  });

  test("parses fallback URL (host= in query, @/ style)", () => {
    const user = rand();
    const pass = rand();
    const db = rand();
    const socketPath = `/tmp/pg-${rand()}.sock`;
    const url = `postgres://${user}:${pass}@/${db}?host=${encodeURIComponent(socketPath)}`;
    const result = parseUnixSocketUrl(url);
    expect(result).toEqual({
      host: socketPath,
      database: db,
      username: user,
      password: pass,
      port: undefined,
    });
  });

  test("decodes URL-encoded database and username", () => {
    const user = "user%40domain";
    const db = "db%2Fname";
    const socketPath = `/cloudsql/${rand()}:${rand()}:${rand()}`;
    const url = `postgresql://${user}:pass@/${db}?host=${encodeURIComponent(socketPath)}`;
    const result = parseUnixSocketUrl(url);
    expect(result?.username).toBe("user@domain");
    expect(result?.database).toBe("db/name");
  });
});

describe("IndexerLogger", () => {
  test("logQuery logs query without params", () => {
    const logger = new IndexerLogger();
    const spy = spyOn(console, "log").mockImplementation(() => {});
    try {
      logger.logQuery("SELECT 1", []);
      expect(spy).toHaveBeenCalledWith("[sql] SELECT 1");
    } finally {
      spy.mockRestore();
    }
  });

  test("logQuery logs query with params", () => {
    const logger = new IndexerLogger();
    const spy = spyOn(console, "log").mockImplementation(() => {});
    try {
      logger.logQuery("SELECT * FROM t WHERE id = $1", ["abc"]);
      expect(spy).toHaveBeenCalledWith('[sql] SELECT * FROM t WHERE id = $1  -- params: ["abc"]');
    } finally {
      spy.mockRestore();
    }
  });

  test("logQuery truncates long queries", () => {
    const logger = new IndexerLogger();
    const spy = spyOn(console, "log").mockImplementation(() => {});
    try {
      const longQuery = `SELECT ${"x".repeat(150)}`;
      logger.logQuery(longQuery, []);
      expect(spy).toHaveBeenCalledWith(expect.stringMatching(/^\[sql\] .{120}…$/));
    } finally {
      spy.mockRestore();
    }
  });
});

describe("createDatabase", () => {
  test("creates PGlite db when no URL provided", async () => {
    const { db, close } = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    try {
      const result = await db.execute(sql.raw("SELECT 1 as num"));
      expect(result).toBeDefined();
    } finally {
      await close();
    }
  });

  test("creates PGlite db with default dataDir when empty opts", async () => {
    const { db, close } = await createDatabase({});
    try {
      const result = await db.execute(sql.raw("SELECT 1 as num"));
      expect(result).toBeDefined();
    } finally {
      await close();
    }
  });

  test("throws on invalid postgres URL", async () => {
    await expect(createDatabase({ url: "postgresql://u:p@127.0.0.1:1/db" })).rejects.toThrow(
      /Database connection failed/,
    );
  });
});

describe("bootstrapInternalSchema", () => {
  test("creates internal tables", async () => {
    const { db, close } = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    try {
      await bootstrapInternalSchema(db);
      const result = (await db.execute(
        sql.raw(
          `SELECT tablename FROM pg_tables WHERE schemaname = '${INTERNAL_SCHEMA}' ORDER BY tablename`,
        ),
      )) as { rows: { tablename: string }[] };
      const tables = result.rows.map((r) => r.tablename);
      expect(tables).toContain("_multiverse_raw_events");
      expect(tables).toContain("_multiverse_checkpoint");
      expect(tables).toContain("_multiverse_chain_cache");
    } finally {
      await close();
    }
  });

  test("creates indexes on raw_events", async () => {
    const { db, close } = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    try {
      await bootstrapInternalSchema(db);
      const result = (await db.execute(
        sql.raw(
          `SELECT indexname FROM pg_indexes WHERE schemaname = '${INTERNAL_SCHEMA}' AND tablename = '_multiverse_raw_events'`,
        ),
      )) as { rows: { indexname: string }[] };
      const indexes = result.rows.map((r) => r.indexname);
      expect(indexes).toContain("idx_raw_source_timestamp");
      expect(indexes).toContain("idx_raw_source_tx");
      expect(indexes).toContain("idx_raw_order");
    } finally {
      await close();
    }
  });
});

describe("db client read validation", () => {
  let db: Awaited<ReturnType<typeof createDatabase>>["db"];
  let close: () => Promise<void>;

  beforeEach(async () => {
    const conn = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    db = conn.db;
    close = conn.close;
    await bootstrapInternalSchema(db);
  });

  afterEach(async () => {
    await close();
  });

  test("raw SELECT returns rows", async () => {
    const result = (await db.execute(sql.raw("SELECT 42 as answer"))) as {
      rows: { answer: number }[];
    };
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]?.answer).toBe(42);
  });

  test("getCheckpoint returns null when empty", async () => {
    const cp = await getCheckpoint(db, "src1");
    expect(cp).toBeNull();
  });

  test("getCheckpoint returns saved checkpoint", async () => {
    await updateCheckpoint(db, "src1", "erd1abc", ["transfer"], "txABC", 5000, 42);
    const cp = await getCheckpoint(db, "src1");
    expect(cp).not.toBeNull();
    expect(cp?.lastTxHash).toBe("txABC");
    expect(cp?.lastTimestamp).toBe(5000);
    expect(cp?.lastFromIndex).toBe(42);
  });

  test("getCheckpointForContract returns min timestamp across event identifiers", async () => {
    await updateCheckpoint(db, "src1", "erd1abc", ["EventA"], "txA", 1000, null);
    await updateCheckpoint(db, "src1", "erd1abc", ["EventB"], "txB", 2000, null);
    const cp = await getCheckpointForContract(db, "src1", "erd1abc", ["EventA", "EventB"]);
    expect(cp).not.toBeNull();
    expect(cp?.lastTimestamp).toBe(1000);
    expect(cp?.lastTxHash).toBe("txA");
  });

  test("getCheckpointForContract supports multiple events from same contract", async () => {
    await updateCheckpoint(db, "src1", "erd1abc", ["Event1", "Event2"], "txLast", 5000, null);
    const cpEvent1 = await getCheckpointForContract(db, "src1", "erd1abc", ["Event1"]);
    const cpEvent2 = await getCheckpointForContract(db, "src1", "erd1abc", ["Event2"]);
    const cpBoth = await getCheckpointForContract(db, "src1", "erd1abc", ["Event1", "Event2"]);
    expect(cpEvent1?.lastTimestamp).toBe(5000);
    expect(cpEvent2?.lastTimestamp).toBe(5000);
    expect(cpBoth?.lastTimestamp).toBe(5000);
  });

  test("countRawEvents returns 0 when empty", async () => {
    expect(await countRawEvents(db)).toBe(0);
    expect(await countRawEvents(db, "src1")).toBe(0);
  });

  test("countRawEvents counts inserted events", async () => {
    const events = [
      makeEvent({ id: "s:tx1:0", sourceId: "src1", txHash: "tx1", eventIndex: 0 }),
      makeEvent({ id: "s:tx1:1", sourceId: "src1", txHash: "tx1", eventIndex: 1 }),
      makeEvent({ id: "s:tx2:0", sourceId: "src2", txHash: "tx2", eventIndex: 0 }),
    ];
    await batchInsertRawEvents(db, events);
    expect(await countRawEvents(db)).toBe(3);
    expect(await countRawEvents(db, "src1")).toBe(2);
    expect(await countRawEvents(db, "src2")).toBe(1);
    expect(await countRawEvents(db, "other")).toBe(0);
  });

  test("countRawEventsForContract filters by contract", async () => {
    const events = [
      makeEvent({ id: "s:tx1:0", address: "erd1a", sourceId: "src1" }),
      makeEvent({ id: "s:tx2:0", address: "erd1a", sourceId: "src1" }),
      makeEvent({ id: "s:tx3:0", address: "erd1b", sourceId: "src1" }),
    ];
    await batchInsertRawEvents(db, events);
    expect(await countRawEventsForContract(db, "erd1a")).toBe(2);
    expect(await countRawEventsForContract(db, "erd1b")).toBe(1);
    expect(await countRawEventsForContract(db, "erd1c")).toBe(0);
  });

  test("eventExistsInRaw returns false when absent", async () => {
    expect(await eventExistsInRaw(db, "nonexistent:id")).toBe(false);
  });

  test("eventExistsInRaw returns true when present", async () => {
    const ev = makeEvent();
    await batchInsertRawEvents(db, [ev]);
    expect(await eventExistsInRaw(db, ev.id)).toBe(true);
  });

  test("readRawEventsChunked returns events in order", async () => {
    const events = [
      makeEvent({ id: "s:tx3:0", txHash: "tx3", timestamp: 3000, eventIndex: 0 }),
      makeEvent({ id: "s:tx1:0", txHash: "tx1", timestamp: 1000, eventIndex: 0 }),
      makeEvent({ id: "s:tx2:0", txHash: "tx2", timestamp: 2000, eventIndex: 0 }),
    ];
    await batchInsertRawEvents(db, events);

    const all = [];
    for await (const chunk of readRawEventsChunked(db, 10)) {
      all.push(...chunk);
    }
    expect(all).toHaveLength(3);
    expect(all[0]?.timestamp).toBe(1000);
    expect(all[1]?.timestamp).toBe(2000);
    expect(all[2]?.timestamp).toBe(3000);
  });

  test("readRawEventsChunked respects chunkSize", async () => {
    const events = Array.from({ length: 5 }, (_, i) =>
      makeEvent({ id: `s:tx${i}:0`, txHash: `tx${i}`, timestamp: 1000 + i, eventIndex: 0 }),
    );
    await batchInsertRawEvents(db, events);

    const chunks: unknown[][] = [];
    for await (const chunk of readRawEventsChunked(db, 2)) {
      chunks.push(chunk);
    }
    expect(chunks).toHaveLength(3);
    expect(chunks[0]).toHaveLength(2);
    expect(chunks[1]).toHaveLength(2);
    expect(chunks[2]).toHaveLength(1);
  });

  test("purgeOrphanedData removes raw events and checkpoints not in config", async () => {
    await batchInsertRawEvents(db, [
      makeEvent({ id: "s:tx1:0", sourceId: "src1", address: "erd1a", identifier: "EventA" }),
      makeEvent({ id: "s:tx2:0", sourceId: "src1", address: "erd1a", identifier: "EventB" }),
      makeEvent({ id: "s:tx3:0", sourceId: "src1", address: "erd1b", identifier: "EventA" }),
    ]);
    await updateCheckpoint(db, "src1", "erd1a", ["EventA", "EventB"], "tx2", 2000, null);
    await updateCheckpoint(db, "src1", "erd1b", ["EventA"], "tx3", 3000, null);

    const allowedKeys = buildAllowedEventKeys([
      { sourceId: "src1", address: "erd1a", eventIdentifiers: ["EventA"] },
    ]);
    const { rawEventsDeleted, checkpointsDeleted } = await purgeOrphanedData(db, allowedKeys);

    expect(rawEventsDeleted).toBe(2);
    expect(checkpointsDeleted).toBe(2);
    expect(await countRawEvents(db)).toBe(1);
    expect(await countRawEventsForContract(db, "erd1a")).toBe(1);
    expect(await countRawEventsForContract(db, "erd1b")).toBe(0);
    const cp = await getCheckpointForContract(db, "src1", "erd1a", ["EventA"]);
    expect(cp).not.toBeNull();
    expect(cp?.lastTxHash).toBe("tx2");
  });

  test("readRawEventsChunked filters by sourceId", async () => {
    const events = [
      makeEvent({ id: "s1:tx1:0", sourceId: "src1", txHash: "tx1", timestamp: 1000 }),
      makeEvent({ id: "s2:tx1:0", sourceId: "src2", txHash: "tx1", timestamp: 1000 }),
    ];
    await batchInsertRawEvents(db, events);

    const forSrc1 = [];
    for await (const chunk of readRawEventsChunked(db, 10, "src1")) {
      forSrc1.push(...chunk);
    }
    expect(forSrc1).toHaveLength(1);
    expect(forSrc1[0]?.sourceId).toBe("src1");
  });
});
