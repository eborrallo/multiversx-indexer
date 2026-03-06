import { describe, expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import {
  bootstrapInternalSchema,
  createDatabase,
  INTERNAL_SCHEMA,
  maskDbUrl,
  parseUnixSocketUrl,
} from "./db";

function rand(): string {
  return Math.random().toString(36).slice(2, 10);
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
});

describe("createDatabase", () => {
  test.skip("creates PGlite db when no URL provided", async () => {
    const { db, close } = await createDatabase({ dataDir: ":memory:" });
    try {
      const result = await db.execute(sql.raw("SELECT 1 as num"));
      expect(result).toBeDefined();
    } finally {
      await close();
    }
  });

  test.skip("creates PGlite db with default dataDir when empty opts", async () => {
    const { db, close } = await createDatabase({});
    try {
      const result = await db.execute(sql.raw("SELECT 1 as num"));
      expect(result).toBeDefined();
    } finally {
      await close();
    }
  });
});

describe("bootstrapInternalSchema", () => {
  test.skip("creates internal tables", async () => {
    const { db, close } = await createDatabase({ dataDir: ":memory:" });
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
});
