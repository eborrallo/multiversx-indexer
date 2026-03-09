import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { integer, pgSchema, text } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/pglite";
import type { IndexerDb } from "./db";
import { createGraphQLHandler } from "./graphql";
import { syncUserSchema } from "./schema-sync";

const SCHEMA_NAME = "test_app";
const testSchema = pgSchema(SCHEMA_NAME);
const testItems = testSchema.table("test_items", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  value: integer("value").notNull(),
});

const schema = { testItems };

let client: PGlite;
let db: IndexerDb;

async function graphqlRequest(
  query: string,
  variables?: Record<string, unknown>,
): Promise<unknown> {
  const handler = createGraphQLHandler({ db, schema, schemaName: SCHEMA_NAME });
  if (!handler) throw new Error("GraphQL handler is null");

  const res = await handler(
    new Request("http://localhost/graphql", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    }),
  );

  const json = (await res.json()) as { data?: unknown; errors?: unknown[] };
  if (json.errors?.length) throw new Error(JSON.stringify(json.errors));
  return json.data;
}

beforeEach(async () => {
  client = new PGlite("memory://");
  db = drizzle(client) as unknown as IndexerDb;
  await syncUserSchema(db, schema, SCHEMA_NAME);

  // Insert 5 test rows
  await db.insert(testItems).values([
    { id: "1", name: "first", value: 10 },
    { id: "2", name: "second", value: 20 },
    { id: "3", name: "third", value: 30 },
    { id: "4", name: "fourth", value: 40 },
    { id: "5", name: "fifth", value: 50 },
  ]);
});

afterEach(async () => {
  await client.close();
});

describe("GraphQL list query pagination (limit, offset)", () => {
  const listQuery = `
    query ListTestItems($limit: Int, $offset: Int, $orderBy: TestItemOrderBy) {
      testItems(limit: $limit, offset: $offset, orderBy: $orderBy) {
        id
        name
        value
      }
    }
  `;

  test("returns first page with limit and offset", async () => {
    const data = (await graphqlRequest(listQuery, {
      limit: 2,
      offset: 0,
      orderBy: { field: "VALUE", direction: "ASC" },
    })) as { testItems: { id: string }[] };

    expect(data.testItems).toHaveLength(2);
    expect(data.testItems[0]?.id).toBe("1");
    expect(data.testItems[1]?.id).toBe("2");
  });

  test("returns second page with offset", async () => {
    const data = (await graphqlRequest(listQuery, {
      limit: 2,
      offset: 2,
      orderBy: { field: "VALUE", direction: "ASC" },
    })) as { testItems: { id: string }[] };

    expect(data.testItems).toHaveLength(2);
    expect(data.testItems[0]?.id).toBe("3");
    expect(data.testItems[1]?.id).toBe("4");
  });

  test("returns last partial page", async () => {
    const data = (await graphqlRequest(listQuery, {
      limit: 2,
      offset: 4,
      orderBy: { field: "VALUE", direction: "ASC" },
    })) as { testItems: { id: string }[] };

    expect(data.testItems).toHaveLength(1);
    expect(data.testItems[0]?.id).toBe("5");
  });

  test("returns empty when offset exceeds total", async () => {
    const data = (await graphqlRequest(listQuery, {
      limit: 10,
      offset: 10,
    })) as { testItems: unknown[] };

    expect(data.testItems).toHaveLength(0);
  });

  test("uses default limit 100 when not specified", async () => {
    const data = (await graphqlRequest(listQuery)) as { testItems: { id: string }[] };

    expect(data.testItems).toHaveLength(5);
    expect(data.testItems.map((r) => r.id)).toEqual(["1", "2", "3", "4", "5"]);
  });

  test("clamps negative offset to 0", async () => {
    const data = (await graphqlRequest(listQuery, {
      limit: 2,
      offset: -5,
      orderBy: { field: "VALUE", direction: "ASC" },
    })) as { testItems: { id: string }[] };

    expect(data.testItems).toHaveLength(2);
    expect(data.testItems[0]?.id).toBe("1");
    expect(data.testItems[1]?.id).toBe("2");
  });

  test("pagination works with where filter", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($limit: Int, $offset: Int, $where: TestItemWhere) {
        testItems(limit: $limit, offset: $offset, where: $where) {
          id
          value
        }
      }
    `,
      {
        limit: 1,
        offset: 1,
        where: { value_in: [10, 20, 30] },
      },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(1);
    expect(data.testItems[0]?.id).toBe("2");
    expect(data.testItems[0]?.value).toBe(20);
  });
});

describe("GraphQL numeric comparison operators (gt, gte, lt, lte)", () => {
  test("value_gt filters greater than", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($where: TestItemWhere) {
        testItems(where: $where) { id value }
      }
    `,
      { where: { value_gt: 30 } },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(2);
    expect(data.testItems.map((r) => r.value)).toEqual([40, 50]);
  });

  test("value_gte filters greater than or equal", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($where: TestItemWhere) {
        testItems(where: $where, orderBy: { field: VALUE, direction: ASC }) { id value }
      }
    `,
      { where: { value_gte: 30 } },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(3);
    expect(data.testItems.map((r) => r.value)).toEqual([30, 40, 50]);
  });

  test("value_lt filters less than", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($where: TestItemWhere) {
        testItems(where: $where, orderBy: { field: VALUE, direction: ASC }) { id value }
      }
    `,
      { where: { value_lt: 30 } },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(2);
    expect(data.testItems.map((r) => r.value)).toEqual([10, 20]);
  });

  test("value_lte filters less than or equal", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($where: TestItemWhere) {
        testItems(where: $where, orderBy: { field: VALUE, direction: ASC }) { id value }
      }
    `,
      { where: { value_lte: 30 } },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(3);
    expect(data.testItems.map((r) => r.value)).toEqual([10, 20, 30]);
  });

  test("combines gt and lt for range", async () => {
    const data = (await graphqlRequest(
      `
      query ListTestItems($where: TestItemWhere) {
        testItems(where: $where, orderBy: { field: VALUE, direction: ASC }) { id value }
      }
    `,
      { where: { value_gt: 15, value_lt: 45 } },
    )) as { testItems: { id: string; value: number }[] };

    expect(data.testItems).toHaveLength(3);
    expect(data.testItems.map((r) => r.value)).toEqual([20, 30, 40]);
  });
});
