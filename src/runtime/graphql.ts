import {
  and,
  asc,
  desc,
  eq,
  getTableColumns,
  getTableName,
  gt,
  gte,
  ilike,
  inArray,
  lt,
  lte,
  type SQL,
} from "drizzle-orm";
import type { Column } from "drizzle-orm/column";
import type { Table } from "drizzle-orm/table";
import type { GraphQLInputType } from "graphql";
import {
  GraphQLBoolean,
  GraphQLEnumType,
  type GraphQLFieldConfigMap,
  GraphQLFloat,
  GraphQLID,
  GraphQLInputObjectType,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  type GraphQLOutputType,
  GraphQLSchema,
  GraphQLString,
} from "graphql";
import { createYoga } from "graphql-yoga";
import {
  GRAPHQL_DEFAULT_LIMIT,
  GRAPHQL_DEFAULT_ORDER_DIRECTION,
  GRAPHQL_MAX_LIMIT,
} from "../constants";
import type { IndexerDb } from "./db";

function isPgColumn(col: unknown): col is Column & { getSQLType(): string } {
  if (col == null || typeof col !== "object") return false;
  const c = col as Record<string, unknown>;
  return typeof c.getSQLType === "function";
}

function isDrizzleTable(obj: unknown): obj is Table {
  if (obj == null || typeof obj !== "object") return false;
  try {
    getTableName(obj as Parameters<typeof getTableName>[0]);
    getTableColumns(obj as Parameters<typeof getTableColumns>[0]);
    return true;
  } catch {
    return false;
  }
}

/** snake_case to camelCase for query names (e.g. reward_events -> rewardEvents) */
function tableNameToQueryName(tableName: string): string {
  const parts = tableName.split("_");
  return (
    parts[0]?.toLowerCase() +
    parts
      .slice(1)
      .map((s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase())
      .join("")
  );
}

/** Plural list query name (reward_events -> rewardEvents) */
function listQueryName(tableName: string): string {
  return tableNameToQueryName(tableName);
}

/** Singular query name (reward_events -> rewardEvent) */
function singleQueryName(tableName: string): string {
  const camel = tableNameToQueryName(tableName);
  return camel.endsWith("s") ? camel.slice(0, -1) : camel;
}

function sqlTypeToGraphQL(sqlType: string): GraphQLOutputType {
  const lower = sqlType.toLowerCase();
  if (lower.includes("int") && !lower.includes("bigint")) return GraphQLInt;
  if (lower.includes("bigint") || lower.includes("numeric") || lower.includes("decimal"))
    return GraphQLString;
  if (lower.includes("float") || lower.includes("double") || lower.includes("real"))
    return GraphQLFloat;
  if (lower === "boolean" || lower === "bool") return GraphQLBoolean;
  return GraphQLString;
}

/** camelCase to SCREAMING_SNAKE_CASE for enum values */
function toEnumValue(key: string): string {
  return key
    .replace(/([A-Z])/g, "_$1")
    .replace(/^_/, "")
    .toUpperCase();
}

const orderDirectionEnum = new GraphQLEnumType({
  name: "OrderDirection",
  values: {
    ASC: { value: "asc" },
    DESC: { value: "desc" },
  },
});

export interface GraphQLContext {
  db: IndexerDb;
}

interface TableMeta {
  table: Table;
  tableName: string;
  typeName: string;
  listQueryName: string;
  singleQueryName: string;
  pkColumn: Column | null;
  pkColumnKey: string | null;
  columns: Array<{ columnKey: string; column: Column; gqlType: GraphQLOutputType }>;
}

function collectTableMeta(schema: Record<string, unknown>): TableMeta[] {
  const result: TableMeta[] = [];
  for (const [_, value] of Object.entries(schema)) {
    if (!isDrizzleTable(value)) continue;
    const table = value as Table;
    const tableName = getTableName(table as Parameters<typeof getTableName>[0]);
    const columnsMap = getTableColumns(table as Parameters<typeof getTableColumns>[0]);
    let pkColumn: Column | null = null;
    let pkColumnKey: string | null = null;
    const columns: Array<{ columnKey: string; column: Column; gqlType: GraphQLOutputType }> = [];
    for (const [colKey, col] of Object.entries(columnsMap)) {
      if (!isPgColumn(col)) continue;
      const sqlType = col.getSQLType();
      columns.push({
        columnKey: colKey,
        column: col as Column,
        gqlType: sqlTypeToGraphQL(sqlType),
      });
      if ((col as Column).primary) {
        pkColumn = col as Column;
        pkColumnKey = colKey;
      }
    }
    result.push({
      table,
      tableName,
      typeName:
        singleQueryName(tableName).charAt(0).toUpperCase() + singleQueryName(tableName).slice(1),
      listQueryName: listQueryName(tableName),
      singleQueryName: singleQueryName(tableName),
      pkColumn: pkColumn ?? null,
      pkColumnKey,
      columns,
    });
  }
  return result;
}

export function buildGraphQLSchema(
  schema: Record<string, unknown>,
  _schemaName: string,
): GraphQLSchema | null {
  const tables = collectTableMeta(schema);
  if (tables.length === 0) return null;

  const queryFields: GraphQLFieldConfigMap<unknown, GraphQLContext> = {};

  for (const meta of tables) {
    const fields: GraphQLFieldConfigMap<Record<string, unknown>, GraphQLContext> = {};
    for (const { columnKey, gqlType } of meta.columns) {
      fields[columnKey] = {
        type: gqlType,
        resolve: (parent: Record<string, unknown>) => parent[columnKey],
      };
    }
    const objectType = new GraphQLObjectType({
      name: meta.typeName,
      fields,
    });

    const orderByFieldEnum = new GraphQLEnumType({
      name: `${meta.typeName}OrderByField`,
      values: Object.fromEntries(
        meta.columns.map((c) => [toEnumValue(c.columnKey), { value: c.columnKey }]),
      ),
    });

    const orderByInput = new GraphQLInputObjectType({
      name: `${meta.typeName}OrderBy`,
      fields: {
        field: { type: new GraphQLNonNull(orderByFieldEnum) },
        direction: { type: orderDirectionEnum, defaultValue: GRAPHQL_DEFAULT_ORDER_DIRECTION },
      },
    });

    const whereInputFields: Record<string, { type: GraphQLInputType }> = {};
    for (const { columnKey, gqlType } of meta.columns) {
      const scalar: GraphQLInputType =
        gqlType === GraphQLInt || gqlType === GraphQLFloat || gqlType === GraphQLBoolean
          ? (gqlType as GraphQLInputType)
          : GraphQLString;
      whereInputFields[`${columnKey}_eq`] = { type: scalar };
      whereInputFields[`${columnKey}_in`] = { type: new GraphQLList(new GraphQLNonNull(scalar)) };
      if (gqlType === GraphQLString) {
        whereInputFields[`${columnKey}_contains`] = { type: GraphQLString };
      }
      if (gqlType === GraphQLInt || gqlType === GraphQLFloat) {
        whereInputFields[`${columnKey}_gt`] = { type: scalar };
        whereInputFields[`${columnKey}_gte`] = { type: scalar };
        whereInputFields[`${columnKey}_lt`] = { type: scalar };
        whereInputFields[`${columnKey}_lte`] = { type: scalar };
      }
    }
    const whereInput = new GraphQLInputObjectType({
      name: `${meta.typeName}Where`,
      fields: whereInputFields,
    });

    queryFields[meta.listQueryName] = {
      type: new GraphQLList(objectType),
      args: {
        limit: { type: GraphQLInt, defaultValue: 100 },
        offset: { type: GraphQLInt, defaultValue: 0 },
        orderBy: { type: orderByInput },
        where: { type: whereInput },
      },
      resolve: async (_source, args, context) => {
        const limit = Math.min(Number(args.limit) || GRAPHQL_DEFAULT_LIMIT, GRAPHQL_MAX_LIMIT);
        const offset = Math.max(0, Number(args.offset) || 0);
        let query = context.db.select().from(meta.table);

        const orderByArg = args.orderBy as { field?: string; direction?: string } | undefined;
        if (orderByArg?.field) {
          const col = meta.columns.find((c) => c.columnKey === orderByArg.field);
          if (col) {
            const dir = orderByArg.direction === "desc" ? "desc" : "asc";
            const colRef = meta.table[col.columnKey as keyof typeof meta.table];
            query = query.orderBy(
              dir === "desc" ? desc(colRef as never) : asc(colRef as never),
            ) as typeof query;
          }
        }

        const whereArg = args.where as Record<string, unknown> | undefined;
        if (whereArg && typeof whereArg === "object") {
          const conditions: SQL[] = [];
          for (const [key, value] of Object.entries(whereArg)) {
            if (value == null) continue;
            const eqMatch = key.match(/^(.+)_eq$/);
            const inMatch = key.match(/^(.+)_in$/);
            const containsMatch = key.match(/^(.+)_contains$/);
            const gtMatch = key.match(/^(.+)_gt$/);
            const gteMatch = key.match(/^(.+)_gte$/);
            const ltMatch = key.match(/^(.+)_lt$/);
            const lteMatch = key.match(/^(.+)_lte$/);
            const colKey =
              eqMatch?.[1] ??
              inMatch?.[1] ??
              containsMatch?.[1] ??
              gtMatch?.[1] ??
              gteMatch?.[1] ??
              ltMatch?.[1] ??
              lteMatch?.[1];
            if (!colKey) continue;
            const col = meta.columns.find((c) => c.columnKey === colKey);
            if (!col) continue;
            const colRef = meta.table[colKey as keyof typeof meta.table];
            if (eqMatch) {
              conditions.push(eq(colRef as never, value) as SQL);
            } else if (inMatch && Array.isArray(value)) {
              conditions.push(inArray(colRef as never, value) as SQL);
            } else if (containsMatch && typeof value === "string") {
              conditions.push(ilike(colRef as never, `%${value}%`) as SQL);
            } else if (gtMatch && (typeof value === "number" || typeof value === "string")) {
              conditions.push(gt(colRef as never, value) as SQL);
            } else if (gteMatch && (typeof value === "number" || typeof value === "string")) {
              conditions.push(gte(colRef as never, value) as SQL);
            } else if (ltMatch && (typeof value === "number" || typeof value === "string")) {
              conditions.push(lt(colRef as never, value) as SQL);
            } else if (lteMatch && (typeof value === "number" || typeof value === "string")) {
              conditions.push(lte(colRef as never, value) as SQL);
            }
          }
          if (conditions.length > 0) {
            query = query.where(and(...conditions) as SQL) as typeof query;
          }
        }

        query = query.limit(limit).offset(offset) as typeof query;
        const rows = await query;
        return rows as Record<string, unknown>[];
      },
    };

    if (meta.pkColumn && meta.pkColumnKey) {
      queryFields[meta.singleQueryName] = {
        type: objectType,
        args: {
          id: { type: new GraphQLNonNull(GraphQLID) },
        },
        resolve: async (_source, args, context) => {
          const colRef = meta.table[meta.pkColumnKey as keyof typeof meta.table];
          const rows = await context.db
            .select()
            .from(meta.table)
            .where(eq(colRef as never, args.id as string) as SQL)
            .limit(1);
          return (rows[0] ?? null) as Record<string, unknown> | null;
        },
      };
    }
  }

  const queryType = new GraphQLObjectType({
    name: "Query",
    fields: queryFields,
  });

  return new GraphQLSchema({ query: queryType });
}

export function createGraphQLHandler(options: {
  db: IndexerDb;
  schema: Record<string, unknown>;
  schemaName: string;
}): ((request: Request) => Promise<Response>) | null {
  const graphqlSchema = buildGraphQLSchema(options.schema, options.schemaName);
  if (!graphqlSchema) return null;

  const yoga = createYoga({
    schema: graphqlSchema,
    graphiql: true,
    context: (): GraphQLContext => ({ db: options.db }),
  });

  return (request: Request): Promise<Response> => Promise.resolve(yoga.fetch(request));
}
