import { getTableColumns, getTableName, sql } from "drizzle-orm";
import type { Column } from "drizzle-orm/column";
import type { Table } from "drizzle-orm/table";
import type { IndexerDb } from "./db";

function log(msg: string) {
  console.log(msg);
}

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

/**
 * Build CREATE TABLE from a Drizzle pg Table.
 * If schemaName is provided, creates "schemaName"."tableName".
 */
function buildCreateTableSQL(table: Table, schemaName?: string): string {
  const tableName = getTableName(table as Parameters<typeof getTableName>[0]);
  const columns = getTableColumns(table as Parameters<typeof getTableColumns>[0]);
  const parts: string[] = [];
  const primaryKeys: string[] = [];

  for (const [_, col] of Object.entries(columns)) {
    if (!isPgColumn(col)) continue;
    const dbName = (col as Column).name;
    const quoted = `"${dbName.replace(/"/g, '""')}"`;
    if ((col as Column).primary) primaryKeys.push(quoted);
  }

  for (const [_, col] of Object.entries(columns)) {
    if (!isPgColumn(col)) continue;
    const dbName = (col as Column).name;
    const quoted = `"${dbName.replace(/"/g, '""')}"`;
    const type = col.getSQLType();
    const notNull = (col as Column).notNull ? " NOT NULL" : "";
    const primary = primaryKeys.length === 1 && (col as Column).primary ? " PRIMARY KEY" : "";
    parts.push(`${quoted} ${type}${notNull}${primary}`);
  }

  const body = parts.join(", ");
  const pkConstraint = primaryKeys.length > 1 ? `, PRIMARY KEY (${primaryKeys.join(", ")})` : "";
  const fullName = schemaName
    ? `"${schemaName.replace(/"/g, '""')}"."${tableName.replace(/"/g, '""')}"`
    : `"${tableName.replace(/"/g, '""')}"`;
  return `CREATE TABLE ${fullName} (${body}${pkConstraint})`;
}

/**
 * Drop user schema if it exists (CASCADE), create it, then create all tables from config.schema inside it.
 * This gives a clean processed-data schema on every run (Ponder-style).
 */
export async function syncUserSchema(
  db: IndexerDb,
  schema: Record<string, unknown> | undefined,
  schemaName: string,
): Promise<void> {
  if (!schema) return;

  const safe = schemaName.replace(/"/g, '""');
  await db.execute(sql.raw(`DROP SCHEMA IF EXISTS "${safe}" CASCADE`));
  await db.execute(sql.raw(`CREATE SCHEMA "${safe}"`));

  const tables: string[] = [];
  for (const [_, table] of Object.entries(schema)) {
    if (!isDrizzleTable(table)) continue;

    const tableName = getTableName(table as Parameters<typeof getTableName>[0]);
    const createSQL = buildCreateTableSQL(table, schemaName);
    try {
      await db.execute(sql.raw(createSQL));
      tables.push(tableName);
    } catch (err) {
      console.error(`syncUserSchema failed on "${tableName}":`, createSQL, err);
      throw err;
    }
  }

  if (tables.length > 0) {
    log(`  Schema "${schemaName}" ready (tables: ${tables.join(", ")})`);
  }
}
