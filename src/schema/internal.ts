import { index, integer, pgSchema, text } from "drizzle-orm/pg-core";
import { INTERNAL_SCHEMA } from "../runtime/db";

const internal = pgSchema(INTERNAL_SCHEMA);

export const multiverseRawEvents = internal.table(
  "_multiverse_raw_events",
  {
    id: text("id").primaryKey(),
    sourceId: text("source_id").notNull(),
    txHash: text("tx_hash").notNull(),
    blockTimestamp: integer("block_timestamp").notNull(),
    contractAddress: text("contract_address").notNull(),
    eventIdentifier: text("event_identifier").notNull(),
    topics: text("topics").notNull(),
    data: text("data"),
    additionalData: text("additional_data"),
    eventIndex: integer("event_index").notNull(),
    rawPayload: text("raw_payload"),
  },
  (table) => ({
    sourceTimestampIdx: index("idx_raw_source_timestamp").on(table.sourceId, table.blockTimestamp),
    sourceTxIdx: index("idx_raw_source_tx").on(table.sourceId, table.txHash),
    orderIdx: index("idx_raw_order").on(table.blockTimestamp, table.txHash, table.eventIndex),
  }),
);

export const multiverseCheckpoint = internal.table("_multiverse_checkpoint", {
  sourceId: text("source_id").primaryKey(),
  contractAddress: text("contract_address").notNull(),
  lastTxHash: text("last_tx_hash"),
  lastTimestamp: integer("last_timestamp"),
  lastFromIndex: integer("last_from_index"),
  updatedAt: integer("updated_at"),
});

/** Cache for chain read calls (getAccount, getTransaction, queryContract). Persists across reindexes. */
export const multiverseChainCache = internal.table("_multiverse_chain_cache", {
  key: text("key").primaryKey(),
  value: text("value").notNull(),
  method: text("method").notNull(), // "account" | "transaction" | "query"
});
