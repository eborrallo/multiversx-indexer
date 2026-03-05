import { pgSchema, pgTable, text, integer } from "multiverse-indexer";

/** Must match indexer.config schemaName (or default from package.json name). */
export const PROCESSED_SCHEMA = "app";

const processed = pgSchema(PROCESSED_SCHEMA);

export const exampleEvents = processed.table("example_events", {
  id: text("id").primaryKey(),
  txHash: text("tx_hash").notNull(),
  timestamp: integer("timestamp").notNull(),
  data: text("data"),
});
