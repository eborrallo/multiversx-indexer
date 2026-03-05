import { defineConfig } from "multiverse-indexer";
import * as schema from "./schema";
import { handleExampleEvent } from "./handlers/example-event";

const CONTRACT = "erd1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq";

export default defineConfig({
  database: process.env.DATABASE_URL
    ? { url: process.env.DATABASE_URL }
    : { dataDir: "./indexer-data" },

  schemaName: schema.PROCESSED_SCHEMA,

  sources: [
    {
      id: "kepler_mainnet",
      type: "kepler",
      apiKey: process.env.KEPLER_API_KEY ?? "",
      batchSize: 100,
      multiversxApiUrl: "https://api.multiversx.com",
    },
  ],

  contracts: [
    {
      sourceId: "kepler_mainnet",
      address: CONTRACT,
      eventIdentifiers: ["example_event"],
    },
  ],

  schema,

  handlers: {
    [`${CONTRACT}:example_event`]: handleExampleEvent,
  },

  healthPort: 42069,
});
