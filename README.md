# multiverse-indexer

MultiversX event indexer with [Kepler](https://projectx.mx) integration, Drizzle schema sync, and real-time backfill.

## Features

- **Kepler integration** — Index events from MultiversX via Kepler's Elasticsearch and WebSocket APIs
- **Drizzle ORM** — Define your schema with Drizzle, auto-sync migrations
- **Backfill + realtime** — Historical backfill from ES, then live events via WebSocket
- **PGLite or Postgres** — Use embedded PGLite for local dev, or PostgreSQL for production
- **Type-safe handlers** — Event handlers with full TypeScript support

## Quick start

### Create a new project

```bash
bunx multiverse-indexer init
# or in a subdirectory
bunx multiverse-indexer init ./my-indexer
```

This scaffolds:

- `indexer.config.ts` — Configuration
- `schema.ts` — Drizzle schema for your processed tables
- `drizzle.config.ts` — Drizzle Kit config
- `handlers/` — Event handlers
- `docker-compose.yml` — Optional Postgres for local dev
- `.env.example` — Environment template

### Configure

1. Copy `.env.example` to `.env` and set `KEPLER_API_KEY` (get one at [projectx.mx](https://projectx.mx))
2. Edit `indexer.config.ts` with your contract address and event identifiers
3. Customize `schema.ts` and handlers for your use case

### Run

```bash
# Optional: start Postgres
docker compose up -d

# Run the indexer (dev mode with auto-restart)
bun run dev
```

## Commands

| Command | Description |
|---------|-------------|
| `init [dir]` | Scaffold a new indexer project |
| `start [config]` | Run indexer: migrate → backfill → realtime |
| `dev [config]` | Same as start, with auto-restart on file changes |
| `studio [config]` | Open Drizzle Studio for the database |

## Configuration

```ts
import { defineConfig } from "multiverse-indexer";
import * as schema from "./schema";
import { handleMyEvent } from "./handlers/my-event";

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
      address: "erd1qqqq...",
      eventIdentifiers: ["my_event"],
    },
  ],

  schema,
  handlers: {
    "erd1qqqq...:my_event": handleMyEvent,
  },

  healthPort: 42069,
});
```

## Event handlers

Handlers receive events and a context with `db`, `schema`, and `client` (chain reader):

```ts
import type { EventHandler } from "multiverse-indexer";
import { base64ToUtf8 } from "multiverse-indexer";
import type * as schema from "../schema";

export const handleMyEvent: EventHandler = async (event, context) => {
  const data = event.topics[0] ? base64ToUtf8(event.topics[0]) : "";

  const table = (context.schema as typeof schema).myTable;
  await context.db
    .insert(table)
    .values({
      id: event.id,
      txHash: event.txHash,
      timestamp: event.timestamp,
      data,
    })
    .onConflictDoNothing();
};
```

## Requirements

- **Node.js** 18+ or **Bun**
- **Kepler API key** — [Get one here](https://projectx.mx)

## License

MIT
