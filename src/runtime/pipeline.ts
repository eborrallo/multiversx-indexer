import type { HandlerContext, IndexerConfig, KeplerSourceConfig } from "../config";
import type { MultiversXEvent } from "../schema/types";
import { createBatchedDb, flushInsertBuffer } from "./batched-db";
import type { ChainReader } from "./chain-client";
import { createChainClientsForConfig } from "./chain-client";
import type { IndexerDb } from "./db";
import { bootstrapInternalSchema, createDatabase, maskDbUrl } from "./db";
import { getDeploymentBlock, getLatestBlock } from "./deployment-block";
import { KeplerEsFetcher, KeplerWsClient } from "./fetcher";
import { addProcessed, setHealthy, setPhase, setReady, startHealthServer } from "./health";
import { syncUserSchema } from "./schema-sync";
import {
  batchInsertRawEvents,
  countRawEvents,
  countRawEventsForContract,
  eventExistsInRaw,
  getCheckpoint,
  readRawEventsChunked,
  updateCheckpoint,
} from "./store";

const HANDLER_BATCH_SIZE = 1500;

function log(msg: string) {
  console.log(msg);
}

// ---------------------------------------------------------------------------
// Backfill raw events from Kepler ES (delta only)
// ---------------------------------------------------------------------------
async function phase1Backfill(config: IndexerConfig, db: IndexerDb): Promise<void> {
  setPhase("backfill");
  log("Backfill raw events");

  for (const source of config.sources) {
    if (source.type !== "kepler") continue;

    const fetcher = new KeplerEsFetcher(source);
    const contracts = config.contracts.filter((c) => c.sourceId === source.id);

    let cachedLatestBlock: number | null = null;
    try {
      cachedLatestBlock = await getLatestBlock(source.multiversxApiUrl);
    } catch {
      /* ignore */
    }
    if (cachedLatestBlock != null) {
      log(`  Chain latest block: ${cachedLatestBlock.toLocaleString()}`);
    }

    for (const contract of contracts) {
      const addrShort = `${contract.address.slice(0, 12)}...${contract.address.slice(-6)}`;
      const identifiers = contract.eventIdentifiers.join(", ");
      log("");
      log(`  ┌ ${addrShort} [${identifiers}]`);

      // --- resolve block range ---
      const checkpoint = await getCheckpoint(db, source.id);
      const afterTimestamp = checkpoint?.lastTimestamp ?? contract.startTimestamp ?? null;
      let afterBlock = checkpoint != null ? null : (contract.fromBlock ?? null);

      let deploymentBlock: number | null = null;
      if (checkpoint == null) {
        deploymentBlock = await getDeploymentBlock(contract.address, source.multiversxApiUrl);
        if (deploymentBlock != null) {
          if (afterBlock != null && deploymentBlock > afterBlock) {
            log(
              `  │ ⚠ fromBlock (${afterBlock}) < deployment block (${deploymentBlock}) — using deployment block`,
            );
            afterBlock = deploymentBlock;
          } else if (afterBlock == null) {
            afterBlock = deploymentBlock;
          }
        }
      }

      let beforeBlock = contract.toBlock ?? null;
      if (beforeBlock == null && afterBlock != null) {
        beforeBlock = cachedLatestBlock;
      }

      const fromLabel =
        afterBlock != null
          ? afterBlock.toLocaleString()
          : deploymentBlock != null
            ? deploymentBlock.toLocaleString()
            : "—";
      const toLabel = beforeBlock != null ? beforeBlock.toLocaleString() : "latest";
      log(`  │ Block range: ${fromLabel} → ${toLabel}`);

      // --- count events in DB for this contract ---
      const eventsInDb = await countRawEventsForContract(db, contract.address);

      // --- count remaining on API ---
      let apiRemaining: number | null = null;
      try {
        apiRemaining = await fetcher.countEvents(
          [contract],
          afterTimestamp ?? null,
          contract.endTimestamp ?? null,
          afterBlock ?? null,
          beforeBlock ?? null,
        );
      } catch {
        /* ignore */
      }

      if (checkpoint != null) {
        log(`  │ Checkpoint: resuming from timestamp ${afterTimestamp}`);
      }

      log(
        `  │ In DB: ${eventsInDb.toLocaleString()} events` +
          (apiRemaining != null ? ` | On API (remaining): ~${apiRemaining.toLocaleString()}` : ""),
      );

      if (apiRemaining != null && apiRemaining <= 0) {
        log(`  └ ✓ Fully synced — nothing to fetch`);
        continue;
      }

      // --- fetch loop ---
      let totalFetched = 0;
      const fetchStart = Date.now();

      for await (const batch of fetcher.fetchAllEvents(
        [contract],
        afterTimestamp,
        contract.endTimestamp ?? null,
        afterBlock,
        beforeBlock,
      )) {
        await batchInsertRawEvents(db, batch);
        totalFetched += batch.length;

        const lastEvent = batch[batch.length - 1];
        if (lastEvent) {
          await updateCheckpoint(
            db,
            source.id,
            contract.address,
            lastEvent.txHash,
            lastEvent.timestamp,
            null,
          );
        }

        const shouldLog =
          apiRemaining != null && apiRemaining > 0
            ? totalFetched % 10_000 === 0 || totalFetched >= apiRemaining
            : totalFetched % 10_000 === 0;

        if (shouldLog) {
          if (apiRemaining != null && apiRemaining > 0) {
            const pct = Math.min(100, Math.round((totalFetched / apiRemaining) * 100));
            log(
              `  │ Fetching... ${totalFetched.toLocaleString()} / ~${apiRemaining.toLocaleString()} (${pct}%)`,
            );
          } else {
            log(`  │ Fetching... ${totalFetched.toLocaleString()} events`);
          }
        }
      }

      const elapsed = ((Date.now() - fetchStart) / 1000).toFixed(1);
      const finalCount = await countRawEventsForContract(db, contract.address);

      if (totalFetched > 0) {
        log(
          `  └ ✓ Done — fetched ${totalFetched.toLocaleString()} new events in ${elapsed}s (${finalCount.toLocaleString()} total in DB)`,
        );
      } else {
        log(`  └ ✓ Already synced — ${finalCount.toLocaleString()} events in DB`);
      }
    }
  }

  log("");
  log("Backfill complete.");
}

// ---------------------------------------------------------------------------
// Drop/recreate user schema, run handlers in event order
// ---------------------------------------------------------------------------
async function phase2ProcessHandlers(
  config: IndexerConfig,
  db: IndexerDb,
  chainClients: Map<string, ChainReader>,
): Promise<void> {
  setPhase("processing");
  log("Process raw events into user tables");

  if (config.schema) {
    log(`  Dropping & recreating schema "${config.schemaName}"...`);
    await syncUserSchema(db, config.schema, config.schemaName ?? "app");
  }

  if (config.onSetup) {
    await config.onSetup({ db, schema: config.schema ?? {} });
  }

  const eventIdentifiers = [...new Set(config.contracts.flatMap((c) => c.eventIdentifiers))];
  const totalRaw = await countRawEvents(db);

  if (totalRaw === 0) {
    log("  No raw events in DB — nothing to process.");
    log("Process complete.");
    return;
  }

  log(`  Raw events to process: ${totalRaw.toLocaleString()} [${eventIdentifiers.join(", ")}]`);

  let processed = 0;
  let _lastTimestamp: number | null = null;
  const procStart = Date.now();

  for await (const chunk of readRawEventsChunked(db, HANDLER_BATCH_SIZE)) {
    await db.transaction(async (tx) => {
      const insertBuffer = new Map();
      const batchedDb = createBatchedDb(tx as unknown as IndexerDb, insertBuffer);
      const ctx: HandlerContext = {
        db: batchedDb as unknown as IndexerDb,
        sourceId: chunk[0]?.sourceId ?? "",
        schema: config.schema ?? {},
        client: chainClients.get(chunk[0]?.sourceId ?? "") ?? null,
      };

      for (const event of chunk) {
        const handlerKey = `${event.address}:${event.identifier}`;
        const handler = config.handlers[handlerKey];
        if (!handler) continue;
        ctx.sourceId = event.sourceId;
        ctx.client = chainClients.get(event.sourceId) ?? null;
        await handler(event, ctx);
      }

      await flushInsertBuffer(tx as unknown as IndexerDb, insertBuffer);
    });

    processed += chunk.length;
    const lastInChunk = chunk[chunk.length - 1];
    if (lastInChunk) _lastTimestamp = lastInChunk.timestamp;
    addProcessed(chunk.length);

    const shouldLog = processed % 10_000 === 0 || chunk.length < HANDLER_BATCH_SIZE;
    if (shouldLog) {
      const pct = Math.round((processed / totalRaw) * 100);
      const elapsed = ((Date.now() - procStart) / 1000).toFixed(1);
      log(
        `  Processing... ${processed.toLocaleString()} / ${totalRaw.toLocaleString()} (${pct}%) — ${elapsed}s`,
      );
    }
  }

  const totalElapsed = ((Date.now() - procStart) / 1000).toFixed(1);
  log(`Process complete — ${processed.toLocaleString()} events processed in ${totalElapsed}s`);
}

// ---------------------------------------------------------------------------
// Gap fill + real-time WebSocket
// ---------------------------------------------------------------------------
async function phase3Realtime(
  config: IndexerConfig,
  db: IndexerDb,
  chainClients: Map<string, ChainReader>,
): Promise<KeplerWsClient | null> {
  log("Gap fill + real-time");

  let gapFilled = 0;
  for (const source of config.sources) {
    if (source.type !== "kepler") continue;

    const fetcher = new KeplerEsFetcher(source);
    const contracts = config.contracts.filter((c) => c.sourceId === source.id);

    for (const contract of contracts) {
      const checkpoint = await getCheckpoint(db, source.id);
      const afterTimestamp = checkpoint?.lastTimestamp ?? null;

      for await (const batch of fetcher.fetchAllEvents(
        [contract],
        afterTimestamp,
        undefined,
        null,
        null,
      )) {
        const toProcess: MultiversXEvent[] = [];
        for (const event of batch) {
          const alreadyExists = await eventExistsInRaw(db, event.id);
          if (alreadyExists) continue;
          await batchInsertRawEvents(db, [event]);
          toProcess.push(event);
          gapFilled++;
        }

        if (toProcess.length > 0) {
          await db.transaction(async (tx) => {
            const insertBuffer = new Map();
            const batchedDb = createBatchedDb(tx as unknown as IndexerDb, insertBuffer);
            for (const event of toProcess) {
              const handlerKey = `${event.address}:${event.identifier}`;
              const handler = config.handlers[handlerKey];
              if (handler) {
                await handler(event, {
                  db: batchedDb as unknown as IndexerDb,
                  sourceId: event.sourceId,
                  schema: config.schema ?? {},
                  client: chainClients.get(event.sourceId) ?? null,
                });
              }
            }
            await flushInsertBuffer(tx as unknown as IndexerDb, insertBuffer);
          });
        }

        const lastEvent = batch[batch.length - 1];
        if (lastEvent) {
          await updateCheckpoint(
            db,
            source.id,
            contract.address,
            lastEvent.txHash,
            lastEvent.timestamp,
            null,
          );
        }
      }
    }
  }

  if (gapFilled > 0) {
    log(`  Gap fill: ${gapFilled} new event(s) found and processed`);
  } else {
    log("  Gap fill: no new events since last checkpoint");
  }

  const keplerSource = config.sources.find((s) => s.type === "kepler") as
    | KeplerSourceConfig
    | undefined;
  if (!keplerSource) return null;

  const wsClient = new KeplerWsClient(keplerSource);
  const contracts = config.contracts.filter((c) => c.sourceId === keplerSource.id);

  wsClient.connect(
    contracts,
    async (event) => {
      const alreadyExists = await eventExistsInRaw(db, event.id);
      if (alreadyExists) return;

      await batchInsertRawEvents(db, [event]);

      const handlerKey = `${event.address}:${event.identifier}`;
      const handler = config.handlers[handlerKey];
      if (handler) {
        await handler(event, {
          db,
          sourceId: event.sourceId,
          schema: config.schema ?? {},
          client: chainClients.get(event.sourceId) ?? null,
        });
      }

      await updateCheckpoint(
        db,
        keplerSource.id,
        event.address,
        event.txHash,
        event.timestamp,
        null,
      );

      const contract = contracts.find((c) => c.address === event.address);
      const subscribedId = contract?.eventIdentifiers?.[0] ?? event.identifier;
      log(`  New event: ${subscribedId} tx=${event.txHash.slice(0, 16)}...`);
    },
    (error) => {
      console.error("WS error:", error);
    },
  );

  const totalEvents = await countRawEvents(db);
  setPhase("realtime");
  setReady(true);
  log(
    `  WebSocket connected — listening for new events (${totalEvents.toLocaleString()} total in DB)`,
  );
  return wsClient;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function startIndexer(config: IndexerConfig): Promise<{ close: () => Promise<void> }> {
  const dbUrl = config.database.url;
  const hasUrl = !!dbUrl;
  const dbLabel = dbUrl ? maskDbUrl(dbUrl) : (config.database.dataDir ?? "./indexer-data");
  const contractCount = config.contracts.length;
  const identifiers = [...new Set(config.contracts.flatMap((c) => c.eventIdentifiers))];

  log("═══════════════════════════════════════════════════");
  log(`Starting indexer`);
  log(`  DB: ${hasUrl ? "postgres" : "pglite"} → ${dbLabel}`);
  log(`  Contracts: ${contractCount} | Events: ${identifiers.join(", ")}`);
  log(`  Schema: ${config.schemaName ?? "default"}`);
  log("═══════════════════════════════════════════════════");

  const indexerDb = await createDatabase(config.database);
  await bootstrapInternalSchema(indexerDb.db);

  setHealthy(true);

  const healthServer = config.healthPort
    ? startHealthServer(
        config.healthPort,
        config.schema && config.schemaName
          ? {
              db: indexerDb.db,
              schema: config.schema,
              schemaName: config.schemaName,
            }
          : undefined,
      )
    : null;

  const chainClients = createChainClientsForConfig(config.sources, indexerDb.db);

  await phase1Backfill(config, indexerDb.db);
  await phase2ProcessHandlers(config, indexerDb.db, chainClients);
  const wsClient = await phase3Realtime(config, indexerDb.db, chainClients);

  log("═══════════════════════════════════════════════════");
  log("Indexer ready — all phases complete");
  log("═══════════════════════════════════════════════════");

  return {
    close: async () => {
      wsClient?.close();
      healthServer?.stop();
      await indexerDb.close();
      log("Indexer stopped.");
    },
  };
}

export async function reindex(config: IndexerConfig): Promise<void> {
  const dbUrl = config.database.url;
  const hasUrl = !!dbUrl;
  const dbLabel = dbUrl ? maskDbUrl(dbUrl) : (config.database.dataDir ?? "./indexer-data");
  log(`Reindexing (${hasUrl ? "postgres" : "pglite"}: ${dbLabel})...`);

  const indexerDb = await createDatabase(config.database);
  await bootstrapInternalSchema(indexerDb.db);

  const chainClients = createChainClientsForConfig(config.sources, indexerDb.db);

  await phase2ProcessHandlers(config, indexerDb.db, chainClients);

  await indexerDb.close();
  log("Reindex complete.");
}
