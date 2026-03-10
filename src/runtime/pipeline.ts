import type { EventHandler, HandlerContext, IndexerConfig, KeplerSourceConfig } from "../config";
import {
  DEFAULT_DATA_DIR,
  DEFAULT_SCHEMA_NAME,
  EMPTY_SCHEMA,
  EMPTY_SOURCE_ID,
  HANDLER_BATCH_SIZE,
} from "../constants";
import type { MultiversXEvent } from "../schema/types";
import { createBatchedDb, flushInsertBuffer } from "./batched-db";
import type { ChainReader } from "./chain-client";
import { createChainClientsForConfig } from "./chain-client";
import type { IndexerDb } from "./db";
import { bootstrapInternalSchema, createDatabase, maskDbUrl } from "./db";
import { getDeploymentBlock, getLatestBlock } from "./deployment-block";
import { KeplerEsFetcher } from "./elasticsearch-client";
import { addProcessed, setHealthy, setPhase, setReady, startHealthServer } from "./health";
import { syncUserSchema } from "./schema-sync";
import {
  batchInsertRawEvents,
  buildAllowedEventKeys,
  countRawEvents,
  countRawEventsForContract,
  eventExistsInRaw,
  getCheckpointForContract,
  purgeOrphanedData,
  readRawEventsChunked,
  resetInternalTables,
  updateCheckpoint,
} from "./store";
import { KeplerWsClient } from "./websocket-client";

function log(msg: string) {
  console.log(msg);
}

/** Resolve handler for an event by exact address:identifier match. */
function resolveHandler(
  event: MultiversXEvent,
  handlers: Record<string, EventHandler>,
): EventHandler | null {
  const key = `${event.address}:${event.identifier}`;
  return handlers[key] ?? null;
}

// ---------------------------------------------------------------------------
// Backfill raw events from Kepler ES (delta only)
// ---------------------------------------------------------------------------
async function phase1Backfill(
  config: IndexerConfig,
  db: IndexerDb,
  _chainClients: Map<string, ChainReader>,
  options?: { isDev?: boolean },
): Promise<void> {
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
      const checkpoint = await getCheckpointForContract(
        db,
        source.id,
        contract.address,
        contract.eventIdentifiers,
      );
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

      // --- count events in DB by contract address ---
      const eventsInDb = await countRawEventsForContract(db, contract.address);

      // --- count remaining on API per event type ---
      const apiRemainingByEvent = new Map<string, number>();
      let apiRemainingTotal: number | null = null;
      try {
        for (const evId of contract.eventIdentifiers) {
          const count = await fetcher.countEvents(
            [{ ...contract, eventIdentifiers: [evId] }],
            afterTimestamp ?? null,
            contract.endTimestamp ?? null,
            afterBlock ?? null,
            beforeBlock ?? null,
          );
          apiRemainingByEvent.set(evId, count);
        }
        apiRemainingTotal = [...apiRemainingByEvent.values()].reduce((a, b) => a + b, 0);
      } catch {
        /* ignore */
      }

      if (checkpoint != null) {
        log(`  │ Checkpoint: resuming from timestamp ${afterTimestamp}`);
      }

      log(
        `  │ In DB: ${eventsInDb.toLocaleString()} events` +
          (apiRemainingTotal != null
            ? ` | On API (remaining): ~${apiRemainingTotal.toLocaleString()}`
            : ""),
      );

      if (apiRemainingTotal != null && apiRemainingTotal <= 0) {
        log(`  └ ✓ Fully synced — nothing to fetch`);
        continue;
      }

      // --- fetch loop ---
      const fetchedByEvent = new Map<string, number>();
      for (const evId of contract.eventIdentifiers) {
        fetchedByEvent.set(evId, 0);
      }
      let totalFetched = 0;
      const fetchStart = Date.now();
      const useProgressBar =
        (options?.isDev ?? false) &&
        process.stdout.isTTY &&
        apiRemainingTotal != null &&
        apiRemainingTotal > 0;
      const barWidth = 20;

      let hasRenderedBar = false;
      function renderBackfillBar() {
        if (!useProgressBar || apiRemainingTotal == null) return;
        const elapsed = ((Date.now() - fetchStart) / 1000).toFixed(1);
        const lines: string[] = [];
        const seenThisRender = new Set<string>();
        for (const evId of contract.eventIdentifiers) {
          const label = `${addrShort} ${evId}`;
          if (seenThisRender.has(label)) continue;
          seenThisRender.add(label);
          const fetched = fetchedByEvent.get(evId) ?? 0;
          const remaining = apiRemainingByEvent.get(evId) ?? 0;
          const pct = remaining > 0 ? Math.min(1, fetched / remaining) : 1;
          const filled = Math.round(barWidth * pct);
          const bar = "█".repeat(filled) + "░".repeat(barWidth - filled);
          const labelPadded = label.padEnd(42).slice(0, 42);
          lines.push(
            `  │ ${labelPadded} [${bar}] ${fetched.toLocaleString()}/~${remaining.toLocaleString()} (${Math.round(pct * 100)}%)`,
          );
        }
        const n = lines.length;
        const moveUp = hasRenderedBar && n > 0 ? `\x1b[${n}A\r` : "";
        process.stdout.write(`${moveUp}${lines.join("\n")} — ${elapsed}s\x1b[K`);
        hasRenderedBar = true;
      }

      for await (const batch of fetcher.fetchAllEvents(
        [contract],
        afterTimestamp,
        contract.endTimestamp ?? null,
        afterBlock,
        beforeBlock,
      )) {
        await batchInsertRawEvents(db, batch);
        for (const ev of batch) {
          const cur = fetchedByEvent.get(ev.identifier) ?? 0;
          fetchedByEvent.set(ev.identifier, cur + 1);
        }
        totalFetched += batch.length;

        const lastEvent = batch[batch.length - 1];
        if (lastEvent) {
          await updateCheckpoint(
            db,
            source.id,
            contract.address,
            contract.eventIdentifiers,
            lastEvent.txHash,
            lastEvent.timestamp,
            null,
          );
        }

        if (useProgressBar) {
          renderBackfillBar();
        } else {
          const shouldLog =
            apiRemainingTotal != null && apiRemainingTotal > 0
              ? totalFetched % 10_000 === 0 || totalFetched >= apiRemainingTotal
              : totalFetched % 10_000 === 0;

          if (shouldLog) {
            if (apiRemainingTotal != null && apiRemainingTotal > 0) {
              const pct = Math.min(100, Math.round((totalFetched / apiRemainingTotal) * 100));
              log(
                `  │ Fetching... ${totalFetched.toLocaleString()} / ~${apiRemainingTotal.toLocaleString()} (${pct}%)`,
              );
            } else {
              log(`  │ Fetching... ${totalFetched.toLocaleString()} events`);
            }
          }
        }
      }

      if (useProgressBar) process.stdout.write("\n");

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
  options?: { isDev?: boolean },
): Promise<void> {
  setPhase("processing");
  log("Process raw events into user tables");

  if (config.schema) {
    log(`  Dropping & recreating schema "${config.schemaName}"...`);
    await syncUserSchema(db, config.schema, config.schemaName ?? "app");
  }

  if (config.onSetup) {
    await config.onSetup({ db, schema: config.schema ?? EMPTY_SCHEMA });
  }

  const eventIdentifiers = [...new Set(config.contracts.flatMap((c) => c.eventIdentifiers))];
  const totalRaw = await countRawEvents(db);

  if (totalRaw === 0) {
    log("  No raw events in DB — nothing to process.");
    log("Process complete.");
    return;
  }

  const useProgressBar = (options?.isDev ?? false) && process.stdout.isTTY;
  const barWidth = 24;

  log(`  Raw events to process: ${totalRaw.toLocaleString()} [${eventIdentifiers.join(", ")}]`);

  let processed = 0;
  let _lastTimestamp: number | null = null;
  const procStart = Date.now();

  function renderProgressBar() {
    if (!useProgressBar) return;
    const pct = totalRaw > 0 ? processed / totalRaw : 1;
    const filled = Math.round(barWidth * pct);
    const bar = "█".repeat(filled) + "░".repeat(barWidth - filled);
    const elapsed = ((Date.now() - procStart) / 1000).toFixed(1);
    process.stdout.write(
      `\r  [${bar}] ${processed.toLocaleString()}/${totalRaw.toLocaleString()} (${Math.round(pct * 100)}%) — ${elapsed}s\x1b[K`,
    );
  }

  if (useProgressBar) renderProgressBar();

  for await (const chunk of readRawEventsChunked(db, HANDLER_BATCH_SIZE)) {
    await db.transaction(async (tx) => {
      const insertBuffer = new Map();
      const batchedDb = createBatchedDb(tx as unknown as IndexerDb, insertBuffer);
      const ctx: HandlerContext = {
        db: batchedDb as unknown as IndexerDb,
        sourceId: chunk[0]?.sourceId ?? EMPTY_SOURCE_ID,
        schema: config.schema ?? EMPTY_SCHEMA,
        client: chainClients.get(chunk[0]?.sourceId ?? EMPTY_SOURCE_ID) ?? null,
      };

      for (const event of chunk) {
        const handler = resolveHandler(event, config.handlers);
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

    if (useProgressBar) {
      renderProgressBar();
    } else {
      const shouldLog = processed % 10_000 === 0 || chunk.length < HANDLER_BATCH_SIZE;
      if (shouldLog) {
        const pct = Math.round((processed / totalRaw) * 100);
        const elapsed = ((Date.now() - procStart) / 1000).toFixed(1);
        log(
          `  Processing... ${processed.toLocaleString()} / ${totalRaw.toLocaleString()} (${pct}%) — ${elapsed}s`,
        );
      }
    }
  }

  if (useProgressBar) {
    process.stdout.write("\n");
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
      const checkpoint = await getCheckpointForContract(
        db,
        source.id,
        contract.address,
        contract.eventIdentifiers,
      );
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
              const handler = resolveHandler(event, config.handlers);
              if (handler) {
                await handler(event, {
                  db: batchedDb as unknown as IndexerDb,
                  sourceId: event.sourceId,
                  schema: config.schema ?? EMPTY_SCHEMA,
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
            contract.eventIdentifiers,
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

      const handler = resolveHandler(event, config.handlers);
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
        [event.identifier],
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

export async function startIndexer(
  config: IndexerConfig,
  options?: { isDev?: boolean },
): Promise<{ close: () => Promise<void> }> {
  const dbUrl = config.database.url;
  const hasUrl = !!dbUrl;
  const dbLabel = dbUrl ? maskDbUrl(dbUrl) : (config.database.dataDir ?? DEFAULT_DATA_DIR);
  const contractCount = config.contracts.length;
  const identifiers = [...new Set(config.contracts.flatMap((c) => c.eventIdentifiers))];

  log("═══════════════════════════════════════════════════");
  log(`Starting indexer`);
  log(`  DB: ${hasUrl ? "postgres" : "pglite"} → ${dbLabel}`);
  log(`  Contracts: ${contractCount} | Events: ${identifiers.join(", ")}`);
  log(`  Schema: ${config.schemaName ?? DEFAULT_SCHEMA_NAME}`);
  log("═══════════════════════════════════════════════════");

  const indexerDb = await createDatabase(config.database);
  await bootstrapInternalSchema(indexerDb.db);

  if (config.resetInternalTables) {
    await resetInternalTables(indexerDb.db);
    log("  Internal tables reset (raw_events, checkpoint cleared)");
  } else {
    const allowedKeys = buildAllowedEventKeys(config.contracts);
    const { rawEventsDeleted, checkpointsDeleted } = await purgeOrphanedData(
      indexerDb.db,
      allowedKeys,
    );
    if (rawEventsDeleted > 0 || checkpointsDeleted > 0) {
      log(
        `  Config sync: removed ${rawEventsDeleted.toLocaleString()} orphaned raw events, ${checkpointsDeleted} checkpoint(s)`,
      );
    }
  }

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

  await phase1Backfill(config, indexerDb.db, chainClients, options);
  await phase2ProcessHandlers(config, indexerDb.db, chainClients, options);
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

export async function reindex(config: IndexerConfig, options?: { isDev?: boolean }): Promise<void> {
  const dbUrl = config.database.url;
  const hasUrl = !!dbUrl;
  const dbLabel = dbUrl ? maskDbUrl(dbUrl) : (config.database.dataDir ?? DEFAULT_DATA_DIR);
  log(`Reindexing (${hasUrl ? "postgres" : "pglite"}: ${dbLabel})...`);

  const indexerDb = await createDatabase(config.database);
  await bootstrapInternalSchema(indexerDb.db);

  if (config.resetInternalTables) {
    await resetInternalTables(indexerDb.db);
    log("  Internal tables reset (raw_events, checkpoint cleared)");
  } else {
    const allowedKeys = buildAllowedEventKeys(config.contracts);
    const { rawEventsDeleted, checkpointsDeleted } = await purgeOrphanedData(
      indexerDb.db,
      allowedKeys,
    );
    if (rawEventsDeleted > 0 || checkpointsDeleted > 0) {
      log(
        `  Config sync: removed ${rawEventsDeleted.toLocaleString()} orphaned raw events, ${checkpointsDeleted} checkpoint(s)`,
      );
    }
  }

  const chainClients = createChainClientsForConfig(config.sources, indexerDb.db);

  await phase2ProcessHandlers(config, indexerDb.db, chainClients, options);

  await indexerDb.close();
  log("Reindex complete.");
}
