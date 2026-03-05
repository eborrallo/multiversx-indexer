import type { EventHandler } from "multiverse-indexer";
import { base64ToUtf8 } from "multiverse-indexer";
import type * as schema from "../schema";

/** Example handler: persist events. Use context.client for read-only chain/contract calls. */
export const handleExampleEvent: EventHandler = async (event, context) => {
  const dataB64 = event.topics[0];
  const data = dataB64 ? base64ToUtf8(dataB64) : "";

  const table = (context.schema as typeof schema).exampleEvents;
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
