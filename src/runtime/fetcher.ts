import type { ContractConfig, KeplerSourceConfig } from "../config";
import type { MultiversXEvent } from "../schema/types";
import { base64ToUtf8, makeEventId, utf8ToBase64 } from "../schema/types";

interface EsLogDoc {
  address: string;
  timestamp: number;
  events: Array<{
    identifier: string;
    topics: string[];
    data?: string | null;
    additionalData?: string[];
    order?: number;
  }>;
}

interface FetchResult {
  events: MultiversXEvent[];
  total: number;
  hasMore: boolean;
  /** Sort values from the last hit — pass as `searchAfter` to the next page. */
  lastSort: unknown[] | null;
}

export class KeplerEsFetcher {
  private esUrl: string;
  private apiKey: string;
  private batchSize: number;
  private sourceId: string;
  private requestDelayMs: number;

  constructor(source: KeplerSourceConfig) {
    this.esUrl = source.esUrl ?? "https://kepler-api.projectx.mx/mainnet/es";
    this.apiKey = source.apiKey;
    this.batchSize = source.batchSize ?? 100;
    this.sourceId = source.id;
    this.requestDelayMs = source.requestDelayMs ?? 200;
  }

  async fetchEvents(
    contracts: ContractConfig[],
    afterTimestamp?: number | null,
    beforeTimestamp?: number | null,
    afterBlock?: number | null,
    beforeBlock?: number | null,
    searchAfter?: unknown[] | null,
  ): Promise<FetchResult> {
    const filters: unknown[] = [];

    const addressFilters = contracts.map((c) => ({
      term: { address: c.address },
    }));
    if (addressFilters.length === 1) {
      filters.push(addressFilters[0]);
    } else {
      filters.push({
        bool: { should: addressFilters, minimum_should_match: 1 },
      });
    }

    const topicFilters = contracts.flatMap((c) =>
      c.eventIdentifiers.map((id) => ({
        term: { "events.topics.keyword": utf8ToBase64(id) },
      })),
    );
    if (topicFilters.length === 1) {
      filters.push(topicFilters[0]);
    } else if (topicFilters.length > 1) {
      filters.push({
        bool: { should: topicFilters, minimum_should_match: 1 },
      });
    }

    const rangeFilter: Record<string, unknown> = {};
    if (afterTimestamp != null) rangeFilter.gt = afterTimestamp;
    if (beforeTimestamp != null) rangeFilter.lte = beforeTimestamp;
    if (Object.keys(rangeFilter).length > 0) {
      filters.push({ range: { timestamp: rangeFilter } });
    }

    if (afterBlock != null || beforeBlock != null) {
      const blockRange: Record<string, number> = {};
      if (afterBlock != null) blockRange.gte = afterBlock;
      if (beforeBlock != null) blockRange.lte = beforeBlock;
      filters.push({ range: { block: blockRange } });
    }

    const body: Record<string, unknown> = {
      size: this.batchSize,
      track_total_hits: true,
      sort: [{ timestamp: "asc" }, { _doc: "asc" }],
      query: { bool: { filter: filters } },
      _source: ["address", "timestamp", "events"],
    };

    if (searchAfter != null) {
      body.search_after = searchAfter;
    }

    const res = await fetch(`${this.esUrl}/logs/_search`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Api-Key": this.apiKey,
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      throw new Error(`Kepler ES ${res.status}: ${await res.text()}`);
    }

    const result = (await res.json()) as {
      hits?: {
        hits?: Array<{
          _id?: string;
          _source?: EsLogDoc;
          sort?: unknown[];
        }>;
        total?: { value?: number };
      };
    };
    const hits = result.hits?.hits ?? [];
    const total = result.hits?.total?.value ?? 0;
    const lastSort = hits.length > 0 ? (hits[hits.length - 1]?.sort ?? null) : null;

    const events: MultiversXEvent[] = [];
    const eventIdSet = new Set(contracts.flatMap((c) => c.eventIdentifiers));

    for (const hit of hits) {
      const txHash = hit._id ?? "";
      const doc = hit._source;
      if (!doc?.events) continue;

      doc.events.forEach((ev, i) => {
        const isMatchByIdentifier = eventIdSet.has(ev.identifier);
        const decodedTopic = ev.topics?.[0] ? base64ToUtf8(ev.topics[0]) : "";
        const isMatchByTopic = eventIdSet.has(decodedTopic);

        if (!isMatchByIdentifier && !isMatchByTopic) return;

        const identifier = isMatchByIdentifier ? ev.identifier : decodedTopic;

        events.push({
          id: makeEventId(this.sourceId, txHash, ev.order ?? i),
          sourceId: this.sourceId,
          txHash,
          timestamp: doc.timestamp,
          address: doc.address,
          identifier,
          topics: ev.topics ?? [],
          data: ev.data ?? null,
          additionalData: ev.additionalData ?? [],
          eventIndex: ev.order ?? i,
        });
      });
    }

    return {
      events,
      total,
      hasMore: hits.length >= this.batchSize,
      lastSort,
    };
  }

  /**
   * Count matching log documents via the ES _count endpoint (no 10k cap, no docs returned).
   */
  async countEvents(
    contracts: ContractConfig[],
    afterTimestamp?: number | null,
    beforeTimestamp?: number | null,
    afterBlock?: number | null,
    beforeBlock?: number | null,
  ): Promise<number> {
    const filters: unknown[] = [];

    const addressFilters = contracts.map((c) => ({
      term: { address: c.address },
    }));
    if (addressFilters.length === 1) {
      filters.push(addressFilters[0]);
    } else {
      filters.push({
        bool: { should: addressFilters, minimum_should_match: 1 },
      });
    }

    const topicFilters = contracts.flatMap((c) =>
      c.eventIdentifiers.map((id) => ({
        term: { "events.topics.keyword": utf8ToBase64(id) },
      })),
    );
    if (topicFilters.length === 1) {
      filters.push(topicFilters[0]);
    } else if (topicFilters.length > 1) {
      filters.push({
        bool: { should: topicFilters, minimum_should_match: 1 },
      });
    }

    const rangeFilter: Record<string, unknown> = {};
    if (afterTimestamp != null) rangeFilter.gt = afterTimestamp;
    if (beforeTimestamp != null) rangeFilter.lte = beforeTimestamp;
    if (Object.keys(rangeFilter).length > 0) {
      filters.push({ range: { timestamp: rangeFilter } });
    }

    if (afterBlock != null || beforeBlock != null) {
      const blockRange: Record<string, number> = {};
      if (afterBlock != null) blockRange.gte = afterBlock;
      if (beforeBlock != null) blockRange.lte = beforeBlock;
      filters.push({ range: { block: blockRange } });
    }

    const res = await fetch(`${this.esUrl}/logs/_count`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Api-Key": this.apiKey,
      },
      body: JSON.stringify({
        query: { bool: { filter: filters } },
      }),
    });

    if (!res.ok) {
      throw new Error(`Kepler ES ${res.status}: ${await res.text()}`);
    }

    const result = (await res.json()) as { count?: number };
    return result.count ?? 0;
  }

  async *fetchAllEvents(
    contracts: ContractConfig[],
    afterTimestamp?: number | null,
    beforeTimestamp?: number | null,
    afterBlock?: number | null,
    beforeBlock?: number | null,
  ): AsyncGenerator<MultiversXEvent[], void, unknown> {
    let cursor: unknown[] | null = null;
    while (true) {
      const result = await this.fetchEvents(
        contracts,
        afterTimestamp,
        beforeTimestamp,
        afterBlock,
        beforeBlock,
        cursor,
      );
      if (result.events.length > 0) {
        yield result.events;
      }
      if (!result.hasMore || result.events.length === 0 || result.lastSort == null) break;
      cursor = result.lastSort;

      if (this.requestDelayMs > 0) {
        await new Promise((r) => setTimeout(r, this.requestDelayMs));
      }
    }
  }
}

/** Plain text → base64 topic. If already base64, returns as-is. */
function toBase64Topic(s: string): string {
  const t = s.trim();
  if (!t) return "";
  if (/^[A-Za-z0-9+/]+=*$/.test(t) && (t.includes("=") || t.length % 4 === 0)) return t;
  return Buffer.from(t, "utf8").toString("base64");
}

/**
 * Kepler WebSocket client for real-time events.
 *
 * Uses raw wss:// protocol. MultiversX notifier subscription format:
 * { subscriptionEntries: [{ address, topics: [base64] }] } — topics filter by first topic.
 * Incoming: { type: "all_events", data: [event1, event2] } (Kepler) or data: { hash, events } (notifier).
 */
export class KeplerWsClient {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private apiKey: string;
  private sourceId: string;
  private reconnectDelayMs = 5000;
  private shouldReconnect = true;
  private queue: MultiversXEvent[] = [];
  private processing = false;
  private onEventHandler: ((event: MultiversXEvent) => Promise<void>) | null = null;
  private onErrorHandler: ((error: Error) => void) | null = null;
  private contracts: ContractConfig[] = [];
  private onEventCb: ((event: MultiversXEvent) => Promise<void>) | null = null;
  private onErrorCb: ((error: Error) => void) | null = null;
  /** Base64 topics we subscribed to — filter incoming events by first topic. */
  private subscribedTopics = new Set<string>();

  private matchesSubscribedTopic(firstTopic: string): boolean {
    if (this.subscribedTopics.size === 0) return true;
    return this.subscribedTopics.has(firstTopic);
  }

  constructor(source: KeplerSourceConfig) {
    this.wsUrl = source.wsUrl ?? "wss://kepler-api.projectx.mx/mainnet/events";
    this.apiKey = source.apiKey;
    this.sourceId = source.id;
  }

  private async drainQueue(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    try {
      while (this.queue.length > 0) {
        const event = this.queue.shift();
        if (!event) break;
        try {
          await this.onEventHandler?.(event);
        } catch (err) {
          this.onErrorHandler?.(err as Error);
        }
      }
    } finally {
      this.processing = false;
    }
  }

  connect(
    contracts: ContractConfig[],
    onEvent: (event: MultiversXEvent) => Promise<void>,
    onError?: (error: Error) => void,
  ): void {
    this.onEventHandler = onEvent;
    this.onErrorHandler = onError ?? null;
    this.contracts = contracts;
    this.onEventCb = onEvent;
    this.onErrorCb = onError ?? null;

    this.ws = new WebSocket(this.wsUrl, {
      headers: { "Api-Key": this.apiKey },
    } as never);

    this.ws.onopen = () => {
      console.log(`WebSocket connected to ${this.wsUrl}`);
      const entries: Array<{ address: string; topics?: string[] }> = [];
      this.subscribedTopics.clear();
      for (const c of contracts) {
        const topics = (c.eventTopics ?? c.eventIdentifiers).map(toBase64Topic).filter(Boolean);
        if (topics.length > 0) {
          for (const topic of topics) {
            entries.push({ address: c.address, topics: [topic] });
            this.subscribedTopics.add(topic);
          }
        } else {
          entries.push({ address: c.address });
        }
      }
      const subscription = { subscriptionEntries: entries };
      console.log(`WebSocket subscribing to:`, JSON.stringify(subscription, null, 2));
      this.ws?.send(JSON.stringify(subscription));
    };

    this.ws.onmessage = (msg) => {
      try {
        const raw = typeof msg.data === "string" ? msg.data : String(msg.data);
        const packet = JSON.parse(raw);
        if (!packet) return;

        // Kepler: { type: "all_events", data: [event1, event2] } — data is the array
        // Notifier: { type, data: { hash, events: [...] } }
        const data = packet.data ?? packet;
        const events = Array.isArray(data) ? data : Array.isArray(data?.events) ? data.events : [];
        if (events.length === 0) return; // skip empty/heartbeat

        // Filter: keep only events whose first topic matches our subscription (same as tmp-ws-test)
        const filteredEvents = events.filter((ev: { topics?: string[] }) =>
          this.matchesSubscribedTopic(ev?.topics?.[0] ?? ""),
        );
        if (filteredEvents.length === 0) return;

        const blockHash = !Array.isArray(data) ? (data.hash ?? "") : "";
        const timestamp =
          (!Array.isArray(data) ? data.timestamp : null) ?? Math.floor(Date.now() / 1000);

        for (let i = 0; i < filteredEvents.length; i++) {
          const ev = filteredEvents[i] as Record<string, unknown>;
          const txHash = (ev.txHash as string) ?? (ev.hash as string) ?? blockHash;
          this.queue.push({
            id: makeEventId(this.sourceId, txHash, typeof ev.order === "number" ? ev.order : i),
            sourceId: this.sourceId,
            txHash,
            timestamp: (ev.timestamp as number) ?? timestamp,
            address: (ev.address as string) ?? "",
            identifier: (ev.identifier as string) ?? "",
            topics: Array.isArray(ev.topics) ? (ev.topics as string[]) : [],
            data: (ev.data as string) ?? null,
            additionalData: Array.isArray(ev.additionalData) ? (ev.additionalData as string[]) : [],
            eventIndex: typeof ev.order === "number" ? ev.order : i,
          });
        }
        this.drainQueue();
      } catch (err) {
        this.onErrorHandler?.(err as Error);
      }
    };

    this.ws.onclose = () => {
      console.log("WebSocket disconnected");
      const onEvent = this.onEventCb;
      if (this.shouldReconnect && this.contracts.length > 0 && onEvent) {
        console.log(`Reconnecting in ${this.reconnectDelayMs}ms...`);
        setTimeout(
          () => this.connect(this.contracts, onEvent, this.onErrorCb ?? undefined),
          this.reconnectDelayMs,
        );
      }
    };

    this.ws.onerror = () => {
      this.onErrorHandler?.(new Error("WebSocket connection error"));
    };
  }

  close(): void {
    this.shouldReconnect = false;
    this.ws?.close();
    this.ws = null;
  }
}
