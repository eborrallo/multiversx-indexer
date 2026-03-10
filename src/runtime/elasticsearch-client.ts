import type { ContractConfig, KeplerSourceConfig } from "../config";
import { KEPLER_BATCH_SIZE, KEPLER_ES_URL, KEPLER_REQUEST_DELAY_MS } from "../constants";
import type { MultiversXEvent } from "../schema/types";
import { base64ToUtf8, makeEventId, utf8ToBase64 } from "../schema/types";

interface EsLogDoc {
  address: string;
  timestamp: number;
  events: Array<{
    identifier: string;
    address?: string;
    topics: string[];
    data?: string | null;
    additionalData?: string[];
    order?: number;
  }>;
}

export interface FetchResult {
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
    this.esUrl = source.esUrl ?? KEPLER_ES_URL;
    this.apiKey = source.apiKey;
    this.batchSize = source.batchSize ?? KEPLER_BATCH_SIZE;
    this.sourceId = source.id;
    this.requestDelayMs = source.requestDelayMs ?? KEPLER_REQUEST_DELAY_MS;
  }

  async fetchEvents(
    contracts: ContractConfig[],
    afterTimestamp?: number | null,
    beforeTimestamp?: number | null,
    afterBlock?: number | null,
    beforeBlock?: number | null,
    searchAfter?: unknown[] | null,
  ): Promise<FetchResult> {
    const filters = this.buildFilters(
      contracts,
      afterTimestamp,
      beforeTimestamp,
      afterBlock,
      beforeBlock,
    );

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

    const events = this.parseHitsToEvents(hits, contracts);

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
    const filters = this.buildFilters(
      contracts,
      afterTimestamp,
      beforeTimestamp,
      afterBlock,
      beforeBlock,
    );

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

  private buildFilters(
    contracts: ContractConfig[],
    afterTimestamp?: number | null,
    beforeTimestamp?: number | null,
    afterBlock?: number | null,
    beforeBlock?: number | null,
  ): unknown[] {
    const filters: unknown[] = [];

    const addresses = [...new Set(contracts.map((c) => c.address).filter(Boolean))];
    if (addresses.length === 1) {
      filters.push({ term: { "events.address": addresses[0] } });
    } else if (addresses.length > 1) {
      filters.push({
        bool: {
          should: addresses.map((a) => ({ term: { "events.address": a } })),
          minimum_should_match: 1,
        },
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

    return filters;
  }

  private parseHitsToEvents(
    hits: Array<{ _id?: string; _source?: EsLogDoc }>,
    contracts: ContractConfig[],
  ): MultiversXEvent[] {
    const events: MultiversXEvent[] = [];
    const eventIdSet = new Set(contracts.flatMap((c) => c.eventIdentifiers));
    const addressSet = new Set(contracts.map((c) => c.address));

    for (const hit of hits) {
      const txHash = hit._id ?? "";
      const doc = hit._source;
      if (!doc?.events) continue;

      doc.events.forEach((ev, i) => {
        const evAddress = ev.address ?? doc.address;
        if (evAddress && !addressSet.has(evAddress)) return;

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
          address: evAddress ?? doc.address,
          identifier,
          topics: ev.topics ?? [],
          data: ev.data ?? null,
          additionalData: ev.additionalData ?? [],
          eventIndex: ev.order ?? i,
        });
      });
    }

    return events;
  }
}
