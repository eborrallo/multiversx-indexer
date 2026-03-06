import type { ContractConfig, KeplerSourceConfig } from "../config";
import { KEPLER_WS_URL } from "../constants";
import type { MultiversXEvent } from "../schema/types";
import { makeEventId } from "../schema/types";

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
    this.wsUrl = source.wsUrl ?? KEPLER_WS_URL;
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

        // Filter: keep only events whose first topic matches our subscription
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
