import type { IndexerDb } from "./db";
import { createGraphQLHandler } from "./graphql";

export interface GraphQLServerOptions {
  db: IndexerDb;
  schema: Record<string, unknown>;
  schemaName: string;
}

export interface HealthState {
  healthy: boolean;
  ready: boolean;
  phase: "idle" | "backfill" | "processing" | "realtime";
  eventsProcessed: number;
  startedAt: number;
}

const state: HealthState = {
  healthy: false,
  ready: false,
  phase: "idle",
  eventsProcessed: 0,
  startedAt: Date.now(),
};

export function getHealthState(): HealthState {
  return { ...state };
}

export function setHealthy(v: boolean) {
  state.healthy = v;
}
export function setReady(v: boolean) {
  state.ready = v;
}
export function setPhase(p: HealthState["phase"]) {
  state.phase = p;
}
export function addProcessed(n: number) {
  state.eventsProcessed += n;
}

export function startHealthServer(
  port: number = 42069,
  options?: GraphQLServerOptions,
): ReturnType<typeof Bun.serve> {
  const graphqlHandler = options ? createGraphQLHandler(options) : null;

  const server = Bun.serve({
    port,
    fetch(req) {
      const url = new URL(req.url);

      if (url.pathname === "/graphql" && graphqlHandler) {
        return graphqlHandler(req);
      }

      if (url.pathname === "/health") {
        return Response.json(
          { healthy: state.healthy, phase: state.phase },
          { status: state.healthy ? 200 : 503 },
        );
      }

      if (url.pathname === "/ready") {
        return Response.json(
          {
            ready: state.ready,
            phase: state.phase,
            eventsProcessed: state.eventsProcessed,
            uptimeMs: Date.now() - state.startedAt,
          },
          { status: state.ready ? 200 : 503 },
        );
      }

      return new Response("Not Found", { status: 404 });
    },
  });

  console.log(`Health server listening on http://localhost:${port}`);
  if (graphqlHandler) {
    const base = `http://localhost:${port}/graphql`;
    console.log(`GraphQL endpoint: ${base}`);
    console.log(`GraphQL playground: ${base} (open in browser)`);
  }
  return server;
}
