/**
 * Default values used when config/options are null or undefined.
 * Centralizes fallbacks for consistency and easier tuning.
 */

/** Kepler Elasticsearch API base URL (mainnet). */
export const KEPLER_ES_URL = "https://kepler-api.projectx.mx/mainnet/es";

/** Kepler WebSocket events URL (mainnet). */
export const KEPLER_WS_URL = "wss://kepler-api.projectx.mx/mainnet/events";

/** MultiversX public API base URL (mainnet). Use testnet-api.multiversx.com for testnet. */
export const MULTIVERSX_API_URL = "https://api.multiversx.com";

/** PGlite data directory when no postgres URL is provided. */
export const DEFAULT_DATA_DIR = "./indexer-data";

/** Default schema name for processed (user) tables when not set. */
export const DEFAULT_SCHEMA_NAME = "app";

/** Empty schema object fallback. */
export const EMPTY_SCHEMA: Record<string, unknown> = Object.freeze({});

/** Empty string fallback for sourceId. */
export const EMPTY_SOURCE_ID = "";

/** Kepler ES fetch batch size (events per request). */
export const KEPLER_BATCH_SIZE = 10_000;

/** Delay in ms between Kepler ES requests (rate limiting). */
export const KEPLER_REQUEST_DELAY_MS = 200;

/** Handler processing batch size (events per transaction). */
export const HANDLER_BATCH_SIZE = 10_000;

/** Raw event insert batch size. */
export const RAW_BATCH_SIZE = 500;

/** Default config file path. */
export const DEFAULT_CONFIG_PATH = "indexer.config.ts";

/** GraphQL list query default limit. */
export const GRAPHQL_DEFAULT_LIMIT = 100;

/** GraphQL list query max limit. */
export const GRAPHQL_MAX_LIMIT = 1000;

/** GraphQL order direction default. */
export const GRAPHQL_DEFAULT_ORDER_DIRECTION = "asc" as const;
