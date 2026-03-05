/**
 * Re-exports reindex from pipeline for the plan-specified module layout.
 * This module exists so users can import { reindex } from "runtime/reindex"
 * as described in the framework layout. The implementation lives in pipeline.ts
 * to share the phase2ProcessHandlers logic with startIndexer.
 */
export { reindex } from "./pipeline";
