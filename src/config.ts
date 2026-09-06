import { homedir } from "node:os";
import { resolve } from "node:path";

export const HARD_LIMITS = Object.freeze({
  maxSteps: 100,
  maxIntermediateBytes: 10 * 1024 * 1024,
  maxNestedDepth: 16,
  maxInvocations: 1_000,
});

export const DEFAULT_LIMITS = Object.freeze({
  maxSteps: 32,
  maxIntermediateBytes: 1024 * 1024,
  maxNestedDepth: 8,
  maxInvocations: 128,
});

export const MANAGEMENT_OUTPUT_MAX_BYTES = 50 * 1024;
export const CATALOG_DEFAULT_LIMIT = 50;
export const CATALOG_MAX_LIMIT = 200;
export const PREVIEW_MAX_CHARS = 256;
export const PIPELINE_MAX_DESCRIPTION_BYTES = 4_096;
export const PIPELINE_MAX_TAGS = 64;
export const PIPELINE_MAX_TAG_BYTES = 128;
export const PIPELINE_MAX_MAPPINGS_PER_STEP = 256;
export const PIPELINE_MAX_SCHEMA_DEPTH = 32;
export const PIPELINE_MAX_JSON_DEPTH = 64;

export function defaultStateDirectory(): string {
  const configured = process.env.PI_PE_STATE_DIR?.trim();
  return configured ? resolve(configured) : resolve(homedir(), ".pi", "agent", "state", "pi-pe");
}
