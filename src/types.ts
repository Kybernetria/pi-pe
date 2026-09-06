export interface JsonSchemaLite {
  type?: "string" | "number" | "integer" | "boolean" | "object" | "array" | "null";
  description?: string;
  required?: string[];
  properties?: Record<string, JsonSchemaLite>;
  additionalProperties?: boolean;
  items?: JsonSchemaLite;
  enum?: JsonValue[];
  [key: string]: unknown;
}

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export interface ProvideSnapshot {
  name: string;
  description: string;
  inputSchema: JsonSchemaLite;
  outputSchema: JsonSchemaLite;
  effects?: string[];
  effectsKnown?: boolean;
  tags?: string[];
  version?: string;
}

export type Assurance = "static" | "runtime_only" | "invalid";
export type DependencyPolicy = "pinned" | "compatible";

export interface PipelineLimits {
  maxSteps?: number;
  maxIntermediateBytes?: number;
  maxNestedDepth?: number;
  maxInvocations?: number;
}

export type ValueSource =
  | { source: "pipeline_input"; pointer?: string }
  | { source: "previous"; pointer?: string }
  | { source: "step"; stepId: string; pointer?: string };

export interface Binding { to: string; from: ValueSource; required?: boolean; }

export interface PipelineStepV1 {
  id: string;
  target: string;
  input:
    | { mode: "pass"; from: ValueSource }
    | { mode: "object"; bindings: Binding[]; constants?: Array<{ to: string; value: JsonValue }> };
}

export interface DependencySnapshot {
  target: string;
  nodeId: string;
  provide: string;
  packageId?: string;
  nodeVersion?: string;
  provideVersion?: string;
  effects: string[];
  effectsKnown?: boolean;
  inputSchema: JsonSchemaLite;
  outputSchema: JsonSchemaLite;
  fingerprint: string;
}

export interface PipelineReview { runtimeOnly: boolean; reviewedAt: string; }

export interface PipelineSpecV1 {
  schemaVersion: 1;
  id: string;
  version: string;
  name: string;
  description: string;
  tags: string[];
  inputSchema: JsonSchemaLite;
  outputSchema: JsonSchemaLite;
  limits?: PipelineLimits;
  dependencyPolicy?: DependencyPolicy;
  steps: PipelineStepV1[];
  output?: ValueSource;
  createdAt: string;
  updatedAt: string;
  dependencies?: DependencySnapshot[];
  review?: PipelineReview;
}

export interface Issue { code: string; message: string; path?: string; stepId?: string; target?: string; }
export interface ValidationReport { valid: boolean; assurance: Assurance; errors: Issue[]; warnings: Issue[]; dependencies: DependencySnapshot[]; }

export interface ResolvedTarget {
  provide: ProvideSnapshot;
  packageId?: string;
  nodeVersion?: string;
}
export type TargetResolver = (target: string) => ResolvedTarget | undefined;

export type PipelineEntryStatus = "enabled" | "disabled" | "quarantined";
export interface PipelineStatus {
  id: string;
  name?: string;
  version?: string;
  status: PipelineEntryStatus;
  assurance?: Assurance;
  issues: Issue[];
  updatedAt?: string;
}
export interface PipelineCard extends PipelineStatus { description?: string; tags?: string[]; stepCount?: number; dependencyPolicy?: DependencyPolicy; }

export interface PersistedIndexV1 { schemaVersion: 1; updatedAt: string; pipelines: PipelineStatus[]; }
export interface DryRunStep {
  stepId: string;
  target: string;
  input?: unknown;
  inputValid: boolean;
  error?: string;
  outputValid?: boolean;
  outputError?: string;
}
