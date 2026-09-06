import { CATALOG_DEFAULT_LIMIT, CATALOG_MAX_LIMIT } from "../config.ts";
import type { NativeToolMetadata, ToolRuntime } from "../tools.ts";
import { schemaForTool } from "../tools.ts";
import type { ResolvedTarget, TargetResolver } from "../types.ts";
import { fingerprintTarget } from "../pipeline/fingerprints.ts";

export interface CatalogFilter { query?: string; nodeId?: string; tags?: string[]; executionType?: "native"; effects?: string[]; limit?: number; }
export interface CatalogCard {
  target: string;
  nodeId: string;
  provide: string;
  description: string;
  purpose: string;
  promptGuidelines: string[];
  tags: string[];
  executionType: "native";
  effects: string[];
  effectsKnown: false;
  management: boolean;
  source: string;
}

export function createToolTargetResolver(runtime: ToolRuntime): TargetResolver {
  return (target) => {
    const tool = findTool(runtime, target);
    if (!tool) return undefined;
    return {
      provide: snapshotForNativeTool(tool),
      packageId: `pi-native/${tool.sourceInfo.source}`,
    };
  };
}

export function catalogProvides(runtime: ToolRuntime, managementTools: ReadonlySet<string>, filter: CatalogFilter = {}): { items: CatalogCard[]; total: number; truncated: boolean } {
  const query = filter.query?.trim().toLowerCase();
  const requestedTags = filter.tags?.map((tag) => tag.toLowerCase()) ?? [];
  const requestedEffects = filter.effects?.map((effect) => effect.toLowerCase()) ?? [];
  const limit = Math.min(Math.max(filter.limit ?? CATALOG_DEFAULT_LIMIT, 1), CATALOG_MAX_LIMIT);
  const matches = runtime.getTools().map((tool) => cardForTool(tool, managementTools)).filter((card) => {
    if (filter.nodeId && card.nodeId !== filter.nodeId) return false;
    if (filter.executionType && card.executionType !== filter.executionType) return false;
    if (requestedTags.length && !requestedTags.every((tag) => card.tags.some((candidate) => candidate.toLowerCase() === tag))) return false;
    if (requestedEffects.length && !requestedEffects.every((effect) => card.effects.some((candidate) => candidate.toLowerCase() === effect))) return false;
    if (query) {
      const searchable = `${card.target} ${card.description} ${card.purpose} ${card.tags.join(" ")} ${card.effects.join(" ")}`.toLowerCase();
      if (!query.split(/\s+/).every((term) => searchable.includes(term))) return false;
    }
    return true;
  }).sort((left, right) => left.target.localeCompare(right.target));
  return { items: matches.slice(0, limit), total: matches.length, truncated: matches.length > limit };
}

export function describeTarget(runtime: ToolRuntime, managementTools: ReadonlySet<string>, target: string): {
  target: string;
  node: { nodeId: string; purpose: string; tags: string[] };
  provide: ReturnType<typeof snapshotForNativeTool>;
  fingerprint: string;
  management: boolean;
  limitations: string[];
  promptGuidelines: string[];
} | undefined {
  const tool = findTool(runtime, target);
  if (!tool) return undefined;
  const provide = snapshotForNativeTool(tool);
  const tags = tagsForTool(tool);
  return {
    target,
    node: { nodeId: tool.name, purpose: tool.description, tags },
    provide,
    fingerprint: fingerprintTarget({ provide, packageId: `pi-native/${tool.sourceInfo.source}` }),
    management: managementTools.has(tool.name),
    promptGuidelines: [...(tool.promptGuidelines ?? [])],
    limitations: [
      "Pi SDK metadata exposes the native input schema but not an output schema, effects, version, or callable execute function.",
      "Pi-PE therefore validates output compatibility as runtime-only and never invokes this target.",
    ],
  };
}

export function snapshotForNativeTool(tool: NativeToolMetadata): ResolvedTarget["provide"] {
  return {
    name: tool.name,
    description: tool.description,
    inputSchema: schemaForTool(tool),
    outputSchema: tool.outputSchema ?? {},
    ...(tool.effects ? { effects: [...tool.effects] } : {}),
    effectsKnown: tool.effectsKnown ?? false,
    tags: [...new Set(["native", ...(tool.tags ?? [])])],
  };
}

function findTool(runtime: ToolRuntime, target: string): NativeToolMetadata | undefined {
  return runtime.getTools().find((candidate) => candidate.name === target);
}

function cardForTool(tool: NativeToolMetadata, managementTools: ReadonlySet<string>): CatalogCard {
  return {
    target: tool.name,
    nodeId: tool.name,
    provide: tool.name,
    description: tool.description,
    purpose: tool.description,
    promptGuidelines: [...(tool.promptGuidelines ?? [])],
    tags: tagsForTool(tool),
    executionType: "native",
    effects: [],
    effectsKnown: false,
    management: managementTools.has(tool.name),
    source: tool.sourceInfo.source,
  };
}

function tagsForTool(tool: NativeToolMetadata): string[] {
  return ["native", ...(tool.sourceInfo.source === "builtin" ? ["builtin"] : []), ...(tool.tags ?? [])];
}
