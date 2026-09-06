import type { ExtensionContext, ToolDefinition as PiToolDefinition, ToolInfo } from "@earendil-works/pi-coding-agent";
import type { JsonSchemaLite } from "./types.ts";

/** Read-only metadata exposed by Pi's native ExtensionAPI.getAllTools(). */
export type NativeToolMetadata = Pick<ToolInfo, "name" | "description" | "parameters" | "promptGuidelines" | "sourceInfo"> & {
  /** Optional test/host adapter metadata; Pi's native getAllTools() does not supply these fields. */
  inputSchema?: JsonSchemaLite;
  outputSchema?: JsonSchemaLite;
  effects?: string[];
  effectsKnown?: boolean;
  version?: string;
  tags?: string[];
};

/** A Pi tool definition used only to register Pi-PE's management tools. */
export type ManagementToolDefinition = PiToolDefinition<any, any, any> & {
  inputSchema?: JsonSchemaLite;
  outputSchema?: JsonSchemaLite;
  effects?: string[];
  effectsKnown?: boolean;
  version?: string;
  tags?: string[];
  purpose?: string;
};

export interface ToolRuntime {
  /** Metadata only. Pi-PE never receives native execute callbacks. */
  getTools(): NativeToolMetadata[];
  registerTool(definition: ManagementToolDefinition): void;
}

export function schemaForTool(tool: NativeToolMetadata | ManagementToolDefinition): JsonSchemaLite {
  if ("inputSchema" in tool && tool.inputSchema) return tool.inputSchema;
  return normalizeToolSchema(tool.parameters);
}

/** Convert the JSON-shaped TypeBox schema to the deliberately small local schema subset. */
function normalizeToolSchema(value: unknown): JsonSchemaLite {
  if (!isRecord(value)) return {};
  const result: JsonSchemaLite = {};
  for (const key of ["type", "description", "required", "additionalProperties", "enum"] as const) {
    if (value[key] !== undefined) result[key] = value[key] as never;
  }
  if (isRecord(value.properties)) {
    result.properties = Object.fromEntries(Object.entries(value.properties).map(([key, child]) => [key, normalizeToolSchema(child)]));
  }
  if (value.items !== undefined) result.items = normalizeToolSchema(value.items);
  return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function jsonToolResult(output: unknown): { content: [{ type: "text"; text: string }]; details: unknown } {
  let text: string;
  try { text = JSON.stringify(output); } catch { text = String(output); }
  return { content: [{ type: "text", text }], details: output };
}

export type { ExtensionContext };
