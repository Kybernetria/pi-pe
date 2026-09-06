import { readFile } from "node:fs/promises";
import type { PipelineSpecV1, ResolvedTarget, TargetResolver, JsonSchemaLite } from "../src/types.ts";
import type { ManagementToolDefinition, NativeToolMetadata, ToolRuntime } from "../src/tools.ts";
import { jsonToolResult } from "../src/tools.ts";
import { snapshotForNativeTool } from "../src/management/catalog.ts";
import { validateJsonSchemaValue } from "../src/schemas.ts";

type Handler = (input: unknown, context?: { signal?: AbortSignal }) => unknown | Promise<unknown>;

export class TestToolRuntime implements ToolRuntime {
  private readonly tools = new Map<string, ManagementToolDefinition & NativeToolMetadata>();
  getTools(): NativeToolMetadata[] { return [...this.tools.values()]; }
  registerTool(definition: ManagementToolDefinition): void {
    this.tools.set(definition.name, {
      ...definition,
      sourceInfo: { path: `<test:${definition.name}>`, source: "test", scope: "temporary", origin: "top-level" },
    });
  }
  async call(name: string, input: unknown, options: { ctx?: unknown; signal?: AbortSignal } = {}): Promise<unknown> {
    const tool = this.tools.get(name);
    if (!tool) throw new Error(`tool not found: ${name}`);
    const error = validateJsonSchemaValue(tool.inputSchema ?? (tool.parameters as unknown as JsonSchemaLite), input, `tool ${name} input`);
    if (error) throw new Error(error);
    return tool.execute(name, input as never, options.signal, undefined, options.ctx as never);
  }
}

export async function fixture(name: string): Promise<PipelineSpecV1> {
  return JSON.parse(await readFile(new URL(`../fixtures/${name}`, import.meta.url), "utf8")) as PipelineSpecV1;
}

export function registerHandler(runtime: TestToolRuntime, name: string, inputSchema: JsonSchemaLite, outputSchema: JsonSchemaLite, handler: Handler, effects: string[] = [], version = "1.0.0"): ManagementToolDefinition {
  const definition: ManagementToolDefinition = {
    name,
    label: name,
    description: `Test tool ${name}`,
    parameters: inputSchema as never,
    inputSchema,
    outputSchema,
    effects,
    effectsKnown: true,
    version,
    async execute(_id, input, signal) { return jsonToolResult(await handler(input, { signal })); },
  };
  runtime.registerTool(definition);
  return definition;
}

export function registerMappedFixtures(runtime: TestToolRuntime, order: string[] = []): void {
  registerHandler(runtime, "fixture_upper", { type: "object", required: ["text"], properties: { text: { type: "string" } } }, { type: "object", required: ["value"], properties: { value: { type: "string" } } }, (input) => { order.push("upper"); return { value: String((input as { text: string }).text).toUpperCase() }; });
  registerHandler(runtime, "fixture_wrap", { type: "object", required: ["value", "prefix"], properties: { value: { type: "string" }, prefix: { type: "string" } } }, { type: "object", required: ["result"], properties: { result: { type: "string" } } }, (input) => { order.push("wrap"); const value = input as { value: string; prefix: string }; return { result: `${value.prefix}${value.value}` }; }, ["fs.write"]);
}

export function resolverFrom(runtime: TestToolRuntime): TargetResolver {
  return (target): ResolvedTarget | undefined => {
    const tool = runtime.getTools().find((candidate) => candidate.name === target);
    return tool ? { provide: snapshotForNativeTool(tool), packageId: `test/${tool.name}`, nodeVersion: tool.version } : undefined;
  };
}
