import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MANAGEMENT_OUTPUT_MAX_BYTES } from "../config.ts";
import { PipelineError } from "../errors.ts";
import { jsonToolResult, type ManagementToolDefinition, type ToolRuntime } from "../tools.ts";
import type { JsonSchemaLite } from "../types.ts";
import type { PipelineService } from "../pipeline/service.ts";
import { catalogProvides, describeTarget, type CatalogFilter } from "./catalog.ts";

export const MANAGEMENT_TOOL_NAMES = [
  "pi_pe_catalog", "pi_pe_describe_target", "pi_pe_validate_pipeline", "pi_pe_save_pipeline", "pi_pe_get_pipeline",
  "pi_pe_list_pipelines", "pi_pe_delete_pipeline", "pi_pe_dry_run_mapping", "pi_pe_reload_pipelines",
] as const;

export function createManagementTools(runtime: ToolRuntime, service: PipelineService): ManagementToolDefinition[] {
  const management = new Set<string>(MANAGEMENT_TOOL_NAMES);
  const tool = (
    name: string,
    description: string,
    snippet: string,
    guidelines: string[],
    inputSchema: JsonSchemaLite,
    run: (input: unknown, context: ExtensionContext, signal: AbortSignal | undefined, toolCallId: string) => Promise<unknown>,
  ): ManagementToolDefinition => ({
    name,
    label: name,
    description,
    promptSnippet: snippet,
    promptGuidelines: guidelines,
    parameters: inputSchema as never,
    inputSchema,
    tags: ["pipelines", "management"],
    purpose: "Offline Pi-PE pipeline authoring and inspection",
    async execute(toolCallId, input, signal, _onUpdate, ctx) {
      return jsonToolResult(await run(input, ctx, signal, toolCallId));
    },
  });

  return [
    tool(
      "pi_pe_catalog",
      "List or search the native Pi tools visible to this session. Returns metadata only; it never runs a tool.",
      "List or search native Pi tool metadata without executing tools",
      ["Use pi_pe_catalog to discover native Pi tool names and input schemas before authoring a pipeline."],
      {
        type: "object",
        properties: {
          query: { type: "string", maxLength: 2_000 },
          nodeId: { type: "string", maxLength: 128 },
          tags: { type: "array", items: { type: "string", maxLength: 256 } },
          executionType: { type: "string", enum: ["native"] },
          effects: { type: "array", items: { type: "string", maxLength: 256 } },
          limit: { type: "integer", minimum: 1, maximum: 100 },
        },
        additionalProperties: false,
      },
      async (input) => bound(catalogProvides(runtime, management, asRecord(input) as CatalogFilter)),
    ),
    tool(
      "pi_pe_describe_target",
      "Describe one exact native Pi tool's name, description, input schema, prompt guidance, and metadata limitations.",
      "Describe one exact native Pi tool without executing it",
      ["Use pi_pe_describe_target to inspect a target contract before mapping data into it."],
      { type: "object", required: ["target"], properties: { target: { type: "string" } }, additionalProperties: false },
      async (input) => {
        const target = requiredString(asRecord(input), "target");
        const description = describeTarget(runtime, management, target);
        if (!description) throw new PipelineError("DEPENDENCY_NOT_FOUND", `native Pi tool is not configured: ${target}`);
        return bound(description);
      },
    ),
    tool(
      "pi_pe_validate_pipeline",
      "Validate a data-only pipeline without writing files or invoking native Pi tools.",
      "Validate a data-only pipeline without writes or tool execution",
      ["Use pi_pe_validate_pipeline before saving a pipeline; validation is offline and does not call targets."],
      { type: "object", required: ["spec"], properties: { spec: { type: "object", additionalProperties: true } }, additionalProperties: false },
      async (input) => bound(await service.validate(asRecord(input).spec)),
    ),
    tool(
      "pi_pe_save_pipeline",
      "Validate and persist a data-only pipeline. Saving does not register or execute a generated tool.",
      "Validate and save a data-only pipeline specification",
      ["Use pi_pe_save_pipeline only after reviewing its validation report; saved pipelines remain offline specifications."],
      { type: "object", required: ["spec"], properties: { spec: { type: "object", additionalProperties: true }, allowRuntimeOnly: { type: "boolean" } }, additionalProperties: false },
      async (input) => {
        const request = asRecord(input);
        return bound(await service.save(request.spec, { allowRuntimeOnly: request.allowRuntimeOnly === true }));
      },
    ),
    tool(
      "pi_pe_get_pipeline",
      "Retrieve one saved offline pipeline specification and its current native metadata dependency status.",
      "Get one saved offline pipeline and dependency status",
      ["Use pi_pe_get_pipeline to inspect persisted pipeline data and whether its native metadata is still available."],
      { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
      async (input) => {
        const request = asRecord(input);
        const pipeline = service.get(requiredString(request, "id"));
        if (!pipeline) throw new PipelineError("PIPELINE_NOT_FOUND", `pipeline not found: ${request.id}`);
        return bound(pipeline);
      },
    ),
    tool(
      "pi_pe_list_pipelines",
      "List saved offline pipelines and their validation or native metadata status.",
      "List saved offline pipeline specifications",
      ["Use pi_pe_list_pipelines to review saved pipeline status without invoking any target."],
      { type: "object", additionalProperties: false },
      async () => bound({ pipelines: service.list() }),
    ),
    tool(
      "pi_pe_delete_pipeline",
      "Delete one saved offline pipeline only when confirm is true.",
      "Delete one saved pipeline after explicit confirmation",
      ["Use pi_pe_delete_pipeline only when the user explicitly confirms deletion and pass confirm true."],
      { type: "object", required: ["id", "confirm"], properties: { id: { type: "string" }, confirm: { type: "boolean" } }, additionalProperties: false },
      async (input) => {
        const request = asRecord(input);
        return bound(await service.delete(requiredString(request, "id"), request.confirm === true));
      },
    ),
    tool(
      "pi_pe_dry_run_mapping",
      "Construct and validate pipeline step inputs from supplied values without invoking native Pi tools.",
      "Preview pipeline mappings without invoking targets",
      ["Use pi_pe_dry_run_mapping to inspect constructed inputs and output selection before any external execution outside Pi-PE."],
      { type: "object", required: ["pipelineInput"], properties: { id: { type: "string" }, spec: { type: "object" }, stepOutputs: { type: "object" }, pipelineInput: {} }, additionalProperties: false },
      async (input) => {
        const request = asRecord(input);
        return bound(await service.dryRun({ ...(typeof request.id === "string" ? { id: request.id } : {}), ...(request.spec !== undefined ? { spec: request.spec } : {}), pipelineInput: request.pipelineInput, ...(isRecord(request.stepOutputs) ? { stepOutputs: request.stepOutputs } : {}) }));
      },
    ),
    tool(
      "pi_pe_reload_pipelines",
      "Re-read saved offline pipeline specifications and refresh their validation status; no tools are registered or run.",
      "Reload saved offline pipeline specifications",
      ["Use pi_pe_reload_pipelines after native extension changes to refresh offline dependency metadata."],
      { type: "object", additionalProperties: false },
      async () => bound({ pipelines: await service.reload() }),
    ),
  ];
}

function bound<T>(value: T): T | { truncated: true; totalBytes: number; preview: string } {
  let text: string;
  try { text = JSON.stringify(value); } catch (error) { throw new PipelineError("PIPELINE_INVALID", `management output is not serializable: ${error instanceof Error ? error.message : String(error)}`); }
  const bytes = Buffer.byteLength(text, "utf8");
  if (bytes <= MANAGEMENT_OUTPUT_MAX_BYTES) return value;
  const envelope = (preview: string) => ({ truncated: true as const, totalBytes: bytes, preview });
  let preview = Buffer.from(text).subarray(0, MANAGEMENT_OUTPUT_MAX_BYTES).toString("utf8");
  let envelopeBytes = Buffer.byteLength(JSON.stringify(envelope(preview)), "utf8");
  while (envelopeBytes > MANAGEMENT_OUTPUT_MAX_BYTES && preview) {
    const excess = envelopeBytes - MANAGEMENT_OUTPUT_MAX_BYTES;
    const previewBytes = Buffer.byteLength(preview, "utf8");
    preview = Buffer.from(preview).subarray(0, Math.max(0, previewBytes - Math.max(excess, 1))).toString("utf8");
    envelopeBytes = Buffer.byteLength(JSON.stringify(envelope(preview)), "utf8");
  }
  return envelope(preview);
}
function asRecord(value: unknown): Record<string, unknown> { if (!isRecord(value)) throw new PipelineError("PIPELINE_INVALID", "management input must be an object"); return value; }
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function requiredString(value: Record<string, unknown>, key: string): string { const selected = value[key]; if (typeof selected !== "string" || !selected.trim()) throw new PipelineError("PIPELINE_INVALID", `${key} must be a non-empty string`); return selected; }
