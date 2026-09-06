import type { ManagementToolDefinition, ToolRuntime } from "../tools.ts";
import type { PipelineService } from "../pipeline/service.ts";
import { createManagementTools } from "./handlers.ts";

export function registerManagementTools(runtime: ToolRuntime, service: PipelineService): ManagementToolDefinition[] {
  const tools = createManagementTools(runtime, service);
  for (const tool of tools) runtime.registerTool(tool);
  return tools;
}
