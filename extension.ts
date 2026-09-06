import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { ToolRuntime } from "./src/tools.ts";
import { PipelineService } from "./src/pipeline/service.ts";
import { registerManagementTools } from "./src/management/registration.ts";

/**
 * Pi-PE is intentionally limited to offline pipeline authoring. The Pi SDK
 * exposes getAllTools() for native metadata, but no arbitrary native invoke
 * API, so this extension does not create a dispatcher or generated executors.
 */
export default async function piPipelineEngineExtension(pi: ExtensionAPI): Promise<void> {
  const runtime: ToolRuntime = {
    getTools: () => pi.getAllTools(),
    registerTool: (definition) => pi.registerTool(definition),
  };
  const service = new PipelineService(runtime);
  registerManagementTools(runtime, service);
  pi.on("session_start", async () => { await service.initialize(); });
  pi.on("session_shutdown", async () => { await service.dispose(); });
}
