import { createProtocolNamespace, registerProtocolManifest, type ProtocolFabric, type ProtocolHandler } from "@kybernetria/pi-protocol";
import type { PipelineExecutor } from "../pipeline/execute.ts";
import { GENERATED_NODE_PREFIX, GENERATED_RUN_PROVIDE } from "../schemas.ts";
import type { PiProtocolManifest, PipelineRuntimeSnapshot } from "../types.ts";

export function registerGeneratedPipeline(
  fabric: ProtocolFabric,
  manifest: PiProtocolManifest,
  snapshot: PipelineRuntimeSnapshot,
  executor: PipelineExecutor,
): void {
  const handler: ProtocolHandler = async (input, context) => (await executor.execute(snapshot, input, context)).output;
  const protocol = createProtocolNamespace(manifest);
  const runHandler = GENERATED_RUN_PROVIDE;
  protocol.handler(runHandler);
  registerProtocolManifest(fabric, { manifest, handlers: { [runHandler]: handler } });
}

export function unregisterOwnedGeneratedNodes(fabric: ProtocolFabric): void {
  for (const node of fabric.registry().nodes) {
    if (node.packageId?.startsWith("pi-pe/generated/") || (node.nodeId.startsWith(GENERATED_NODE_PREFIX) && node.tags?.includes("generated"))) {
      fabric.unregister(node.nodeId);
    }
  }
}
