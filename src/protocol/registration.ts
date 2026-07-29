import { registerProtocolManifest, type ProtocolFabric } from "@kybernetria/pi-protocol";
import type { PipelineService } from "../pipeline/service.ts";
import { createManagementHandlers } from "./handlers.ts";
import { loadManagementProtocol } from "./manifest.ts";

export function registerManagementNode(fabric: ProtocolFabric, service: PipelineService): void {
  const { manifest, namespace } = loadManagementProtocol();
  fabric.unregister(namespace.nodeId);
  registerProtocolManifest(fabric, {
    manifest,
    handlers: createManagementHandlers(fabric, service, namespace),
  });
}
