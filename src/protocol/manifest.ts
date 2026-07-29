import { readFileSync } from "node:fs";
import {
  createProtocolNamespace,
  parseProtocolManifest,
  type ProtocolNamespace,
  type PiProtocolManifest,
} from "@kybernetria/pi-protocol";

export interface ManagementProtocolDefinition {
  manifest: PiProtocolManifest;
  namespace: ProtocolNamespace;
}

export function loadManagementProtocol(
  url = new URL("../../pi.protocol.json", import.meta.url),
): ManagementProtocolDefinition {
  const manifest = parseProtocolManifest(readFileSync(url, "utf8"));
  return { manifest, namespace: createProtocolNamespace(manifest) };
}

export function loadManagementManifest(url = new URL("../../pi.protocol.json", import.meta.url)): PiProtocolManifest {
  return loadManagementProtocol(url).manifest;
}
