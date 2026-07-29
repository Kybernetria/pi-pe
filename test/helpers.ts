import { readFile } from "node:fs/promises";
import type { JsonSchemaLite, ProtocolFabric, ProtocolHandler, ProtocolNode } from "@kybernetria/pi-protocol";
import type { PipelineSpecV1, ResolvedTarget, TargetResolver } from "../src/types.ts";

export async function fixture(name: string): Promise<PipelineSpecV1> {
  return JSON.parse(await readFile(new URL(`../fixtures/${name}`, import.meta.url), "utf8")) as PipelineSpecV1;
}

export function registerHandler(
  fabric: ProtocolFabric,
  nodeId: string,
  provide: string,
  inputSchema: JsonSchemaLite,
  outputSchema: JsonSchemaLite,
  handler: ProtocolHandler,
  effects: string[] = [],
  version = "1.0.0",
): void {
  const node: ProtocolNode = {
    protocolVersion: "0.2.0",
    nodeId,
    packageId: `test/${nodeId}`,
    version,
    purpose: `Test node ${nodeId}`,
    provides: [{
      name: provide,
      description: `Test provide ${provide}`,
      inputSchema,
      outputSchema,
      execution: { type: "handler", handler: provide },
      effects,
      version,
    }],
  };
  fabric.register({ node, handlers: { [provide]: handler } });
}

export function registerMappedFixtures(fabric: ProtocolFabric, order: string[] = []): void {
  registerHandler(
    fabric,
    "fixture",
    "upper",
    { type: "object", required: ["text"], properties: { text: { type: "string" } } },
    { type: "object", required: ["value"], properties: { value: { type: "string" } } },
    (input) => {
      order.push("upper");
      return { value: String((input as { text: string }).text).toUpperCase() };
    },
  );
  // A protocol node cannot be registered twice, so add wrap by replacing fixture with both provides.
  fabric.unregister("fixture");
  fabric.register({
    node: {
      protocolVersion: "0.2.0",
      nodeId: "fixture",
      packageId: "test/fixture",
      version: "1.0.0",
      purpose: "Mapped fixture targets",
      provides: [
        {
          name: "upper",
          description: "Uppercase text",
          inputSchema: { type: "object", required: ["text"], properties: { text: { type: "string" } } },
          outputSchema: { type: "object", required: ["value"], properties: { value: { type: "string" } } },
          execution: { type: "handler", handler: "upper" },
        },
        {
          name: "wrap",
          description: "Wrap a value",
          inputSchema: {
            type: "object",
            required: ["value", "prefix"],
            properties: { value: { type: "string" }, prefix: { type: "string" } },
          },
          outputSchema: { type: "object", required: ["result"], properties: { result: { type: "string" } } },
          execution: { type: "handler", handler: "wrap" },
          effects: ["test_write"],
        },
      ],
    },
    handlers: {
      upper: (input) => {
        order.push("upper");
        return { value: String((input as { text: string }).text).toUpperCase() };
      },
      wrap: (input) => {
        order.push("wrap");
        const value = input as { value: string; prefix: string };
        return { result: `${value.prefix}${value.value}` };
      },
    },
  });
}

export function resolverFrom(fabric: ProtocolFabric): TargetResolver {
  return (target): ResolvedTarget | undefined => {
    const [nodeId, provide] = target.split(".");
    const snapshot = fabric.describeProvide(nodeId, provide);
    const node = fabric.describeNode(nodeId);
    return snapshot && node ? { provide: snapshot, packageId: node.packageId, nodeVersion: node.version } : undefined;
  };
}
