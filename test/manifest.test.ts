import assert from "node:assert/strict";
import test from "node:test";
import { createProtocolFabric, registerProtocolManifest } from "@kybernetria/pi-protocol";
import { createGeneratedManifest } from "../src/generated/manifest.ts";
import { createManagementHandlers } from "../src/protocol/handlers.ts";
import { loadManagementManifest, loadManagementProtocol } from "../src/protocol/manifest.ts";
import { PipelineService } from "../src/pipeline/service.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { fixture } from "./helpers.ts";

const EXPECTED = [
  "catalog", "describe_target", "validate_pipeline", "save_pipeline", "get_pipeline", "list_pipelines",
  "delete_pipeline", "run_pipeline", "dry_run_mapping", "reload_pipelines",
];

test("static manifest is protocol 0.2.0 and every provide has a handler", () => {
  const manifest = loadManagementManifest();
  assert.equal(manifest.protocolVersion, "0.2.0");
  assert.deepEqual(manifest.provides.map((provide) => provide.name), EXPECTED);
  assert(manifest.provides.every((provide) => provide.execution.type === "handler"));

  const fabric = createProtocolFabric();
  const service = new PipelineService(fabric, new PipelineRepository("/tmp/pi-pe-manifest-test-unused"));
  const handlers = createManagementHandlers(fabric, service, loadManagementProtocol().namespace);
  assert.deepEqual(Object.keys(handlers), EXPECTED);
  assert.doesNotThrow(() => registerProtocolManifest(fabric, { manifest, handlers }));
});

test("generated manifest exactly exposes the declared business contract", async () => {
  const spec = await fixture("mapped.pipeline.json");
  spec.dependencies = [{
    target: "fixture.wrap",
    nodeId: "fixture",
    provide: "wrap",
    execution: { type: "handler", handler: "wrap" },
    effects: ["file_write"],
    inputSchema: { type: "object" },
    outputSchema: { type: "object" },
    fingerprint: "test",
  }];
  const manifest = createGeneratedManifest(spec);
  assert.equal(manifest.nodeId, "pi_pe_pipeline_mapped");
  assert.equal(manifest.provides.length, 1);
  assert.equal(manifest.provides[0].name, "run");
  assert.deepEqual(manifest.provides[0].inputSchema, spec.inputSchema);
  assert.deepEqual(manifest.provides[0].outputSchema, spec.outputSchema);
  assert.deepEqual(manifest.provides[0].execution, { type: "handler", handler: "run" });
  assert.deepEqual(manifest.provides[0].effects, ["file_write", "protocol_invoke"]);
});
