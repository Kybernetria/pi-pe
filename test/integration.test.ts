import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { PipelineService } from "../src/pipeline/service.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { registerManagementTools } from "../src/management/registration.ts";
import { fixture, registerMappedFixtures, TestToolRuntime } from "./helpers.ts";

test("offline lifecycle catalogs, validates, stores, maps, and deletes without generated tools", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-integration-"));
  const runtime = new TestToolRuntime();
  registerMappedFixtures(runtime);
  const service = new PipelineService(runtime, new PipelineRepository(root));
  registerManagementTools(runtime, service);
  await service.initialize();
  const call = (name: string, input: unknown) => runtime.call(name, input, { ctx: {} });

  const catalog = await call("pi_pe_catalog", { query: "fixture" });
  assert.deepEqual((catalog as any).details.total >= 2, true);
  const spec = await fixture("mapped.pipeline.json");
  const validation = await call("pi_pe_validate_pipeline", { spec });
  assert.equal((validation as any).details.assurance, "static");
  const saved = await call("pi_pe_save_pipeline", { spec });
  assert.equal((saved as any).details.status.status, "enabled");
  assert.equal(runtime.getTools().some((tool) => tool.name.startsWith("pi_pe_pipeline_")), false);

  const mapping = await call("pi_pe_dry_run_mapping", { id: "mapped", pipelineInput: { text: "offline" }, stepOutputs: { upper: { value: "OFFLINE" }, wrap: { result: "Result: OFFLINE" } } });
  assert.deepEqual((mapping as any).details.output, { result: "Result: OFFLINE" });
  const listed = await call("pi_pe_list_pipelines", {});
  assert.equal((listed as any).details.pipelines.length, 1);
  await call("pi_pe_delete_pipeline", { id: "mapped", confirm: true });
  assert.equal(runtime.getTools().some((tool) => tool.name.startsWith("pi_pe_pipeline_")), false);
});
