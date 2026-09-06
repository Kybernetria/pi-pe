import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { PipelineError } from "../src/errors.ts";
import { PipelineService } from "../src/pipeline/service.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { fixture, registerHandler, registerMappedFixtures, TestToolRuntime } from "./helpers.ts";

async function setup() {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-registration-"));
  const runtime = new TestToolRuntime();
  registerMappedFixtures(runtime);
  const service = new PipelineService(runtime, new PipelineRepository(root));
  await service.initialize();
  return { root, runtime, service };
}

test("save persists an offline specification and registers no generated tool", async () => {
  const { root, runtime, service } = await setup();
  const saved = await service.save(await fixture("mapped.pipeline.json"));
  assert.equal(saved.status.status, "enabled");
  assert.equal(runtime.getTools().some((tool) => tool.name.startsWith("pi_pe_pipeline_")), false);
  const persisted = JSON.parse(await readFile(join(root, "pipelines", "mapped", "pipeline.json"), "utf8"));
  assert.equal(persisted.dependencies.length, 2);
  assert.equal(persisted.review.runtimeOnly, false);
  await service.dispose();
  const replacement = new PipelineService(runtime, new PipelineRepository(root));
  const statuses = await replacement.initialize();
  assert.equal(statuses[0]?.status, "enabled");
});

test("runtime-only save requires explicit review and delete requires confirmation", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-runtime-only-"));
  const runtime = new TestToolRuntime();
  registerHandler(runtime, "broad_run", {}, {}, (input) => input);
  const service = new PipelineService(runtime, new PipelineRepository(root));
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  const spec = { ...base, id: "reviewed", inputSchema: {}, outputSchema: {}, steps: [{ id: "run", target: "broad_run", input: { mode: "pass", from: { source: "pipeline_input" } } }] };
  await assert.rejects(() => service.save(spec), (error) => error instanceof PipelineError && /allowRuntimeOnly/.test(error.message));
  await service.save(spec, { allowRuntimeOnly: true });
  await assert.rejects(() => service.delete("reviewed", false), /confirm/);
  assert.deepEqual(await service.delete("reviewed", true), { deleted: true, id: "reviewed" });
});

test("corrupt specs are quarantined while unrelated pipelines remain available", async () => {
  const { root, service } = await setup();
  await service.save(await fixture("mapped.pipeline.json"));
  const bad = join(root, "pipelines", "corrupt");
  await import("node:fs/promises").then(({ mkdir, writeFile }) => mkdir(bad, { recursive: true }).then(() => writeFile(join(bad, "pipeline.json"), "{not-json", "utf8")));
  const statuses = await service.reload();
  assert.equal(statuses.find((item) => item.id === "corrupt")?.status, "quarantined");
  assert.equal(statuses.find((item) => item.id === "mapped")?.status, "enabled");
});
