async function invokeResult(fabric: { invokeTracked(request: any): Promise<any> }, request: any): Promise<any> {
  return (await fabric.invokeTracked(request)).result;
}

import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createProtocolFabric } from "@kybernetria/pi-protocol";
import { PipelineError } from "../src/errors.ts";
import { disposeRegistrationBounded } from "../extension.ts";
import { PipelineService } from "../src/pipeline/service.ts";
import { registerManagementNode } from "../src/protocol/registration.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { fixture, registerHandler, registerMappedFixtures } from "./helpers.ts";

test("saved parent and child pipelines retain pinned generated fingerprints", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-nested-fingerprint-"));
  const fabric = createProtocolFabric();
  registerHandler(fabric, "nested_fixture", "echo", { type: "string" }, { type: "string" }, (input) => input);
  const service = new PipelineService(fabric, new PipelineRepository(root));
  await service.initialize();
  const child = await fixture("passthrough.pipeline.json");
  const childSpec = { ...child, id: "child", steps: [{ id: "echo", target: "nested_fixture.echo", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }] };
  await service.save(childSpec);
  const parent = { ...childSpec, id: "parent", name: "Parent", steps: [{ id: "child", target: "pi_pe_pipeline_child.run", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }] };
  await service.save(parent);
  const status = service.get("parent")?.dependencyStatus;
  assert.equal(status?.usable, true, JSON.stringify(status?.issues));
  const result = await service.run("parent", "nested");
  assert.equal(result.output, "nested");
});

test("child replacement waits for an active pinned parent generation", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-nested-replacement-"));
  const fabric = createProtocolFabric();
  let entered!: () => void;
  let release!: () => void;
  const enteredPromise = new Promise<void>((resolve) => { entered = resolve; });
  const releasePromise = new Promise<void>((resolve) => { release = resolve; });
  registerHandler(fabric, "replacement_fixture", "echo", { type: "string" }, { type: "string" }, async (input) => {
    entered();
    await releasePromise;
    return input;
  });
  const service = new PipelineService(fabric, new PipelineRepository(root));
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  const child = { ...base, id: "child", steps: [{ id: "echo", target: "replacement_fixture.echo", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }] };
  await service.save(child);
  const parent = {
    ...child,
    id: "parent",
    name: "Parent",
    steps: [{ id: "child", target: "pi_pe_pipeline_child.run", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }],
  };
  await service.save(parent);

  const running = service.run("parent", "pinned");
  await enteredPromise;
  let replacementFinished = false;
  const replacement = service.save({ ...child, name: "Child replacement" }).then(() => { replacementFinished = true; });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(replacementFinished, false);
  release();
  assert.equal((await running).output, "pinned");
  await replacement;
});

test("dispose aborts active execution before waiting for a held mutation mutex", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-shutdown-mutation-"));
  const fabric = createProtocolFabric();
  let handlerStarted!: () => void;
  const started = new Promise<void>((resolve) => { handlerStarted = resolve; });
  registerHandler(fabric, "shutdown_mutation", "wait", { type: "string" }, { type: "string" }, async (input, context) => {
    handlerStarted();
    await new Promise<void>((resolve, reject) => {
      context?.abortSignal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true });
    });
    return input;
  });
  const service = new PipelineService(fabric, new PipelineRepository(root));
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  await service.save({
    ...base,
    id: "shutdown-mutation",
    steps: [{ id: "wait", target: "shutdown_mutation.wait", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }],
  });
  const running = service.run("shutdown-mutation", "x");
  await started;

  let releaseMutation!: () => void;
  const mutationReleased = new Promise<void>((resolve) => { releaseMutation = resolve; });
  let mutationHeld!: () => void;
  const mutationHeldPromise = new Promise<void>((resolve) => { mutationHeld = resolve; });
  const holding = service.repository.withMutationLock(async () => {
    mutationHeld();
    await mutationReleased;
  });
  await mutationHeldPromise;
  const reload = service.reload();
  await new Promise((resolve) => setTimeout(resolve, 10));

  const disposal = service.dispose();
  const runOutcome = await Promise.race([
    running.then(() => "completed", (error) => error),
    new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 500)),
  ]);
  assert.notEqual(runOutcome, "timed-out", "dispose must abort execution before mutex acquisition");
  assert(runOutcome instanceof PipelineError && runOutcome.code === "PIPELINE_ABORTED");

  releaseMutation();
  await holding;
  await assert.rejects(reload, /shutting down/);
  await disposal;
});

test("dispose returns degraded by its deadline when mutation ownership never drains", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-shutdown-stuck-mutation-"));
  const fabric = createProtocolFabric();
  const service = new PipelineService(fabric, new PipelineRepository(root));
  await service.initialize();
  let mutationHeld!: () => void;
  const mutationHeldPromise = new Promise<void>((resolve) => { mutationHeld = resolve; });
  let releaseMutation!: () => void;
  const mutationReleased = new Promise<void>((resolve) => { releaseMutation = resolve; });
  const holding = service.repository.withMutationLock(async () => {
    mutationHeld();
    await mutationReleased;
  });
  await mutationHeldPromise;
  const reload = service.reload();
  await new Promise((resolve) => setTimeout(resolve, 10));

  const startedAt = performance.now();
  const result = await service.dispose();
  assert.equal(result.status, "degraded");
  assert.match(result.registrations[0]?.error ?? "", /mutation mutex acquisition/);
  assert(performance.now() - startedAt < 6_000);

  await assert.rejects(() => service.reload(), /shutting down/);
  releaseMutation();
  await holding;
  await assert.rejects(reload, /shutting down/);
  assert.equal(fabric.registry().nodes.some((node) => node.nodeId.startsWith("pi_pe_pipeline_")), false);
});

test("management registration disposal remains bounded", async () => {
  const startedAt = performance.now();
  const result = await disposeRegistrationBounded({ dispose: async () => new Promise<void>(() => undefined) }, 20);
  assert.equal(result, false);
  assert(performance.now() - startedAt < 500);
});

test("dispose reports degraded when a handler ignores abort", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-shutdown-degraded-"));
  const fabric = createProtocolFabric();
  let finishHandler!: () => void;
  const handlerFinished = new Promise<void>((resolve) => { finishHandler = resolve; });
  registerHandler(fabric, "shutdown_fixture", "wait", { type: "string" }, { type: "string" }, async () => {
    await handlerFinished;
    return "finished";
  });
  const originalInstall = fabric.install.bind(fabric);
  const boundedFabric = new Proxy(fabric, {
    get(target, property, receiver) {
      if (property !== "install") return Reflect.get(target, property, receiver);
      return (definition: unknown, bindings: unknown, metadata: { packageId?: string }) => {
        const registration = originalInstall(definition as never, bindings as never, metadata);
        if (!metadata.packageId?.startsWith("pi-pe/generated/")) return registration;
        return {
          registrationId: registration.registrationId,
          nodeId: registration.nodeId,
          get generation() { return registration.generation; },
          get contractDigest() { return registration.contractDigest; },
          replace: (...args: Parameters<typeof registration.replace>) => registration.replace(...args),
          dispose: async () => new Promise<void>(() => undefined),
        };
      };
    },
  });
  const service = new PipelineService(boundedFabric, new PipelineRepository(root));
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  await service.save({
    ...base,
    id: "shutdown",
    steps: [{ id: "wait", target: "shutdown_fixture.wait", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }],
  });
  const running = fabric.invokeTracked({ nodeId: "pi_pe_pipeline_shutdown", provide: "run", input: "x" });
  await new Promise((resolve) => setTimeout(resolve, 10));
  const startedAt = performance.now();
  const outcome = await service.dispose();
  finishHandler();
  await running.catch(() => undefined);
  assert.equal(outcome.status, "degraded");
  assert.equal(outcome.registrations[0]?.status, "timed_out");
  assert(performance.now() - startedAt < 6_000);
});

test("generated pipelines reject transitive reentrant mutation management calls", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-reentrant-proxy-"));
  const fabric = createProtocolFabric();
  const service = new PipelineService(fabric, new PipelineRepository(root));
  const management = registerManagementNode(fabric, service);
  registerHandler(fabric, "mutation_proxy", "run", { type: "object", additionalProperties: true }, { type: "object", additionalProperties: true }, async (input) => {
    const result = await fabric.invokeTracked({ nodeId: "pi_pe", provide: "save_pipeline", input: { spec: (input as { spec: unknown }).spec } });
    if (!result.result.ok) throw new Error(result.result.error.message);
    return result.result.output;
  });
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  await service.save({
    ...base,
    id: "reentrant-proxy",
    inputSchema: { type: "object", required: ["spec"], properties: { spec: { type: "object", additionalProperties: true } } },
    outputSchema: { type: "object", additionalProperties: true },
    steps: [{ id: "proxy", target: "mutation_proxy.run", input: { mode: "pass" as const, from: { source: "pipeline_input" as const } } }],
  }, { allowRuntimeOnly: true });
  const outcome = await Promise.race([
    fabric.invokeTracked({ nodeId: "pi_pe_pipeline_reentrant-proxy", provide: "run", input: { spec: {} } }),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 1_000)),
  ]);
  assert.notEqual(outcome, null, "transitive mutation must fail instead of deadlocking");
  if (outcome && !outcome.result.ok) assert.match(outcome.result.error.message, /active pipelines cannot invoke mutation/);
  await service.dispose();
  await management.dispose();
});

test("generated pipelines reject reentrant mutation management calls", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-reentrant-save-"));
  const fabric = createProtocolFabric();
  const service = new PipelineService(fabric, new PipelineRepository(root));
  const management = registerManagementNode(fabric, service);
  await service.initialize();
  const base = await fixture("passthrough.pipeline.json");
  await service.save({
    ...base,
    id: "reentrant-save",
    inputSchema: { type: "object", required: ["spec"], properties: { spec: { type: "object", additionalProperties: true } } },
    outputSchema: { type: "object", additionalProperties: true },
    steps: [{
      id: "save",
      target: "pi_pe.save_pipeline",
      input: { mode: "object", bindings: [{ to: "/spec", from: { source: "pipeline_input", pointer: "/spec" }, required: true }] },
    }],
  }, { allowRuntimeOnly: true });

  const outcome = await Promise.race([
    fabric.invokeTracked({ nodeId: "pi_pe_pipeline_reentrant-save", provide: "run", input: { spec: {} } }),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 1_000)),
  ]);
  assert.notEqual(outcome, null, "reentrant mutation must fail instead of deadlocking");
  if (outcome && !outcome.ok) assert.match(outcome.error.message, /mutation management target/);
  await service.dispose();
  await management.dispose();
});

test("complete lifecycle works through management protocol provides", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-integration-"));
  const fabric = createProtocolFabric();
  registerMappedFixtures(fabric);
  const service = new PipelineService(fabric, new PipelineRepository(root));
  registerManagementNode(fabric, service);
  await service.initialize();

  const catalog = await invokeResult(fabric, { nodeId: "pi_pe", provide: "catalog", input: { query: "fixture", generated: false } });
  assert.equal(catalog.ok, true);
  if (catalog.ok) assert((catalog.output as { total: number }).total >= 2);

  const described = await invokeResult(fabric, { nodeId: "pi_pe", provide: "describe_target", input: { target: "fixture.upper" } });
  assert.equal(described.ok, true, described.ok ? "" : `${described.error.code}: ${described.error.message}`);
  if (described.ok) assert.equal(typeof (described.output as { fingerprint: string }).fingerprint, "string");

  const spec = await fixture("mapped.pipeline.json");
  const validation = await invokeResult(fabric, { nodeId: "pi_pe", provide: "validate_pipeline", input: { spec } });
  assert.equal(validation.ok, true);
  if (validation.ok) assert.equal((validation.output as { assurance: string }).assurance, "static");

  const saved = await invokeResult(fabric, { nodeId: "pi_pe", provide: "save_pipeline", input: { spec } });
  assert.equal(saved.ok, true, saved.ok ? "" : saved.error.message);
  assert(fabric.describeProvide("pi_pe_pipeline_mapped", "run"));

  const direct = await invokeResult(fabric, { nodeId: "pi_pe_pipeline_mapped", provide: "run", input: { text: "direct" } });
  assert.equal(direct.ok, true);
  if (direct.ok) assert.deepEqual(direct.output, { result: "Result: DIRECT" });

  const managed = await invokeResult(fabric, { nodeId: "pi_pe", provide: "run_pipeline", input: { id: "mapped", input: { text: "managed" } } });
  assert.equal(managed.ok, true);
  if (managed.ok) {
    assert.equal((managed.output as { status: string }).status, "succeeded");
    assert.deepEqual((managed.output as { output: unknown }).output, { result: "Result: MANAGED" });
  }

  const dry = await invokeResult(fabric, {
    nodeId: "pi_pe",
    provide: "dry_run_mapping",
    input: { id: "mapped", pipelineInput: { text: "dry" }, stepOutputs: { upper: { value: "DRY" }, wrap: { result: "Result: DRY" } } },
  });
  assert.equal(dry.ok, true);
  if (dry.ok) assert.equal((dry.output as { steps: unknown[] }).steps.length, 2);

  const listed = await invokeResult(fabric, { nodeId: "pi_pe", provide: "list_pipelines", input: {} });
  assert.equal(listed.ok, true);
  if (listed.ok) assert.equal((listed.output as { pipelines: unknown[] }).pipelines.length, 1);

  const removed = await invokeResult(fabric, { nodeId: "pi_pe", provide: "delete_pipeline", input: { id: "mapped", confirm: true } });
  assert.equal(removed.ok, true);
  assert.equal(fabric.describeNode("pi_pe_pipeline_mapped"), undefined);
});
