async function invokeResult(fabric: { invokeTracked(request: any): Promise<any> }, request: any): Promise<any> {
  return (await fabric.invokeTracked(request)).result;
}

import assert from "node:assert/strict";
import test from "node:test";
import { createProtocolFabric, type CanonicalProvenanceEventV1 } from "@kybernetria/pi-protocol";
import { PipelineError } from "../src/errors.ts";
import { createGeneratedManifest } from "../src/generated/manifest.ts";
import { registerGeneratedPipeline } from "../src/generated/register.ts";
import { PipelineExecutor, createRuntimeSnapshot } from "../src/pipeline/execute.ts";
import { validateParsedPipeline } from "../src/pipeline/validate.ts";
import type { PipelineSpecV1 } from "../src/types.ts";
import { disposeTestNode, fixture, installTestNode, registerHandler, registerMappedFixtures, resolverFrom } from "./helpers.ts";

async function materialize(spec: PipelineSpecV1, fabric: ReturnType<typeof createProtocolFabric>): Promise<PipelineSpecV1> {
  const report = validateParsedPipeline(spec, resolverFrom(fabric)).report;
  assert.equal(report.valid, true, JSON.stringify(report.errors));
  return { ...spec, dependencies: report.dependencies, review: { runtimeOnly: report.assurance === "runtime_only", reviewedAt: new Date().toISOString() } };
}

test("sequential mapped execution returns business output and propagates nested trace context", async () => {
  const fabric = createProtocolFabric();
  const order: string[] = [];
  registerMappedFixtures(fabric, order);
  const spec = await materialize(await fixture("mapped.pipeline.json"), fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  const snapshot = createRuntimeSnapshot(spec);
  registerGeneratedPipeline(fabric, createGeneratedManifest(spec), snapshot, executor);
  const events: CanonicalProvenanceEventV1[] = [];
  fabric.subscribeAudit((event) => { events.push(event); });

  const result = await invokeResult(fabric, {
    nodeId: "pi_pe_pipeline_mapped",
    provide: "run",
    input: { text: "hello" },
    traceId: "trace_test",
    spanId: "root",
    callerNodeId: "root_agent",
    session: { id: "session_test", mode: "continue" },
  });
  assert.deepEqual(order, ["upper", "wrap"]);
  assert.equal(result.ok, true);
  if (result.ok) assert.deepEqual(result.output, { result: "Result: HELLO" });

  await Promise.resolve();
  const starts = events.filter((event): event is Extract<CanonicalProvenanceEventV1, { invocationId: string }> => "invocationId" in event && event.type === "invocation.started");
  const root = starts.find((event) => event.target === "pi_pe_pipeline_mapped.run");
  const upper = starts.find((event) => event.target === "fixture.upper");
  const wrap = starts.find((event) => event.target === "fixture.wrap");
  assert.ok(root);
  assert.equal(upper?.parentInvocationId, root.invocationId);
  assert.equal(wrap?.parentInvocationId, root.invocationId);
});

test("execution fails fast without retries and reports completed effecting steps", async () => {
  const fabric = createProtocolFabric();
  let firstCalls = 0;
  let secondCalls = 0;
  installTestNode(fabric, {
    node: {
      nodeId: "fail_fixture",
      purpose: "Failure fixture",
      provides: [
        { name: "first", description: "first", inputSchema: { type: "string" }, outputSchema: { type: "string" }, execution: { type: "handler", handler: "first" }, effects: ["write"] },
        { name: "second", description: "second", inputSchema: { type: "string" }, outputSchema: { type: "string" }, execution: { type: "handler", handler: "second" } },
      ],
    },
    handlers: {
      first: (input) => { firstCalls += 1; return input; },
      second: () => { secondCalls += 1; throw new Error("boom"); },
    },
  });
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({ ...base, id: "fail-fast", steps: [
    { ...base.steps[0], target: "fail_fixture.first" },
    { ...base.steps[1], target: "fail_fixture.second" },
  ] }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  await assert.rejects(
    () => executor.execute(createRuntimeSnapshot(spec), "value"),
    (error) => {
      assert(error instanceof PipelineError);
      assert.equal(error.code, "STEP_FAILED");
      const execution = error.details?.execution as { completedSideEffectingSteps: unknown[] };
      assert.equal(execution.completedSideEffectingSteps.length, 1);
      return true;
    },
  );
  assert.equal(firstCalls, 1);
  assert.equal(secondCalls, 1);
});

test("effect diagnostics preserve unknown outcomes for throwing and cancelled calls", async () => {
  const fabric = createProtocolFabric();
  registerHandler(fabric, "effect_failure", "run", { type: "string" }, { type: "string" }, () => {
    throw new Error("effect may have happened");
  }, ["fs.write"]);
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({
    ...base,
    id: "effect-failure",
    steps: [{ id: "run", target: "effect_failure.run", input: { mode: "pass", from: { source: "pipeline_input" } } }],
  }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  await assert.rejects(() => executor.execute(createRuntimeSnapshot(spec), "x"), (error) => {
    if (!(error instanceof PipelineError)) return false;
    const execution = error.details?.execution as { dispatchedSideEffects: Array<{ status: string }>; completedSideEffectingSteps: unknown[] };
    assert.deepEqual(execution.dispatchedSideEffects.map((item) => item.status), ["unknown"]);
    assert.equal(execution.completedSideEffectingSteps.length, 0);
    return true;
  });
});

test("step timeout and caller cancellation are classified separately", async () => {
  const create = async (id: string, timeoutMs: number) => {
    const fabric = createProtocolFabric();
    registerHandler(fabric, "slow", "wait", { type: "string" }, { type: "string" }, async (input, context) => {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 500);
        context?.abortSignal?.addEventListener("abort", () => {
          clearTimeout(timer);
          const error = new Error("Invocation aborted");
          error.name = "AbortError";
          reject(error);
        }, { once: true });
      });
      return input;
    }, ["fs.write"]);
    const base = await fixture("passthrough.pipeline.json");
    const raw: PipelineSpecV1 = { ...base, id, limits: { timeoutMs: 1_000 }, steps: [{ id: "wait", target: "slow.wait", input: { mode: "pass", from: { source: "pipeline_input" } }, timeoutMs }] };
    return { fabric, spec: await materialize(raw, fabric) };
  };

  const timed = await create("timed", 20);
  const timedExecutor = new PipelineExecutor(timed.fabric, resolverFrom(timed.fabric));
  await assert.rejects(() => timedExecutor.execute(createRuntimeSnapshot(timed.spec), "x"), (error) => error instanceof PipelineError && error.code === "PIPELINE_TIMEOUT");

  const cancelled = await create("cancelled", 1_000);
  const cancelledExecutor = new PipelineExecutor(cancelled.fabric, resolverFrom(cancelled.fabric));
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 20);
  await assert.rejects(
    () => cancelledExecutor.execute(createRuntimeSnapshot(cancelled.spec), "x", { nodeId: "test", provide: "run", abortSignal: controller.signal }),
    (error) => {
      if (!(error instanceof PipelineError) || error.code !== "PIPELINE_ABORTED") return false;
      const execution = error.details?.execution as { dispatchedSideEffects: Array<{ status: string }> };
      assert.deepEqual(execution.dispatchedSideEffects.map((item) => item.status), ["unknown"]);
      return true;
    },
  );
});

test("shutdown cancels active runs within the bounded shutdown window", async () => {
  const fabric = createProtocolFabric();
  registerHandler(fabric, "shutdown_slow", "wait", { type: "string" }, { type: "string" }, async (input, context) => {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, 5_000);
      context?.abortSignal?.addEventListener("abort", () => {
        clearTimeout(timer);
        reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      }, { once: true });
    });
    return input;
  });
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({ ...base, id: "shutdown", steps: [{ id: "wait", target: "shutdown_slow.wait", input: { mode: "pass", from: { source: "pipeline_input" } } }] }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  const running = executor.execute(createRuntimeSnapshot(spec), "x");
  await new Promise((resolve) => setTimeout(resolve, 10));
  await executor.shutdown(1_000);
  await assert.rejects(running, (error) => error instanceof PipelineError && error.code === "PIPELINE_ABORTED");
});

test("runtime cycle through an external handler is stopped", async () => {
  const fabric = createProtocolFabric();
  let calls = 0;
  registerHandler(fabric, "bounce", "run", { type: "string" }, { type: "string" }, async (input) => {
    calls += 1;
    const nested = await invokeResult(fabric, { nodeId: "pi_pe_pipeline_runtime-cycle", provide: "run", input });
    if (!nested.ok) throw new Error(nested.error.message);
    return nested.output;
  });
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({
    ...base,
    id: "runtime-cycle",
    steps: [{ id: "bounce", target: "bounce.run", input: { mode: "pass", from: { source: "pipeline_input" } } }],
  }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  const snapshot = createRuntimeSnapshot(spec);
  registerGeneratedPipeline(fabric, createGeneratedManifest(spec), snapshot, executor);
  await assert.rejects(
    () => executor.execute(snapshot, "x"),
    (error) => error instanceof PipelineError && error.code === "PIPELINE_CYCLE" && /PIPELINE_CYCLE/.test(error.message),
  );
  assert.equal(calls, 1);
});

test("real fabric keeps an abort-ignoring effect pending through shutdown", async () => {
  const fabric = createProtocolFabric();
  let finish!: () => void;
  const finished = new Promise<void>((resolve) => { finish = resolve; });
  registerHandler(fabric, "real_shutdown_effect", "run", { type: "string" }, { type: "string" }, async () => {
    await finished;
    return "finished";
  }, ["fs.write"]);
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({
    ...base,
    id: "real-shutdown-effect",
    steps: [{ id: "run", target: "real_shutdown_effect.run", input: { mode: "pass", from: { source: "pipeline_input" } } }],
  }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  const controller = new AbortController();
  const running = executor.execute(createRuntimeSnapshot(spec), "x", { nodeId: "test", provide: "run", abortSignal: controller.signal });
  setTimeout(() => controller.abort(), 10);
  await assert.rejects(running, (error) => error instanceof PipelineError && error.code === "PIPELINE_ABORTED");

  const pending = await executor.shutdown(20);
  assert.equal(pending.drained, false);
  assert.equal(pending.activeRunsDrained, true);
  assert.equal(pending.detachedDownstreamInvocationsDrained, false);
  assert.equal(pending.pendingDetachedDownstreamInvocations, 1);

  finish();
  const drained = await executor.shutdown(1_000);
  assert.equal(drained.drained, true);
  assert.equal(drained.pendingDetachedDownstreamInvocations, 0);
});

test("oversized intermediate outputs and changed pinned dependencies are refused", async () => {
  const fabric = createProtocolFabric();
  registerHandler(fabric, "large", "run", { type: "string" }, { type: "string" }, () => "x".repeat(1_000), ["fs.write"]);
  const base = await fixture("passthrough.pipeline.json");
  const spec = await materialize({
    ...base,
    id: "large-output",
    limits: { maxIntermediateBytes: 100 },
    steps: [{ id: "large", target: "large.run", input: { mode: "pass", from: { source: "pipeline_input" } } }],
  }, fabric);
  const executor = new PipelineExecutor(fabric, resolverFrom(fabric));
  await assert.rejects(() => executor.execute(createRuntimeSnapshot(spec), "x"), (error) => {
    if (!(error instanceof PipelineError) || error.code !== "STEP_OUTPUT_TOO_LARGE") return false;
    const execution = error.details?.execution as { steps: Array<{ status: string }>; completedSideEffectingSteps: unknown[] };
    assert.equal(execution.steps[0]?.status, "succeeded");
    assert.equal(execution.completedSideEffectingSteps.length, 1);
    return true;
  });

  await disposeTestNode(fabric, "large");
  registerHandler(fabric, "large", "run", { type: "number" }, { type: "string" }, () => "changed", [], "2.0.0");
  await assert.rejects(() => executor.execute(createRuntimeSnapshot(spec), "x"), (error) => error instanceof PipelineError && error.code === "DEPENDENCY_CHANGED");
});
