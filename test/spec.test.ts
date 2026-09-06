import assert from "node:assert/strict";
import test from "node:test";
import { validatePipelineCandidate } from "../src/pipeline/validate.ts";
import { fixture, registerHandler, registerMappedFixtures, resolverFrom, TestToolRuntime } from "./helpers.ts";

test("fully mapped ordinary tools receive static assurance", async () => {
  const runtime = new TestToolRuntime(); registerMappedFixtures(runtime);
  const report = validatePipelineCandidate(await fixture("mapped.pipeline.json"), resolverFrom(runtime)).report;
  assert.equal(report.valid, true, JSON.stringify(report.errors)); assert.equal(report.assurance, "static"); assert.equal(report.dependencies.length, 2);
});

test("unsafe IDs, empty pipelines, bounds, and unknown tools fail", async () => {
  const runtime = new TestToolRuntime(); registerMappedFixtures(runtime); const base = await fixture("mapped.pipeline.json");
  for (const id of ["../escape", "Upper", "has/slash", ""]) assert.equal(validatePipelineCandidate({ ...base, id }, resolverFrom(runtime)).report.valid, false);
  assert(validatePipelineCandidate({ ...base, steps: [] }, resolverFrom(runtime)).report.errors.some((error) => error.code === "EMPTY_PIPELINE"));
  assert(validatePipelineCandidate({ ...base, limits: { maxSteps: 1 } }, resolverFrom(runtime)).report.errors.some((error) => error.code === "STEP_LIMIT"));
  assert(validatePipelineCandidate(base, resolverFrom(new TestToolRuntime())).report.errors.some((error) => error.code === "DEPENDENCY_NOT_FOUND"));
});

test("schema enums must match their declared type", async () => {
  const runtime = new TestToolRuntime();
  const base = await fixture("passthrough.pipeline.json");
  const report = validatePipelineCandidate({ ...base, inputSchema: { type: "string", enum: [42] } }, resolverFrom(runtime)).report;
  assert.equal(report.valid, false);
  assert(report.errors.some((error) => error.code === "INVALID_SCHEMA_ENUM_TYPE"));
  const impossible = validatePipelineCandidate({ ...base, inputSchema: { type: "object", required: ["missing"], properties: {}, additionalProperties: false } }, resolverFrom(runtime)).report;
  assert(impossible.errors.some((error) => error.code === "INVALID_SCHEMA_REQUIRED"));
});

test("broad pass-through ordinary tools require runtime-only review", async () => {
  const runtime = new TestToolRuntime(); registerHandler(runtime, "broad_run", {}, {}, (input) => input); const base = await fixture("passthrough.pipeline.json");
  const report = validatePipelineCandidate({ ...base, id: "broad-pipe", inputSchema: {}, outputSchema: {}, steps: [{ id: "broad", target: "broad_run", input: { mode: "pass", from: { source: "pipeline_input" } } }] }, resolverFrom(runtime)).report;
  assert.equal(report.valid, true); assert.equal(report.assurance, "runtime_only");
});
