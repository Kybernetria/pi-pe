import assert from "node:assert/strict";
import test from "node:test";
import { createManagementTools, MANAGEMENT_TOOL_NAMES } from "../src/management/handlers.ts";
import { catalogProvides } from "../src/management/catalog.ts";
import { PipelineService } from "../src/pipeline/service.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { registerHandler, TestToolRuntime } from "./helpers.ts";

test("management surface uses native Pi tool definitions with prompt metadata", () => {
  const runtime = new TestToolRuntime();
  const service = new PipelineService(runtime, new PipelineRepository("/tmp/pi-pe-manifest-test-unused"));
  const tools = createManagementTools(runtime, service);
  assert.deepEqual(tools.map((tool) => tool.name), MANAGEMENT_TOOL_NAMES);
  assert(tools.every((tool) => tool.parameters && tool.promptSnippet && tool.promptGuidelines?.every((item) => item.includes(tool.name))));
});

test("catalog reports native metadata limits and never reports generated tools", () => {
  const runtime = new TestToolRuntime();
  registerHandler(runtime, "native_probe", { type: "object" }, {}, () => "unused");
  const result = catalogProvides(runtime, new Set(), {});
  assert.equal(result.items[0]?.executionType, "native");
  assert.equal(result.items[0]?.effectsKnown, false);
});
