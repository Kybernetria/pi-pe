import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import {
  createAgentSession,
  DefaultResourceLoader,
  SessionManager,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";

const extensionPath = fileURLToPath(new URL("../extension.ts", import.meta.url));

test("SDK loads pi-pe as native tools and a mock provider without network calls", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-sdk-"));
  const previousStateDir = process.env.PI_PE_STATE_DIR;
  process.env.PI_PE_STATE_DIR = join(root, "state");
  let probeCalls = 0;
  let providerCalls = 0;
  const loader = new DefaultResourceLoader({
    cwd: root,
    agentDir: join(root, "agent"),
    extensionFactories: [
      (pi) => {
        pi.registerProvider("pi-pe-mock", {
          baseUrl: "http://127.0.0.1:9/no-network",
          apiKey: "mock-key",
          api: "openai-completions",
          models: [{ id: "mock-model", name: "Pi-PE Mock", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 4096, maxTokens: 256 }],
          streamSimple: (() => { providerCalls += 1; throw new Error("mock provider must not be called in metadata test"); }) as never,
        });
        pi.registerTool({
          name: "native_probe",
          label: "Native Probe",
          description: "A test native tool whose execution must be blocked by the SDK hook.",
          parameters: { type: "object", additionalProperties: false } as never,
          async execute() { probeCalls += 1; return { content: [{ type: "text", text: "executed" }], details: {} }; },
        });
        pi.on("tool_call", (event) => event.toolName === "native_probe" ? { block: true, reason: "blocked by test policy" } : undefined);
      },
    ],
    additionalExtensionPaths: [extensionPath],
  });
  await loader.reload();
  const model = { id: "mock-model", name: "Pi-PE Mock", api: "openai-completions", provider: "pi-pe-mock", baseUrl: "http://127.0.0.1:9/no-network", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 4096, maxTokens: 256 };
  const { session, extensionsResult } = await createAgentSession({
    cwd: root,
    agentDir: join(root, "agent"),
    model: model as never,
    resourceLoader: loader,
    sessionManager: SessionManager.inMemory(root),
    settingsManager: SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } }),
    tools: ["pi_pe_catalog", "pi_pe_dry_run_mapping", "native_probe"],
  });
  try {
    assert.equal(extensionsResult.errors.length, 0, JSON.stringify(extensionsResult.errors));
    const tools = session.getAllTools().map((tool) => tool.name);
    assert(tools.includes("pi_pe_catalog"), `tools=${JSON.stringify(tools)} errors=${JSON.stringify(extensionsResult.errors)}`);
    assert(tools.includes("pi_pe_dry_run_mapping"));
    assert(tools.includes("native_probe"));
    const decision = await session.extensionRunner.emitToolCall({ type: "tool_call", toolCallId: "blocked", toolName: "native_probe", input: {} });
    assert.deepEqual(decision, { block: true, reason: "blocked by test policy" });
    assert.equal(probeCalls, 0);
    assert.equal(providerCalls, 0);
  } finally {
    session.dispose();
    if (previousStateDir === undefined) delete process.env.PI_PE_STATE_DIR;
    else process.env.PI_PE_STATE_DIR = previousStateDir;
  }
});
