# Pi-PE implementation notes

## Scope

Pi-PE owns only offline pipeline authoring. It uses the host Pi SDK directly and does not depend on `pi-tools`, `pi-protocol`, a shared dispatcher, a second registry, or generated executing tools.

## SDK finding

The installed `@earendil-works/pi-coding-agent` 0.80.10 API provides:

- `ExtensionAPI.getAllTools()` for read-only native metadata: name, description, TypeBox parameter schema, prompt guidelines, and source metadata.
- `ExtensionAPI.registerTool()` for registering Pi-PE's own management tools.
- `tool_call` as a blocking hook for calls Pi is already about to execute.

The SDK does not provide an arbitrary `invokeTool(name, input)` extension API. `tool_call` is interception, not invocation. Pi-PE therefore does not execute steps, call `AgentSession` internals, route through a hook, or register generated pipeline tools.

The SDK metadata has no native output schema, effect declaration, or tool version. Pi-PE preserves that uncertainty in catalog/describe responses and dependency snapshots. Pipelines whose compatibility cannot be proven are runtime-only and require explicit review when saved.

## Local contract

- Pipeline specifications are data-only and use exact native tool names.
- Save-time validation checks schemas, mappings, native metadata presence, dependency fingerprints, limits, and RFC 6901 pointers.
- `pi_pe_dry_run_mapping` constructs inputs from supplied pipeline/step values and never invokes targets.
- `pipeline.json` is the source of truth; writes are atomic and state paths reject symlinks.
- Invalid entries are quarantined and unavailable/changed dependencies are disabled without hiding unrelated entries.
- Exposed tools have accurate descriptions, prompt snippets, and tool-named guidelines.

## Verification

Run from `pi-pe`:

```bash
npm test
npm run package:check
```

The SDK test loads the extension through `DefaultResourceLoader`, registers a no-network mock provider, verifies Pi-PE's native tools, and proves a native `tool_call` blocking hook prevents the test tool from executing. No paid provider call is made.
