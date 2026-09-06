# Pi-PE

Pi-PE is an offline pipeline authoring and validation extension for Pi. It stores a small, data-only linear pipeline DSL, validates mappings against native Pi tool input metadata, previews mappings from supplied values, and catalogs the tools visible in the current session.

Pi-PE does **not** execute pipeline steps, register generated pipeline tools, discover an out-of-band registry, or ship a dispatcher. A saved pipeline is a specification for inspection or for a future trusted integration outside this package.

## SDK boundary

The installed `@earendil-works/pi-coding-agent` SDK exposes `ExtensionAPI.getAllTools()`, which returns native tool names, descriptions, TypeBox parameter schemas, prompt guidelines, and source metadata. It does not expose an arbitrary native-tool invocation method to extensions. Its `tool_call` event can block a call selected by Pi, but cannot initiate one. Pi-PE follows that boundary and never reaches into SDK internals or uses the native hook as an execution workaround.

Native metadata does not include target output schemas, effects, or versions. Pi-PE records those limitations explicitly, treats output compatibility as runtime-only, and requires `allowRuntimeOnly: true` before saving such a pipeline. Local test adapters may provide richer metadata to exercise the validator.

## Management tools

| Tool | Purpose |
|---|---|
| `pi_pe_catalog` | Search/list native Pi tool metadata |
| `pi_pe_describe_target` | Describe one exact target and SDK limitations |
| `pi_pe_validate_pipeline` | Validate without mutation |
| `pi_pe_save_pipeline` | Validate and persist an offline specification |
| `pi_pe_get_pipeline` | Return a saved specification and dependency status |
| `pi_pe_list_pipelines` | List saved, disabled, and quarantined specifications |
| `pi_pe_delete_pipeline` | Delete only with `confirm: true` |
| `pi_pe_dry_run_mapping` | Construct inputs and select output without calls |
| `pi_pe_reload_pipelines` | Re-read storage and refresh metadata status |

Every exposed management tool includes a concise prompt snippet and a tool-named prompt guideline. These tools only perform their documented local metadata, validation, mapping, or storage operation.

## Pipeline behavior

Specifications support pass-through values, RFC 6901 bindings, JSON constants, pinned or compatible dependency snapshots, static schema compatibility checks, runtime-only review, bounded steps/mappings/schema depth/JSON depth, and input/output validation for offline previews. Targets are exact native Pi tool names; input data cannot select a target and the DSL contains no JavaScript, shell, templates, expressions, retries, rollback, or nested execution.

## Persistence

State is stored under `~/.pi/agent/state/pi-pe/`, or `PI_PE_STATE_DIR` when set:

```text
pipelines/<pipeline-id>/pipeline.json
index.json
```

Writes are atomic. Invalid specifications are quarantined, unavailable or changed native metadata disables a saved specification, and unrelated valid specifications remain readable. No generated `tool.json` artifact is written.

## Development

```bash
npm test
npm run package:check
```

Tests cover validation, compatibility, pointer mapping, storage safety, native SDK loading, prompt metadata, and a real SDK `tool_call` blocking hook using a mock provider that makes no paid or network calls.
