import type { ToolRuntime } from "../tools.ts";
import { PipelineError } from "../errors.ts";
import { deepCloneJson, parsePipelineSpec } from "../schemas.ts";
import { PipelineRepository } from "../storage/repository.ts";
import type {
  DryRunStep,
  Issue,
  PipelineCard,
  PipelineSpecV1,
  PipelineStatus,
  ResolvedTarget,
  TargetResolver,
  ValidationReport,
} from "../types.ts";
import { checkPipelineDependencies } from "./dependencies.ts";
import { constructStepInput, selectPipelineOutput } from "./map-input.ts";
import { validateJsonSchemaValue } from "../schemas.ts";
import { createToolTargetResolver } from "../management/catalog.ts";
import { validatePipelineCandidate, validateParsedPipeline } from "./validate.ts";

interface LoadedEntry {
  id: string;
  spec?: PipelineSpecV1;
  report?: ValidationReport;
  status: PipelineStatus;
}

export interface SavePipelineOptions { allowRuntimeOnly?: boolean; }

/**
 * Local pipeline authoring service.
 *
 * Pi's ExtensionAPI exposes native tool metadata through getAllTools(), but it
 * deliberately does not expose an arbitrary tool-call method to extensions.
 * This service therefore validates, stores, catalogs, and maps specifications;
 * it has no execution or generated-tool registration path.
 */
export class PipelineService {
  private readonly entries = new Map<string, LoadedEntry>();
  private readonly mutex = new AsyncMutex();

  constructor(
    private readonly runtime: ToolRuntime,
    readonly repository = new PipelineRepository(),
  ) {}

  async initialize(): Promise<PipelineStatus[]> { return this.reload(); }

  async reload(): Promise<PipelineStatus[]> { return this.mutex.run(() => this.reconcileLocked()); }

  async dispose(): Promise<void> {
    await this.mutex.run(async () => { this.entries.clear(); });
  }

  async validate(value: unknown): Promise<ValidationReport> {
    const staged = await this.loadStagedSpecs();
    const parsed = parsePipelineSpec(value);
    if (parsed.spec) staged.set(parsed.spec.id, parsed.spec);
    return validatePipelineCandidate(value, this.createStagedResolver(staged)).report;
  }

  async save(value: unknown, options: SavePipelineOptions = {}): Promise<{ spec: PipelineSpecV1; status: PipelineStatus }> {
    return this.mutex.run(async () => {
      const parsed = parsePipelineSpec(value);
      if (!parsed.spec) throw new PipelineError("PIPELINE_INVALID", "pipeline specification is malformed", { report: invalidReport(parsed.errors) });
      const existing = this.entries.get(parsed.spec.id)?.spec;
      const now = new Date().toISOString();
      const candidate: PipelineSpecV1 = { ...parsed.spec, createdAt: existing?.createdAt ?? parsed.spec.createdAt, updatedAt: now };
      const validation = validateParsedPipeline(candidate, createToolTargetResolver(this.runtime));
      if (!validation.report.valid) throw new PipelineError("PIPELINE_INVALID", "pipeline validation failed", { report: validation.report });
      if (validation.report.assurance === "runtime_only" && options.allowRuntimeOnly !== true) {
        throw new PipelineError("PIPELINE_INVALID", "runtime-only compatibility requires allowRuntimeOnly: true", { report: validation.report });
      }

      const materialized: PipelineSpecV1 = {
        ...candidate,
        dependencies: validation.report.dependencies,
        review: { runtimeOnly: validation.report.assurance === "runtime_only", reviewedAt: now },
      };
      const previous = await this.repository.snapshot(materialized.id);
      await this.repository.persist(materialized);
      try {
        await this.reconcileLocked();
        const saved = this.entries.get(materialized.id);
        if (!saved?.spec || saved.status.status !== "enabled") {
          throw new PipelineError("PIPELINE_INVALID", `saved pipeline could not be enabled: ${saved?.status.issues.map((item) => item.message).join("; ") ?? "unknown error"}`, { status: saved?.status });
        }
        return { spec: deepCloneJson(saved.spec), status: deepCloneJson(saved.status) };
      } catch (error) {
        await this.repository.restore(materialized.id, previous);
        await this.reconcileLocked();
        throw error;
      }
    });
  }

  async delete(id: string, confirmed: boolean): Promise<{ deleted: boolean; id: string }> {
    if (!confirmed) throw new PipelineError("PIPELINE_INVALID", "delete_pipeline requires confirm: true");
    return this.mutex.run(async () => {
      const deleted = await this.repository.delete(id);
      await this.reconcileLocked();
      return { deleted, id };
    });
  }

  list(): PipelineCard[] {
    return [...this.entries.values()].map((entry) => ({
      ...deepCloneJson(entry.status),
      ...(entry.spec ? {
        description: entry.spec.description,
        tags: [...entry.spec.tags],
        stepCount: entry.spec.steps.length,
        dependencyPolicy: entry.spec.dependencyPolicy ?? "pinned",
      } : {}),
    })).sort((left, right) => left.id.localeCompare(right.id));
  }

  get(id: string): { spec: PipelineSpecV1; status: PipelineStatus; dependencyStatus: { usable: boolean; issues: Issue[]; currentTargets: string[] } } | undefined {
    const entry = this.entries.get(id);
    if (!entry?.spec) return undefined;
    const dependencyStatus = checkPipelineDependencies(entry.spec, createToolTargetResolver(this.runtime));
    return {
      spec: deepCloneJson(entry.spec),
      status: deepCloneJson(entry.status),
      dependencyStatus: { usable: dependencyStatus.usable, issues: deepCloneJson(dependencyStatus.issues), currentTargets: [...dependencyStatus.current.keys()].sort() },
    };
  }

  async dryRun(value: { id?: string; spec?: unknown; pipelineInput: unknown; stepOutputs?: Record<string, unknown> }): Promise<{
    validation: ValidationReport;
    pipelineInputValid?: boolean;
    pipelineInputError?: string;
    steps: DryRunStep[];
    output?: unknown;
    outputError?: string;
  }> {
    let spec: PipelineSpecV1 | undefined;
    let validation: ValidationReport;
    if (value.id) {
      const entry = this.entries.get(value.id);
      if (!entry?.spec || !entry.report) throw new PipelineError("PIPELINE_NOT_FOUND", `pipeline not found: ${value.id}`);
      spec = entry.spec;
      validation = entry.report;
    } else {
      const parsed = parsePipelineSpec(value.spec);
      const resolver = createToolTargetResolver(this.runtime);
      const outcome = validatePipelineCandidate(value.spec, resolver);
      spec = outcome.spec;
      validation = outcome.report;
      if (!parsed.spec && !spec) return { validation, steps: [] };
    }
    if (!spec) return { validation, steps: [] };

    const pipelineInputError = validateJsonSchemaValue(spec.inputSchema, value.pipelineInput, "pipeline input");
    const inputStatus = {
      pipelineInputValid: pipelineInputError === undefined,
      ...(pipelineInputError ? { pipelineInputError } : {}),
    };
    const outputs = new Map<string, unknown>();
    const steps: DryRunStep[] = [];
    let previousStepId: string | undefined;
    const resolver = createToolTargetResolver(this.runtime);
    for (const step of spec.steps) {
      try {
        const input = constructStepInput(step, { pipelineInput: value.pipelineInput, outputs, previousStepId });
        const target = resolver(step.target);
        const validationError = target ? validateJsonSchemaValue(target.provide.inputSchema, input, `step ${step.id} input`) : `native tool metadata unavailable: ${step.target}`;
        steps.push({ stepId: step.id, target: step.target, input, inputValid: !validationError, ...(validationError ? { error: validationError } : {}) });
      } catch (error) {
        steps.push({ stepId: step.id, target: step.target, inputValid: false, error: error instanceof Error ? error.message : String(error) });
      }
      if (value.stepOutputs && Object.prototype.hasOwnProperty.call(value.stepOutputs, step.id)) {
        const stepOutput = value.stepOutputs[step.id];
        outputs.set(step.id, stepOutput);
        const target = resolver(step.target);
        const outputError = target ? validateJsonSchemaValue(target.provide.outputSchema, stepOutput, `step ${step.id} output`) : undefined;
        const dryRunStep = steps.at(-1);
        if (dryRunStep) Object.assign(dryRunStep, { outputValid: outputError === undefined, ...(outputError ? { outputError } : {}) });
      }
      previousStepId = step.id;
    }
    try {
      const output = selectPipelineOutput(spec, value.pipelineInput, outputs);
      const outputError = validateJsonSchemaValue(spec.outputSchema, output, "pipeline output");
      return { validation, ...inputStatus, steps, output, ...(outputError ? { outputError } : {}) };
    } catch (error) {
      return { validation, ...inputStatus, steps, outputError: error instanceof Error ? error.message : String(error) };
    }
  }

  private async reconcileLocked(): Promise<PipelineStatus[]> {
    await this.repository.initialize();
    this.entries.clear();
    const records = await this.repository.readAll();
    const resolver = createToolTargetResolver(this.runtime);
    for (const record of records) {
      if (record.error) {
        this.entries.set(record.id, quarantine(record.id, `cannot read pipeline.json: ${record.error}`));
        continue;
      }
      const parsed = parsePipelineSpec(record.value);
      if (!parsed.spec) {
        this.entries.set(record.id, { id: record.id, status: status(record.id, "quarantined", parsed.errors) });
        continue;
      }
      if (parsed.spec.id !== record.id) {
        this.entries.set(record.id, quarantine(record.id, `directory id ${record.id} does not match spec id ${parsed.spec.id}`));
        continue;
      }
      this.entries.set(record.id, evaluateEntry(parsed.spec, resolver));
    }
    const statuses = this.list().map(({ description: _description, tags: _tags, stepCount: _stepCount, dependencyPolicy: _policy, ...item }) => item);
    await this.repository.writeIndex(statuses);
    return statuses;
  }

  private async loadStagedSpecs(): Promise<Map<string, PipelineSpecV1>> {
    const staged = new Map<string, PipelineSpecV1>();
    for (const record of await this.repository.readAll()) {
      if (record.error) continue;
      const parsed = parsePipelineSpec(record.value);
      if (parsed.spec && parsed.spec.id === record.id) staged.set(record.id, parsed.spec);
    }
    return staged;
  }

  private createStagedResolver(_specs: ReadonlyMap<string, PipelineSpecV1>): TargetResolver {
    return createToolTargetResolver(this.runtime);
  }
}

function evaluateEntry(spec: PipelineSpecV1, resolver: TargetResolver): LoadedEntry {
  const validation = validateParsedPipeline(spec, resolver);
  const dependency = checkPipelineDependencies(spec, resolver);
  const issues = [...validation.report.errors, ...validation.report.warnings, ...dependency.issues];
  let entryStatus: PipelineStatus["status"] = "enabled";
  if (!dependency.usable) entryStatus = "disabled";
  else if (!validation.report.valid) entryStatus = "quarantined";
  else if (validation.report.assurance === "runtime_only" && spec.review?.runtimeOnly !== true) entryStatus = "disabled";
  return { id: spec.id, spec, report: validation.report, status: status(spec.id, entryStatus, issues, spec, validation.report.assurance) };
}

function invalidReport(errors: Issue[]): ValidationReport { return { valid: false, assurance: "invalid", errors, warnings: [], dependencies: [] }; }
function status(id: string, entryStatus: PipelineStatus["status"], issues: Issue[], spec?: PipelineSpecV1, assurance?: PipelineStatus["assurance"]): PipelineStatus {
  return { id, ...(spec ? { name: spec.name, version: spec.version, updatedAt: spec.updatedAt } : {}), status: entryStatus, ...(assurance ? { assurance } : {}), issues: deepCloneJson(issues) };
}
function quarantine(id: string, message: string): LoadedEntry { return { id, status: status(id, "quarantined", [{ code: "PIPELINE_INVALID", message }]) }; }

class AsyncMutex {
  private tail: Promise<void> = Promise.resolve();
  async run<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try { return await operation(); } finally { release(); }
  }
}
