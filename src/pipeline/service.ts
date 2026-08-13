import { performance } from "node:perf_hooks";
import type { ProtocolFabric, ProtocolRegistration, ProvideSnapshot } from "@kybernetria/pi-protocol/core";
import type { ProtocolManifestV1 as PiProtocolManifest } from "@kybernetria/pi-protocol/contract";
import { SHUTDOWN_TIMEOUT_MS } from "../config.ts";
import { PipelineError } from "../errors.ts";
import { createGeneratedManifest } from "../generated/manifest.ts";
import { registerGeneratedPipeline, replaceGeneratedPipeline } from "../generated/register.ts";
import { deepCloneJson, GENERATED_NODE_PREFIX, GENERATED_RUN_PROVIDE, generatedTarget, parsePipelineSpec, parseTarget } from "../schemas.ts";
import { PipelineRepository } from "../storage/repository.ts";
import type {
  DryRunStep,
  Issue,
  PipelineCard,
  PipelineRuntimeSnapshot,
  PipelineSpecV1,
  PipelineStatus,
  ResolvedTarget,
  TargetResolver,
  ValidationReport,
} from "../types.ts";
import { findPipelineCycles, cycleTargets, assertPipelinesAcyclic } from "./cycles.ts";
import { checkPipelineDependencies } from "./dependencies.ts";
import { PipelineExecutor, createRuntimeSnapshot, hasActivePipelineExecution } from "./execute.ts";
import { constructStepInput, selectPipelineOutput } from "./map-input.ts";
import { validateJsonSchemaValue } from "../schemas.ts";
import { createFabricTargetResolver } from "../protocol/catalog.ts";
import { validatePipelineCandidate, validateParsedPipeline } from "./validate.ts";

interface LoadedEntry {
  id: string;
  spec?: PipelineSpecV1;
  manifest?: PiProtocolManifest;
  snapshot?: PipelineRuntimeSnapshot;
  report?: ValidationReport;
  status: PipelineStatus;
}

export interface SavePipelineOptions {
  allowRuntimeOnly?: boolean;
}

export interface PipelineDisposeResult {
  status: "completed" | "degraded";
  activeRunsDrained: boolean;
  detachedDownstreamInvocationsDrained: boolean;
  pendingDetachedDownstreamInvocations: number;
  registrations: Array<{ nodeId: string; status: "disposed" | "timed_out" | "failed"; error?: string }>;
}

export class PipelineService {
  private readonly entries = new Map<string, LoadedEntry>();
  private readonly executor: PipelineExecutor;
  private readonly mutex = new AsyncMutex();
  private readonly registrations = new Map<string, ProtocolRegistration>();
  private disposePromise?: Promise<PipelineDisposeResult>;
  private closing = false;

  constructor(
    private readonly fabric: ProtocolFabric,
    readonly repository = new PipelineRepository(),
  ) {
    this.executor = new PipelineExecutor(fabric, (target) => this.resolveRuntimeTarget(target));
  }

  async initialize(): Promise<PipelineStatus[]> {
    return this.reload();
  }

  async reload(): Promise<PipelineStatus[]> {
    this.assertMutationAdmission();
    return this.mutex.run(() => this.repository.withMutationLock((lockSignal) => {
      this.assertMutationAdmission(lockSignal);
      return this.reconcileLocked(lockSignal);
    }));
  }

  async dispose(): Promise<PipelineDisposeResult> {
    if (this.disposePromise) return this.disposePromise;
    this.closing = true;
    // Abort active work and establish the deadline before waiting for a save,
    // delete, or reload that may currently own the mutation mutex.
    const deadline = performance.now() + SHUTDOWN_TIMEOUT_MS;
    const shutdownPromise = this.executor.shutdown(remainingMs(deadline));
    this.disposePromise = (async () => {
      const cleanup = await this.mutex.runUntil(deadline, async () => {
        const shutdown = await shutdownPromise;
        const registrations = await this.disposeGeneratedRegistrations(deadline);
        this.entries.clear();
        return createDisposeResult(shutdown, registrations);
      });
      if (!cleanup.acquired) {
        void this.mutex.run(async () => {
          const eventualDeadline = performance.now() + SHUTDOWN_TIMEOUT_MS;
          await this.disposeGeneratedRegistrations(eventualDeadline);
          this.entries.clear();
        }).catch((error) => console.error("[pi-pe] eventual shutdown cleanup failed", error));
      }
      const result = cleanup.acquired
        ? cleanup.value
        : {
          ...createDisposeResult(await shutdownPromise, [
            ...[...this.registrations.values()].map((registration) => ({
              nodeId: registration.nodeId,
              status: "timed_out" as const,
              error: "mutation mutex acquisition exceeded shutdown deadline",
            })),
            ...(!this.registrations.size ? [{
              nodeId: "(mutation-mutex)",
              status: "timed_out" as const,
              error: "mutation mutex acquisition exceeded shutdown deadline",
            }] : []),
          ]),
          status: "degraded" as const,
        };
      if (result.status === "degraded") console.warn("[pi-pe] shutdown degraded", JSON.stringify(result));
      return result;
    })();
    return this.disposePromise;
  }

  async validate(value: unknown): Promise<ValidationReport> {
    const staged = await this.loadStagedSpecs();
    const parsed = parsePipelineSpec(value);
    if (parsed.spec) staged.set(parsed.spec.id, parsed.spec);
    const resolver = this.createStagedResolver(staged);
    const report = validatePipelineCandidate(value, resolver).report;
    if (!parsed.spec) return report;
    const ownTarget = generatedTarget(parsed.spec.id);
    const cycle = findPipelineCycles(staged.values()).find((path) => path.includes(ownTarget));
    if (!cycle) return report;
    return {
      ...report,
      valid: false,
      assurance: "invalid",
      errors: [...report.errors, { code: "PIPELINE_CYCLE", message: `pipeline cycle detected: ${cycle.join(" -> ")}`, target: ownTarget }],
    };
  }

  async save(value: unknown, options: SavePipelineOptions = {}): Promise<{ spec: PipelineSpecV1; status: PipelineStatus; generatedTarget: string }> {
    this.assertMutationAdmission();
    return this.mutex.run(() => this.repository.withMutationLock(async (lockSignal) => {
      this.assertMutationAdmission(lockSignal);
      await this.reconcileLocked(lockSignal);
      const parsed = parsePipelineSpec(value);
      if (!parsed.spec) throw new PipelineError("PIPELINE_INVALID", "pipeline specification is malformed", { report: invalidReport(parsed.errors) });
      const existing = this.entries.get(parsed.spec.id)?.spec;
      const now = new Date().toISOString();
      const candidate: PipelineSpecV1 = {
        ...parsed.spec,
        createdAt: existing?.createdAt ?? parsed.spec.createdAt,
        updatedAt: now,
      };

      const staged = await this.loadStagedSpecs();
      staged.set(candidate.id, candidate);
      assertPipelinesAcyclic(staged.values());
      const resolver = this.createStagedResolver(staged);
      const validation = validateParsedPipeline(candidate, resolver);
      if (!validation.report.valid) throw new PipelineError("PIPELINE_INVALID", "pipeline validation failed", { report: validation.report });
      if (validation.report.assurance === "runtime_only" && options.allowRuntimeOnly !== true) {
        throw new PipelineError("PIPELINE_INVALID", "runtime-only compatibility requires allowRuntimeOnly: true", { report: validation.report });
      }

      const materialized: PipelineSpecV1 = {
        ...candidate,
        dependencies: validation.report.dependencies,
        review: {
          runtimeOnly: validation.report.assurance === "runtime_only",
          reviewedAt: now,
        },
      };
      const manifest = createGeneratedManifest(materialized);
      const previousFiles = await this.repository.snapshot(materialized.id);
      this.assertMutationAdmission(lockSignal);
      await this.repository.persist(materialized, manifest);
      try {
        this.assertMutationAdmission(lockSignal);
        await this.reconcileLocked(lockSignal);
        const saved = this.entries.get(materialized.id);
        if (!saved?.spec || saved.status.status !== "enabled" || !saved.status.registered) {
          throw new PipelineError("PIPELINE_INVALID", `saved pipeline could not be enabled: ${saved?.status.issues.map((item) => item.message).join("; ") ?? "unknown error"}`, {
            status: saved?.status,
          });
        }
        return { spec: deepCloneJson(saved.spec), status: deepCloneJson(saved.status), generatedTarget: generatedTarget(materialized.id) };
      } catch (error) {
        // The kernel-held advisory lock cannot be stolen while this process is
        // alive, so restoring the last-known-good files remains serialized.
        await this.repository.restore(materialized.id, previousFiles);
        if (!this.closing) await this.reconcileLocked(lockSignal);
        throw error;
      }
    }));
  }

  async delete(id: string, confirmed: boolean): Promise<{ deleted: boolean; id: string; disabledDependents: string[] }> {
    if (!confirmed) throw new PipelineError("PIPELINE_INVALID", "delete_pipeline requires confirm: true");
    this.assertMutationAdmission();
    return this.mutex.run(() => this.repository.withMutationLock(async (lockSignal) => {
      this.assertMutationAdmission(lockSignal);
      await this.reconcileLocked(lockSignal);
      const dependents = [...this.entries.values()].filter((entry) => entry.spec?.steps.some((step) => step.target === generatedTarget(id))).map((entry) => entry.id);
      this.assertMutationAdmission(lockSignal);
      const deleted = await this.repository.delete(id);
      this.assertMutationAdmission(lockSignal);
      await this.reconcileLocked(lockSignal);
      return { deleted, id, disabledDependents: dependents };
    }));
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

  get(id: string): {
    spec: PipelineSpecV1;
    status: PipelineStatus;
    dependencyStatus: { usable: boolean; issues: Issue[]; currentTargets: string[] };
  } | undefined {
    const entry = this.entries.get(id);
    if (!entry?.spec) return undefined;
    const dependencyStatus = checkPipelineDependencies(entry.spec, (target) => this.resolveRuntimeTarget(target));
    return {
      spec: deepCloneJson(entry.spec),
      status: deepCloneJson(entry.status),
      dependencyStatus: {
        usable: dependencyStatus.usable,
        issues: deepCloneJson(dependencyStatus.issues),
        currentTargets: [...dependencyStatus.current.keys()].sort(),
      },
    };
  }

  async run(id: string, input: unknown, context?: Parameters<PipelineExecutor["execute"]>[2]): Promise<import("../types.ts").PipelineRunResult> {
    const entry = this.entries.get(id);
    if (!entry?.snapshot) throw new PipelineError("PIPELINE_NOT_FOUND", `pipeline is not enabled: ${id}`, { status: entry?.status });
    return this.executor.execute(entry.snapshot, input, context);
  }

  async dryRun(value: { id?: string; spec?: unknown; pipelineInput: unknown; stepOutputs?: Record<string, unknown> }): Promise<{
    validation: ValidationReport;
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
      const staged = await this.loadStagedSpecs();
      const parsed = parsePipelineSpec(value.spec);
      if (parsed.spec) staged.set(parsed.spec.id, parsed.spec);
      const outcome = validatePipelineCandidate(value.spec, this.createStagedResolver(staged));
      spec = outcome.spec;
      validation = outcome.report;
    }
    if (!spec) return { validation, steps: [] };

    const outputs = new Map<string, unknown>();
    const steps: DryRunStep[] = [];
    let previousStepId: string | undefined;
    const dryRunSpecs = await this.loadStagedSpecs();
    dryRunSpecs.set(spec.id, spec);
    const resolver = this.createStagedResolver(dryRunSpecs);
    for (const step of spec.steps) {
      try {
        const input = constructStepInput(step, { pipelineInput: value.pipelineInput, outputs, previousStepId });
        const target = resolver(step.target);
        const validationError = target ? validateJsonSchemaValue(target.provide.inputSchema, input, `step ${step.id} input`) : `dependency unavailable: ${step.target}`;
        steps.push({ stepId: step.id, target: step.target, input, inputValid: !validationError, ...(validationError ? { error: validationError } : {}) });
      } catch (error) {
        steps.push({ stepId: step.id, target: step.target, inputValid: false, error: error instanceof Error ? error.message : String(error) });
      }
      if (value.stepOutputs && Object.prototype.hasOwnProperty.call(value.stepOutputs, step.id)) outputs.set(step.id, value.stepOutputs[step.id]);
      previousStepId = step.id;
    }
    try {
      return { validation, steps, output: selectPipelineOutput(spec, value.pipelineInput, outputs) };
    } catch (error) {
      return { validation, steps, outputError: error instanceof Error ? error.message : String(error) };
    }
  }

  private assertMutationAdmission(lockSignal?: AbortSignal): void {
    if (hasActivePipelineExecution()) throw new PipelineError("PIPELINE_REENTRANT_MUTATION", "active pipelines cannot invoke mutation management operations");
    if (lockSignal?.aborted) throw new PipelineError("PIPELINE_ABORTED", "pipeline mutation lock was compromised");
    if (this.closing) throw new PipelineError("PIPELINE_ABORTED", "pipeline service is shutting down");
  }

  private async disposeGeneratedRegistrations(deadline: number): Promise<PipelineDisposeResult["registrations"]> {
    const registrations = [...this.registrations.entries()];
    return Promise.all(registrations.map(async ([id, registration]) => {
      try {
        const disposed = await boundedDispose(registration.dispose(), remainingMs(deadline));
        if (disposed) this.registrations.delete(id);
        return disposed
          ? { nodeId: registration.nodeId, status: "disposed" as const }
          : { nodeId: registration.nodeId, status: "timed_out" as const, error: "registration drain exceeded shutdown deadline" };
      } catch (error) {
        return { nodeId: registration.nodeId, status: "failed" as const, error: error instanceof Error ? error.message : String(error) };
      }
    }));
  }

  private async reconcileLocked(lockSignal?: AbortSignal): Promise<PipelineStatus[]> {
    this.assertMutationAdmission(lockSignal);
    await this.repository.initialize();
    this.entries.clear();
    const records = await this.repository.readAll();
    const parsedSpecs = new Map<string, PipelineSpecV1>();

    for (const record of records) {
      if (record.error) {
        this.entries.set(record.id, quarantine(record.id, `cannot read pipeline.json: ${record.error}`));
        continue;
      }
      const parsed = parsePipelineSpec(record.value);
      if (!parsed.spec) {
        this.entries.set(record.id, {
          id: record.id,
          status: status(record.id, "quarantined", parsed.errors),
        });
        continue;
      }
      if (parsed.spec.id !== record.id) {
        this.entries.set(record.id, quarantine(record.id, `directory id ${record.id} does not match spec id ${parsed.spec.id}`));
        continue;
      }
      parsedSpecs.set(record.id, parsed.spec);
    }

    let cycles: string[][] = [];
    try {
      cycles = findPipelineCycles(parsedSpecs.values());
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const id of parsedSpecs.keys()) this.entries.set(id, quarantine(id, message));
    }
    const cyclic = cycleTargets(cycles);
    const derived = createDerivedTargets(parsedSpecs, cyclic);
    const resolver = createFabricTargetResolver(this.fabric, derived);

    for (const [id, spec] of parsedSpecs) {
      if (this.entries.has(id)) continue;
      const target = generatedTarget(id);
      if (cyclic.has(target)) {
        const cycle = cycles.find((path) => path.includes(target));
        this.entries.set(id, quarantine(id, `pipeline cycle detected: ${cycle?.join(" -> ") ?? target}`, spec));
        continue;
      }
      const validation = validateParsedPipeline(spec, resolver);
      const dependency = checkPipelineDependencies(spec, resolver);
      const issues = [...validation.report.errors, ...validation.report.warnings, ...dependency.issues];
      let entryStatus: PipelineStatus["status"] = "enabled";
      // Unavailable or changed pinned contracts disable a previously reviewed
      // pipeline; they do not turn its readable specification into corruption.
      if (!dependency.usable) entryStatus = "disabled";
      else if (!validation.report.valid) entryStatus = "quarantined";
      else if (validation.report.assurance === "runtime_only" && spec.review?.runtimeOnly !== true) entryStatus = "disabled";
      const manifest = createGeneratedManifest(spec);
      this.entries.set(id, {
        id,
        spec,
        manifest,
        report: validation.report,
        status: status(id, entryStatus, issues, spec, false, validation.report.assurance),
      });
    }

    // A pipeline cannot be enabled when it invokes a generated pipeline that is disabled.
    let changed = true;
    while (changed) {
      changed = false;
      for (const entry of this.entries.values()) {
        if (entry.status.status !== "enabled" || !entry.spec) continue;
        for (const step of entry.spec.steps) {
          const generatedId = idFromGeneratedTarget(step.target);
          if (!generatedId) continue;
          const dependency = this.entries.get(generatedId);
          if (!dependency || dependency.status.status !== "enabled") {
            entry.status.status = "disabled";
            entry.status.issues.push({ code: "DEPENDENCY_NOT_FOUND", message: `generated dependency is not enabled: ${step.target}`, stepId: step.id, target: step.target });
            changed = true;
            break;
          }
        }
      }
    }

    const enabledIds = new Set([...this.entries.values()]
      .filter((entry) => entry.status.status === "enabled" && entry.spec && entry.manifest)
      .map((entry) => entry.id));
    for (const [id, registration] of [...this.registrations]) {
      if (enabledIds.has(id)) continue;
      await registration.dispose();
      this.registrations.delete(id);
    }

    for (const entry of [...this.entries.values()].sort((left, right) => left.id.localeCompare(right.id))) {
      this.assertMutationAdmission(lockSignal);
      if (entry.status.status !== "enabled" || !entry.spec || !entry.manifest) continue;
      try {
        entry.snapshot = createRuntimeSnapshot(entry.spec);
        const current = this.registrations.get(entry.id);
        if (current) await replaceGeneratedPipeline(current, entry.manifest, entry.snapshot, this.executor);
        else this.registrations.set(entry.id, registerGeneratedPipeline(this.fabric, entry.manifest, entry.snapshot, this.executor));
        entry.status.registered = true;
      } catch (error) {
        const current = this.registrations.get(entry.id);
        if (current) {
          await current.dispose().catch(() => undefined);
          this.registrations.delete(entry.id);
        }
        entry.snapshot = undefined;
        entry.status.status = "quarantined";
        entry.status.registered = false;
        entry.status.issues.push({ code: "REGISTRATION_FAILED", message: error instanceof Error ? error.message : String(error) });
      }
    }

    // A late registration failure must also remove already-registered parents.
    changed = true;
    while (changed) {
      changed = false;
      for (const entry of this.entries.values()) {
        if (entry.status.status !== "enabled" || !entry.spec) continue;
        const unavailable = entry.spec.steps.find((step) => {
          const generatedId = idFromGeneratedTarget(step.target);
          if (!generatedId) return false;
          const dependency = this.entries.get(generatedId);
          return !dependency || dependency.status.status !== "enabled" || !dependency.status.registered;
        });
        if (!unavailable) continue;
        if (entry.status.registered) {
          await this.registrations.get(entry.id)?.dispose();
          this.registrations.delete(entry.id);
        }
        entry.status.status = "disabled";
        entry.status.registered = false;
        entry.snapshot = undefined;
        entry.status.issues.push({
          code: "DEPENDENCY_NOT_FOUND",
          message: `generated dependency failed registration: ${unavailable.target}`,
          stepId: unavailable.id,
          target: unavailable.target,
        });
        changed = true;
      }
    }
    const statuses = this.list().map(({ description: _description, tags: _tags, stepCount: _stepCount, dependencyPolicy: _policy, ...item }) => item);
    this.assertMutationAdmission(lockSignal);
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

  private resolveRuntimeTarget(target: string): ResolvedTarget | undefined {
    const parsed = parseTarget(target);
    if (parsed?.provide === GENERATED_RUN_PROVIDE && parsed.nodeId.startsWith(GENERATED_NODE_PREFIX)) {
      const id = parsed.nodeId.slice(GENERATED_NODE_PREFIX.length);
      const entry = this.entries.get(id);
      if (entry?.spec && entry.status.status === "enabled") {
        return createDerivedTargets(new Map([[id, entry.spec]]), new Set()).get(target);
      }
    }
    return createFabricTargetResolver(this.fabric)(target);
  }

  private createStagedResolver(specs: ReadonlyMap<string, PipelineSpecV1>): TargetResolver {
    return createFabricTargetResolver(this.fabric, createDerivedTargets(specs, new Set()));
  }
}

function remainingMs(deadline: number): number {
  return Math.max(0, Math.ceil(deadline - performance.now()));
}

function createDisposeResult(
  shutdown: Awaited<ReturnType<PipelineExecutor["shutdown"]>>,
  registrations: PipelineDisposeResult["registrations"],
): PipelineDisposeResult {
  return {
    status: shutdown.drained && registrations.every((item) => item.status === "disposed") ? "completed" : "degraded",
    activeRunsDrained: shutdown.activeRunsDrained,
    detachedDownstreamInvocationsDrained: shutdown.detachedDownstreamInvocationsDrained,
    pendingDetachedDownstreamInvocations: shutdown.pendingDetachedDownstreamInvocations,
    registrations,
  };
}

function boundedDispose(operation: Promise<void>, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    operation.then(
      () => { clearTimeout(timer); resolve(true); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

function createDerivedTargets(specs: ReadonlyMap<string, PipelineSpecV1>, excluded: ReadonlySet<string>): Map<string, ResolvedTarget> {
  const targets = new Map<string, ResolvedTarget>();
  for (const spec of specs.values()) {
    const target = generatedTarget(spec.id);
    if (excluded.has(target)) continue;
    const manifest = createGeneratedManifest(spec);
    const provide = manifest.provides[0];
    const snapshot: ProvideSnapshot = {
      name: provide.name,
      description: provide.description,
      inputSchema: provide.inputSchema as never,
      outputSchema: provide.outputSchema as never,
      execution: { type: "handler", handler: GENERATED_RUN_PROVIDE },
      ...(provide.tags ? { tags: [...provide.tags] } : {}),
      ...(provide.effects ? { effects: [...provide.effects] } : {}),
      nodeId: manifest.node.id,
      globalId: target,
    };
    targets.set(target, { provide: snapshot, packageId: `pi-pe/generated/${spec.id}`, nodeVersion: spec.version });
  }
  return targets;
}

function invalidReport(errors: Issue[]): ValidationReport {
  return { valid: false, assurance: "invalid", errors, warnings: [], dependencies: [] };
}

function status(
  id: string,
  entryStatus: PipelineStatus["status"],
  issues: Issue[],
  spec?: PipelineSpecV1,
  registered = false,
  assurance?: PipelineStatus["assurance"],
): PipelineStatus {
  return {
    id,
    target: generatedTarget(id),
    ...(spec ? { name: spec.name, version: spec.version, updatedAt: spec.updatedAt } : {}),
    status: entryStatus,
    ...(assurance ? { assurance } : {}),
    registered,
    issues: deepCloneJson(issues),
  };
}

function quarantine(id: string, message: string, spec?: PipelineSpecV1): LoadedEntry {
  return {
    id,
    ...(spec ? { spec } : {}),
    status: status(id, "quarantined", [{ code: "PIPELINE_INVALID", message }], spec),
  };
}

function idFromGeneratedTarget(target: string): string | undefined {
  const parsed = parseTarget(target);
  return parsed?.provide === GENERATED_RUN_PROVIDE && parsed.nodeId.startsWith(GENERATED_NODE_PREFIX)
    ? parsed.nodeId.slice(GENERATED_NODE_PREFIX.length)
    : undefined;
}

class AsyncMutex {
  private tail: Promise<void> = Promise.resolve();

  async run<T>(operation: () => Promise<T>): Promise<T> {
    const outcome = await this.enqueue(operation);
    if (!outcome.acquired) throw new Error("mutex acquisition unexpectedly timed out");
    return outcome.value;
  }

  runUntil<T>(deadline: number, operation: () => Promise<T>): Promise<{ acquired: true; value: T } | { acquired: false }> {
    return this.enqueue(operation, deadline);
  }

  private async enqueue<T>(operation: () => Promise<T>, deadline?: number): Promise<{ acquired: true; value: T } | { acquired: false }> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => { release = resolve; });
    if (deadline !== undefined) {
      const timeoutMs = remainingMs(deadline);
      let timer: ReturnType<typeof setTimeout> | undefined;
      const acquired = await Promise.race([
        previous.then(() => true as const),
        new Promise<false>((resolve) => { timer = setTimeout(() => resolve(false), timeoutMs); }),
      ]);
      if (timer) clearTimeout(timer);
      if (!acquired) {
        void previous.then(release);
        return { acquired: false };
      }
    } else {
      await previous;
    }
    try {
      return { acquired: true, value: await operation() };
    } finally {
      release();
    }
  }
}
