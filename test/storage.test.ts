import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { access, mkdir, mkdtemp, readFile, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { createGeneratedManifest } from "../src/generated/manifest.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { fixture } from "./helpers.ts";

test("repository writes readable artifacts and can restore an exact snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-"));
  const repository = new PipelineRepository(root);
  const spec = await fixture("mapped.pipeline.json");
  await repository.persist(spec, createGeneratedManifest(spec));
  const before = await repository.snapshot(spec.id);
  assert(before?.pipeline?.endsWith("\n"));
  assert.equal((await repository.readAll())[0].id, "mapped");

  const changed = { ...spec, name: "Changed", updatedAt: new Date().toISOString() };
  await repository.persist(changed, createGeneratedManifest(changed));
  assert.equal((await repository.read(spec.id) as { name: string }).name, "Changed");
  await repository.restore(spec.id, before);
  assert.equal((await repository.read(spec.id) as { name: string }).name, "Mapped example");

  await repository.writeIndex([{ id: "mapped", target: "pi_pe_pipeline_mapped.run", status: "enabled", registered: true, issues: [] }]);
  const index = JSON.parse(await readFile(join(root, "index.json"), "utf8"));
  assert.equal(index.schemaVersion, 1);
  assert.equal(index.pipelines[0].id, "mapped");
});

test("repository rejects path-like IDs and deletion is confined", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-safe-"));
  const repository = new PipelineRepository(root);
  await assert.rejects(() => repository.delete("../escape"), /Unsafe/);
  const spec = await fixture("mapped.pipeline.json");
  await repository.persist(spec, createGeneratedManifest(spec));
  assert.equal(await repository.delete(spec.id), true);
  assert.equal(await repository.delete(spec.id), false);
  await assert.rejects(() => access(join(root, "pipelines", spec.id)));
});

test("mutation locking does not depend on a host flock executable", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-portable-lock-"));
  const bin = join(root, "bin");
  await mkdir(bin);
  await writeFile(join(bin, "flock"), "#!/bin/sh\nexit 99\n", { mode: 0o700 });
  const previousPath = process.env.PATH;
  process.env.PATH = bin;
  try {
    const repository = new PipelineRepository(join(root, "state"));
    await repository.withMutationLock(async () => undefined, 500);
  } finally {
    if (previousPath === undefined) delete process.env.PATH;
    else process.env.PATH = previousPath;
  }
});

test("mutation lock is exclusive while held and reusable after release", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-lock-"));
  const repository = new PipelineRepository(root);
  let entered!: () => void;
  let release!: () => void;
  const enteredPromise = new Promise<void>((resolve) => { entered = resolve; });
  const releasePromise = new Promise<void>((resolve) => { release = resolve; });
  const owner = repository.withMutationLock(async () => {
    entered();
    await releasePromise;
  });
  await enteredPromise;
  await assert.rejects(() => repository.withMutationLock(async () => undefined, 25), /timed out waiting/);
  release();
  await owner;
  await repository.withMutationLock(async () => undefined, 500);
});

test("a crashed process leaves a recoverable mutation lock", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-lock-crash-"));
  const repositoryModule = pathToFileURL(join(process.cwd(), "src/storage/repository.ts")).href;
  const child = spawn(process.execPath, ["--import", "tsx", "--eval", `
    import { PipelineRepository } from ${JSON.stringify(repositoryModule)};
    const repository = new PipelineRepository(process.env.PI_PE_LOCK_ROOT);
    await repository.withMutationLock(async () => {
      process.stdout.write("locked\\n");
      setInterval(() => undefined, 1000);
      await new Promise(() => undefined);
    });
  `], {
    cwd: process.cwd(),
    env: { ...process.env, PI_PE_LOCK_ROOT: root },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout.on("data", (chunk) => { output += chunk.toString(); });
  await waitForOutput(child, "locked\n");
  child.kill("SIGKILL");
  const [code, signal] = await once(child, "exit");
  assert.equal(code, null);
  assert.equal(signal, "SIGKILL");
  const recovered = new PipelineRepository(root);
  await recovered.withMutationLock(async () => undefined, 7_000);
  assert.match(output, /locked/);
});

async function waitForOutput(child: ReturnType<typeof spawn>, expected: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let output = "";
    const onData = (chunk: Buffer | string) => {
      output += chunk.toString();
      if (output.includes(expected)) finish();
    };
    const onError = (error: Error) => finish(error);
    const onExit = (code: number | null, signal: string | null) => finish(new Error(`lock owner exited before readiness (${code ?? signal ?? "unknown"})`));
    const finish = (error?: Error) => {
      child.stdout?.removeListener("data", onData);
      child.removeListener("error", onError);
      child.removeListener("exit", onExit);
      error ? reject(error) : resolve();
    };
    child.stdout?.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

test("repository rejects a symlinked root before creating descendants", async () => {
  const parent = await mkdtemp(join(tmpdir(), "pi-pe-storage-root-link-"));
  const outside = await mkdtemp(join(tmpdir(), "pi-pe-storage-root-outside-"));
  const root = join(parent, "state");
  await symlink(outside, root, "dir");
  await assert.rejects(() => new PipelineRepository(root).initialize(), /not a symlink/);
  await assert.rejects(() => access(join(outside, "pipelines")));
});

test("repository refuses symlinked pipeline state directories", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-symlink-"));
  const outside = await mkdtemp(join(tmpdir(), "pi-pe-storage-outside-"));
  const repository = new PipelineRepository(root);
  await repository.initialize();
  await mkdir(join(outside, "mapped"));
  await symlink(join(outside, "mapped"), join(root, "pipelines", "mapped"), "dir");
  const spec = await fixture("mapped.pipeline.json");
  await assert.rejects(() => repository.persist(spec, createGeneratedManifest(spec)), /not a symlink/);
});
