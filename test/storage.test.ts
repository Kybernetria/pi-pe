import assert from "node:assert/strict";
import { access, mkdir, mkdtemp, readFile, readdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { atomicWriteFile } from "../src/storage/atomic-write.ts";
import { PipelineRepository } from "../src/storage/repository.ts";
import { fixture } from "./helpers.ts";

test("atomic writes remove temporary files when writing fails", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-atomic-failure-"));
  const path = join(root, "target.json");
  await assert.rejects(() => atomicWriteFile(path, 123 as unknown as string), /string|buffer|ArrayBuffer/i);
  assert.deepEqual((await readdir(root)).filter((entry) => entry.endsWith(".tmp")), []);
});

test("repository writes readable pipeline specifications and restores an exact snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-"));
  const repository = new PipelineRepository(root);
  const spec = await fixture("mapped.pipeline.json");
  await repository.persist(spec);
  const before = await repository.snapshot(spec.id);
  assert(before?.pipeline?.endsWith("\n"));
  assert.equal((await repository.readAll())[0].id, "mapped");
  const changed = { ...spec, name: "Changed", updatedAt: new Date().toISOString() };
  await repository.persist(changed);
  assert.equal((await repository.read(spec.id) as { name: string }).name, "Changed");
  await repository.restore(spec.id, before);
  assert.equal((await repository.read(spec.id) as { name: string }).name, "Mapped example");
  await repository.writeIndex([{ id: "mapped", status: "enabled", issues: [] }]);
  const index = JSON.parse(await readFile(join(root, "index.json"), "utf8"));
  assert.equal(index.schemaVersion, 1);
  assert.equal(index.pipelines[0].id, "mapped");
});

test("repository rejects path-like IDs and deletion is confined", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-safe-"));
  const repository = new PipelineRepository(root);
  await assert.rejects(() => repository.delete("../escape"), /Unsafe/);
  const spec = await fixture("mapped.pipeline.json");
  await repository.persist(spec);
  assert.equal(await repository.delete(spec.id), true);
  assert.equal(await repository.delete(spec.id), false);
  await assert.rejects(() => access(join(root, "pipelines", spec.id)));
});

test("repository refuses symlinked state roots and files", async () => {
  const parent = await mkdtemp(join(tmpdir(), "pi-pe-storage-root-parent-"));
  const outsideRoot = await mkdtemp(join(parent, "outside-root-"));
  const linkedRoot = join(parent, "linked-root");
  await symlink(outsideRoot, linkedRoot, "dir");
  const linkedRepository = new PipelineRepository(linkedRoot);
  await assert.rejects(() => linkedRepository.initialize(), /not a symlink/);
  await assert.rejects(() => linkedRepository.read("mapped"), /not a symlink/);
  await assert.rejects(() => linkedRepository.snapshot("mapped"), /not a symlink/);
  await assert.rejects(() => linkedRepository.delete("mapped"), /not a symlink/);
  await assert.rejects(() => access(join(outsideRoot, "pipelines")));

  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-file-symlink-"));
  const outside = await mkdtemp(join(tmpdir(), "pi-pe-storage-file-outside-"));
  const repository = new PipelineRepository(root);
  const spec = await fixture("mapped.pipeline.json");
  await repository.persist(spec);
  await rm(join(root, "pipelines", "mapped", "pipeline.json"));
  await symlink(join(outside, "pipeline.json"), join(root, "pipelines", "mapped", "pipeline.json"), "file");
  await assert.rejects(() => repository.snapshot(spec.id), /regular pipeline JSON file/);
});

test("repository refuses symlinked pipeline state directories", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-pe-storage-symlink-"));
  const outside = await mkdtemp(join(tmpdir(), "pi-pe-storage-outside-"));
  const repository = new PipelineRepository(root);
  await repository.initialize();
  await mkdir(join(outside, "mapped"));
  await symlink(join(outside, "mapped"), join(root, "pipelines", "mapped"), "dir");
  const spec = await fixture("mapped.pipeline.json");
  await assert.rejects(() => repository.persist(spec), /not a symlink/);
});
