import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
test("npm pack includes the ordinary Pi extension and runtime source", () => {
  const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")) as { files: string[]; pi: { extensions: string[] }; exports: Record<string, string>; dependencies?: Record<string, string>; bundleDependencies?: string[] };
  assert.equal(manifest.dependencies?.["@kybernetria/pi-tools"], undefined);
  assert.equal(manifest.bundleDependencies, undefined);
  const output = execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], { cwd: packageRoot, encoding: "utf8" });
  const files = new Set((JSON.parse(output) as Array<{ files: Array<{ path: string }> }>)[0]?.files.map((file) => file.path) ?? []);
  for (const file of ["extension.ts", "README.md", ...manifest.pi.extensions.map((path) => path.replace(/^\.\//, "")), ...Object.values(manifest.exports).map((path) => path.replace(/^\.\//, ""))]) assert(files.has(file), `missing packed file: ${file}`);
  assert.equal([...files].some((file) => file.includes("pi-tools") || file.includes("pi.protocol") || file.includes("protocol.generated")), false);
});
