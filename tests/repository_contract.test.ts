import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { test } from "node:test";

const repository = resolve(import.meta.dirname, "..");
const ignoredDirectories = new Set([".git", "node_modules", "playwright-report", "test-results"]);
const forbiddenExtensions = new Set([".py", ".pyi", ".pyc", ".sh", ".bash", ".cjs", ".js", ".mjs", ".ps1", ".rb"]);

function files(directory = repository): string[] {
  const result: string[] = [];
  for (const entry of readdirSync(directory)) {
    if (ignoredDirectories.has(entry)) continue;
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) result.push(...files(path));
    else result.push(path);
  }
  return result;
}

test("repository scripts are TypeScript-only and temporary Forgejo/Harbor paths are absent", () => {
  const paths = files();
  const forbidden = paths
    .map((path) => relative(repository, path))
    .filter((path) => [...forbiddenExtensions].some((extension) => path.endsWith(extension)));
  assert.deepEqual(forbidden, []);
  assert.equal(paths.some((path) => relative(repository, path).startsWith(`.forgejo/`)), false);
  const text = paths
    .filter(
      (path) =>
        !path.endsWith("pnpm-lock.yaml") &&
        relative(repository, path) !== "tests/repository_contract.test.ts",
    )
    .map((path) => readFileSync(path, "utf8"))
    .join("\n");
  assert.doesNotMatch(text, /#!.*\b(?:python|bash|sh)\b/);
  assert.doesNotMatch(text, /\b(?:python3?|PYTHONPATH)\b/);
  assert.doesNotMatch(text, /harbor\.k3s\.onetwo\.website|\.forgejo\/workflows/);
});

test("GitHub CI and v0.8.1 release form one immutable GHCR-only contract", () => {
  const ci = readFileSync(join(repository, ".github/workflows/ci.yml"), "utf8");
  const release = readFileSync(join(repository, ".github/workflows/release.yml"), "utf8");
  const dockerfile = readFileSync(join(repository, "Dockerfile"), "utf8");
  assert.match(ci, /push:\n\s+branches: \[main\]/);
  assert.doesNotMatch(ci, /tags:/);
  assert.match(ci, /pnpm typecheck/);
  assert.match(ci, /pnpm test:scripts/);
  assert.match(ci, /go test -count=1 -race \.\/\.\.\./);
  assert.match(release, /tags: \["v0\.8\.1"\]/);
  assert.doesNotMatch(release, /workflow_dispatch|harbor|forgejo/i);
  assert.doesNotMatch(release, /tags:\s*[^\n]*(?::latest|:main|:v0\.8\.1)/i);
  assert.match(release, /tags: \$\{\{ env\.IMAGE \}\}:\$\{\{ github\.sha \}\}/);
  assert.match(release, /steps\.build\.outputs\.digest/);
  assert.match(release, /release-manifest\.json/);
  assert.match(release, /cpa-session-identity-migrate-linux-amd64/);
  assert.match(release, /cpa-session-archive-backup-linux-amd64/);
  assert.match(release, /verify_buildkit_attestations\.ts/);
  assert.match(dockerfile, /ARG GO_IMAGE=[^\n]+@sha256:[0-9a-f]{64}/);
  assert.match(dockerfile, /ARG GO_IMAGE=golang:1\.26\.7-bookworm@sha256:/);
  assert.match(dockerfile, /ARG RUNTIME_IMAGE=[^\n]+@sha256:[0-9a-f]{64}/);
  assert.doesNotMatch(dockerfile, /^RUN go test /m);
  const uses = [...`${ci}\n${release}`.matchAll(/^\s*- uses: ([^\s]+)(?:\s+#.*)?$/gm)].map(
    (match) => match[1],
  );
  assert.ok(uses.length > 0);
  for (const reference of uses) {
    assert.match(reference, /@[0-9a-f]{40}$/, `action is not pinned: ${reference}`);
  }
});
