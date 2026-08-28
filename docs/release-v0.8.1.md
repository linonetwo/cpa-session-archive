# v0.8.1 release contract

GitHub is the only source and release system, and GHCR is the only container
registry. The code repository remains canonical at
`linonetwo/cpa-session-archive`.

`v0.8.1` supersedes the unpublished `v0.8.0` candidate. The earlier immutable
tag remains preserved for audit, but its release workflow correctly stopped
after Trivy found 19 high-severity vulnerabilities in the unsupported Go
1.24.13 standard library. This patch candidate builds with Go 1.26.7 and does
not move or reuse the protected `v0.8.0` tag.

## Identity and gates

The release workflow accepts only `refs/tags/v0.8.1`. It verifies all of the
following before any publication job starts:

- the tag checkout and `GITHUB_SHA` are the same complete 40-character commit;
- `internal/archive/version.go` declares `0.8.1`;
- the commit is reachable from `origin/main`;
- the exact commit has a completed, successful `ci` workflow run caused by a
  push to `main`.

The CI workflow deliberately ignores tag pushes. Go, race, TypeScript,
Playwright, stable-snapshot, native-binary, and container tests therefore run
once on `main` instead of being duplicated for the tag. If the exact-SHA CI
proof is absent or ambiguous, the release fails closed.

Both Docker build stages pin their upstream images by full registry digest.
The container build compiles the already-tested exact SHA and does not repeat
the Go suite inside BuildKit. The builder is pinned to Go 1.26.7, which
includes all security fixes required by the v0.8.0 Trivy finding.

Before creating the tag, administrators must protect `main` and `v*` refs
against force updates and deletion, and enable immutable GitHub Releases. The
workflow itself cannot replace those repository controls.

## Immutable artifacts

The only container tag is the complete source commit:

```text
ghcr.io/linonetwo/cpa-session-archive:<40-character-commit>
```

It is an audit alias, not a deployment reference. Consumers use only the
verified `ghcr.io/linonetwo/cpa-session-archive@sha256:<digest>` value from
`release-manifest.json`. The workflow fails unless the SHA tag resolves to the
build digest, the digest is anonymously readable, vulnerability scanning
passes, and every runnable manifest has complete native BuildKit SPDX and SLSA
attestations.

The GitHub Release is created only after both the image evidence and all Linux
amd64 binaries are present. `SHA256SUMS` covers the binaries; the release
manifest records the digest and byte size of every release asset, plus the
source, exact commit, version, and workflow URL. No mutable container tag is
published.

## Local preflight

These checks do not publish or contact the cluster:

```bash
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test:scripts
go test ./...
go test -race ./...
pnpm test:e2e
```

The repository contract rejects Python, shell, JavaScript/CommonJS standalone
scripts, temporary Forgejo workflow paths, and non-SHA GitHub Action refs.
