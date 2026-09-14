# v0.8.8 release contract

GitHub is the only source and release system, and GHCR is the only container
registry. The canonical code repository is `linonetwo/cpa-session-archive`.

`v0.8.8` succeeds the immutable `v0.8.7` release without moving, deleting, or
reusing an earlier tag or release. It keeps a client download socket active
with standards-defined interim responses while a stable snapshot is materialized
and verified before its final response.

## Identity and gates

The release workflow accepts only `refs/tags/v0.8.8`. Before publishing, it
requires all of the following:

- the tag checkout and `GITHUB_SHA` are the same complete 40-character commit;
- `internal/archive/version.go` declares `0.8.8`;
- the commit is reachable from `origin/main`;
- that exact main commit has a completed successful `ci` workflow run.

The CI workflow runs the Go, race, TypeScript, browser, native-binary, and
container gates on `main`; tag pushes do not bypass those checks. A missing or
ambiguous proof fails the release closed.

## Immutable artifacts

The release publishes one image tag containing the complete source commit:

```text
ghcr.io/linonetwo/cpa-session-archive:<40-character-commit>
```

Consumers use only the resulting immutable digest from `release-manifest.json`.
The workflow verifies the published digest, anonymous digest access, SBOM and
provenance attestations, and high/critical vulnerability scan before assembling
the GitHub Release. It never publishes `latest`, `main`, or a version image tag.

The release contains the native plugin, collector, identity migrator, backup
tool, checksums, image digest, and attestation evidence. Cluster manifests must
consume the recorded `ghcr.io/...@sha256:...` reference and never build an image
locally.
