#!/usr/bin/env bash
set -euo pipefail
repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow="$repository/.forgejo/workflows/temporary-cpa-session-archive-0.8.0-harbor.yml"
document="$repository/docs/temporary-forgejo-harbor-0.8.0.md"
test -f "$workflow"
test -f "$document"
test ! -e "$repository/.forgejo/workflows/build.yaml"
grep -Fq 'runs-on: mtc-quality-pod' "$workflow"
grep -Fq 'runs-on: mtc-release-rootless' "$workflow"
grep -Fq 'harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive' "$workflow"
grep -Fq 'BUILDKIT_HOST: unix:///run/user/1000/buildkit/buildkitd.sock' "$workflow"
grep -Fq 'buildctl --addr' "$workflow"
grep -Fq -- '--attest type=sbom' "$workflow"
grep -Fq -- '--attest type=provenance,mode=max' "$workflow"
grep -Fq 'trivy image --exit-code 1' "$workflow"
grep -Fq 'cosign sign --yes' "$workflow"
grep -Fq 'cosign verify-attestation' "$workflow"
grep -Fq '"${IMAGE}:${GITHUB_SHA}"' "$workflow"
grep -Fq '2026-09-22' "$document"
for secret in HARBOR_USERNAME HARBOR_PASSWORD COSIGN_PRIVATE_KEY COSIGN_PASSWORD COSIGN_PUBLIC_KEY; do
  grep -Fq "secrets.${secret}" "$workflow"
done
if grep -Eq '(^|[[:space:]])docker([[:space:]]|$)|buildx|services:|container:' "$workflow"; then
  echo "workflow must not depend on a Docker daemon" >&2
  exit 1
fi
if grep -Eq 'type=raw|tags:.*(latest|main)|IMAGE.*:(latest|main)' "$workflow"; then
  echo "workflow contains a mutable image tag" >&2
  exit 1
fi

