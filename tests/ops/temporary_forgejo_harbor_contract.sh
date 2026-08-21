#!/usr/bin/env bash
# The quoted GitHub expressions below are intentionally matched as literals.
# shellcheck disable=SC2016
set -euo pipefail
repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow="$repository/.forgejo/workflows/temporary-cpa-session-archive-0.8.0-harbor.yml"
document="$repository/docs/temporary-forgejo-harbor-0.8.0.md"
actionlint_config="$repository/.github/actionlint.yaml"
test -f "$workflow"
test -f "$document"
test -f "$actionlint_config"
test ! -e "$repository/.forgejo/workflows/build.yaml"
grep -Fq 'runs-on: mtc-quality-pod' "$workflow"
grep -Fq 'runs-on: mtc-release-rootless' "$workflow"
grep -Fq '    - mtc-quality-pod' "$actionlint_config"
grep -Fq '    - mtc-release-rootless' "$actionlint_config"
grep -Fq 'harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive' "$workflow"
grep -Fq 'BUILDKIT_HOST: unix:///run/user/1000/buildkit/buildkitd.sock' "$workflow"
grep -Fq 'bash scripts/build_immutable_image.sh' "$workflow"
grep -Fq 'python3 scripts/verify_buildkit_attestations.py' "$workflow"
grep -Fq 'scripts.test_verify_buildkit_attestations' "$workflow"
grep -Fq 'shellcheck scripts/build_immutable_image.sh tests/ops/*.sh' "$workflow"
grep -Fq 'actionlint@03d0035246f3e81f36aed592ffb4bebf33a03106' "$workflow"
grep -Fq 'crane@59a4b85930392a30c39462519adc8a2026d47181' "$workflow"
grep -Fq 'trivy image --exit-code 1' "$workflow"
grep -Fq 'cosign sign --yes' "$workflow"
grep -Fq 'cosign verify-attestation' "$workflow"
grep -Fq '${IMAGE}:${GITHUB_SHA}' "$repository/scripts/build_immutable_image.sh"
protected_push_condition='if: ${{ github.event_name == '\''push'\'' && github.ref == '\''refs/heads/master'\'' && github.ref_protected == true }}'
test "$(grep -Fc "${protected_push_condition}" "$workflow")" -eq 2
grep -Fq '    branches: [master]' "$workflow"
if grep -Eq 'workflow_dispatch|chore/forgejo-harbor|5b03ec7fae3ffc2229c5a61aa2345ebad354fefe' "$workflow" "$document"; then
  echo "secret-bearing workflow must not expose dispatch, temporary refs or old-SHA allowlists" >&2
  exit 1
fi
grep -Fq 'test "${GITHUB_SERVER_URL}" = "https://git.k3s.onetwo.website"' "$workflow"
grep -Fq 'test "${GITHUB_REPOSITORY}" = "mtc-ci/cpa-session-archive"' "$workflow"
grep -Fq 'test "${GITHUB_EVENT_NAME}" = "push"' "$workflow"
grep -Fq 'test "${GITHUB_REF}" = "refs/heads/master"' "$workflow"
grep -Fq 'test "${GITHUB_REF_PROTECTED:-false}" = "true"' "$workflow"
grep -Fq 'test "$(git rev-parse --verify HEAD)" = "${GITHUB_SHA}"' "$workflow"
grep -Fq 'test -z "$(git status --porcelain=v1 --untracked-files=all)"' "$workflow"
grep -Fq 'umask 077' "$workflow"
grep -Fq 'trap cleanup EXIT HUP INT TERM' "$workflow"
grep -Fq 'if: ${{ always() }}' "$workflow"
grep -Fq 'retention-days: 3' "$workflow"
grep -Fq '2026-09-22' "$document"
for secret in HARBOR_USERNAME HARBOR_PASSWORD COSIGN_PRIVATE_KEY COSIGN_PASSWORD COSIGN_PUBLIC_KEY; do
  test "$(grep -Fc "secrets.${secret}" "$workflow")" -eq 1
done
publish_line="$(grep -nF -- '- name: Publish, scan, attest, sign, and verify immutable image' "$workflow" | cut -d: -f1)"
upload_line="$(grep -nF -- '- name: Upload non-secret release evidence' "$workflow" | cut -d: -f1)"
while IFS=: read -r line _; do
  test "${line}" -gt "${publish_line}"
  test "${line}" -lt "${upload_line}"
done < <(grep -nE 'secrets\.(HARBOR_USERNAME|HARBOR_PASSWORD|COSIGN_PRIVATE_KEY|COSIGN_PASSWORD|COSIGN_PUBLIC_KEY)' "$workflow")
if tail -n "+${upload_line}" "$workflow" | grep -Eq 'secrets\.|HARBOR_PASSWORD|COSIGN_PRIVATE_KEY|COSIGN_PASSWORD|COSIGN_PUBLIC_KEY'; then
  echo "evidence upload step must not receive release secrets" >&2
  exit 1
fi
native_verify_line="$(grep -nF 'python3 scripts/verify_buildkit_attestations.py' "$workflow" | cut -d: -f1)"
sign_line="$(grep -nF 'cosign sign --yes' "$workflow" | cut -d: -f1)"
release_proof_line="$(grep -nF '> release-proof.json' "$workflow" | cut -d: -f1)"
test "${native_verify_line}" -lt "${sign_line}"
test "${sign_line}" -lt "${release_proof_line}"
test "$(grep -Ec '^[[:space:]]*- uses: [^@]+@[0-9a-f]{40}$' "$workflow")" -eq \
  "$(grep -Ec '^[[:space:]]*- uses:' "$workflow")"
test "$(grep -Ec '^[[:space:]]*(HARBOR_USERNAME|HARBOR_PASSWORD|COSIGN_PRIVATE_KEY|COSIGN_PASSWORD|COSIGN_PUBLIC_KEY):' "$workflow")" -eq 5
if grep -Fq 'GITHUB_ENV' "$workflow"; then
  echo "workflow must not persist values through GITHUB_ENV" >&2
  exit 1
fi
if grep -Eq '(^|[[:space:]])\+([[:space:]]|$)' "$workflow"; then
  echo "workflow contains a literal plus instead of a shell continuation" >&2
  exit 1
fi
if grep -Eq '(^|[[:space:]])docker([[:space:]]|$)|buildx|services:|container:' "$workflow"; then
  echo "workflow must not depend on a Docker daemon" >&2
  exit 1
fi
bash "$repository/tests/ops/buildctl_argv_smoke.sh"
if grep -Eq 'type=raw|tags:.*(latest|main)|IMAGE.*:(latest|main)' "$workflow"; then
  echo "workflow contains a mutable image tag" >&2
  exit 1
fi
