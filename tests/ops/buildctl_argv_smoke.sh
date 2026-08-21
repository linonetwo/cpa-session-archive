#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output="$(mktemp)"
trap 'rm -f -- "${output}"' EXIT

BUILDKIT_HOST="unix:///run/user/1000/buildkit/buildkitd.sock" \
IMAGE="harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive" \
GITHUB_SHA="0123456789abcdef0123456789abcdef01234567" \
BUILD_METADATA_FILE="build-metadata.json" \
BUILDCTL_BIN="${repository}/tests/ops/record_argv.sh" \
ARGV_OUTPUT="${output}" \
  bash "${repository}/scripts/build_immutable_image.sh"

mapfile -t actual < "${output}"
expected=(
  "--addr"
  "unix:///run/user/1000/buildkit/buildkitd.sock"
  "build"
  "--frontend"
  "dockerfile.v0"
  "--local"
  "context=."
  "--local"
  "dockerfile=."
  "--opt"
  "filename=Dockerfile"
  "--output"
  "type=image,name=harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive:0123456789abcdef0123456789abcdef01234567,push=true,oci-mediatypes=true,oci-artifact=true"
  "--attest"
  "type=sbom"
  "--attest"
  "type=provenance,mode=max"
  "--metadata-file"
  "build-metadata.json"
)
test "${#actual[@]}" -eq "${#expected[@]}"
for index in "${!expected[@]}"; do
  test "${actual[${index}]}" = "${expected[${index}]}"
done
