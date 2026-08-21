#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKIT_HOST:?BUILDKIT_HOST is required}"
: "${IMAGE:?IMAGE is required}"
: "${GITHUB_SHA:?GITHUB_SHA is required}"
: "${BUILD_METADATA_FILE:?BUILD_METADATA_FILE is required}"

buildctl_bin="${BUILDCTL_BIN:-buildctl}"
"${buildctl_bin}" --addr "${BUILDKIT_HOST}" build \
  --frontend dockerfile.v0 \
  --local context=. \
  --local dockerfile=. \
  --opt filename=Dockerfile \
  --output "type=image,name=${IMAGE}:${GITHUB_SHA},push=true,oci-mediatypes=true,oci-artifact=true" \
  --attest type=sbom \
  --attest type=provenance,mode=max \
  --metadata-file "${BUILD_METADATA_FILE}"
