# Temporary Forgejo and Harbor path for 0.8.0

GitHub remains canonical for linonetwo/cpa-session-archive. The private
Forgejo repository mtc-ci/cpa-session-archive and Harbor repository
harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive are a temporary,
non-deployment validation path.

The accurately named workflow is
.forgejo/workflows/temporary-cpa-session-archive-0.8.0-harbor.yml. It runs
only on chore/forgejo-harbor-0.8.0 or by manual dispatch. Quality uses
mtc-quality-pod; release uses mtc-release-rootless and Pod-local rootless
BuildKit. Docker daemons, mutable image tags, Kubernetes and GitOps are outside
this path.

Required Actions secrets are HARBOR_USERNAME, HARBOR_PASSWORD,
COSIGN_PRIVATE_KEY, COSIGN_PASSWORD and COSIGN_PUBLIC_KEY. The only image tag
is the full 40-character commit SHA. The job verifies the tag digest, generates
and scans a CycloneDX SBOM, signs by digest, attaches the SBOM attestation, and
verifies signature and attestation. BuildKit also emits native SBOM and
maximum-mode provenance.

## Removal deadline

Remove this path no later than **2026-09-22**:

1. Preserve the workflow URL, digest, SBOM and verification evidence with the
   canonical GitHub release.
2. Delete private Forgejo repository mtc-ci/cpa-session-archive.
3. Delete its five Actions secrets and rotate credentials if shared.
4. Remove the Harbor image and signature/attestation referrers after confirming
   there are no test consumers.
5. Delete the temporary GitHub branch and this workflow/document/contract in a
   reviewed GitHub commit.

The temporary image is not approved for deployment.

