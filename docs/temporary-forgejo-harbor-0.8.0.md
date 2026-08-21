# Temporary Forgejo and Harbor path for 0.8.0

GitHub remains canonical for linonetwo/cpa-session-archive. The private
Forgejo repository mtc-ci/cpa-session-archive and Harbor repository
harbor.k3s.onetwo.website/mtc-ci/cpa-session-archive are a temporary,
non-deployment validation path.

The accurately named workflow is
.forgejo/workflows/temporary-cpa-session-archive-0.8.0-harbor.yml. It listens
only for pushes to master. Both jobs additionally require a push event,
refs/heads/master and a true protected-ref context. There is no manual
dispatch, temporary-branch trigger or historical-SHA bypass. Quality uses
mtc-quality-pod; release uses mtc-release-rootless and Pod-local rootless
BuildKit. Docker daemons, mutable image tags, Kubernetes and GitOps are outside
this path.

Forgejo's currently documented workflow schema does not provide GitHub-style
job environments with required reviewers, so this workflow does not claim an
environment approval gate. Do not install the five repository secrets until
operations has protected master against direct and force pushes and required
reviewed changes. A future historical build must be implemented only as a
strictly allowlisted build-context input in the workflow already loaded from
protected master; the target ref must never supply the workflow definition.

Required Actions secrets are HARBOR_USERNAME, HARBOR_PASSWORD,
COSIGN_PRIVATE_KEY, COSIGN_PASSWORD and COSIGN_PUBLIC_KEY. The only image tag
is the full 40-character commit SHA. The job verifies the tag digest, generates
and scans a CycloneDX SBOM, signs by digest, attaches the SBOM attestation, and
verifies signature and attestation. Before signing, the job downloads and
validates the complete native BuildKit SBOM and SLSA in-toto blobs, including
their OCI subject digests. A pinned upload action retains only non-secret
digest, SBOM and verification evidence for three days, even when publishing
fails.

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
