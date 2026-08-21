#!/usr/bin/env python3
"""Fail closed unless BuildKit published complete SBOM and SLSA in-toto blobs."""

import argparse
import hashlib
import json
import pathlib
import subprocess
from typing import Any

IN_TOTO_MEDIA_TYPE = "application/vnd.in-toto+json"
ATTESTATION_ARTIFACT_TYPE = "application/vnd.docker.attestation.manifest.v1+json"
SBOM_PREDICATE = "https://spdx.dev/Document"
SLSA_PREDICATES = {
    "https://slsa.dev/provenance/v0.2",
    "https://slsa.dev/provenance/v1",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crane", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--digest", required=True)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    return parser.parse_args()


def crane_bytes(crane: str, *arguments: str) -> bytes:
    result = subprocess.run(
        [crane, *arguments],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def parse_verified_json(body: bytes, digest: str) -> dict[str, Any]:
    if hashlib.sha256(body).hexdigest() != digest_hex(digest):
        raise ValueError("registry object content digest mismatch")
    return json.loads(body)


def digest_hex(digest: str) -> str:
    algorithm, separator, value = digest.partition(":")
    if separator != ":" or algorithm != "sha256" or len(value) != 64:
        raise ValueError(f"unsupported digest: {digest}")
    int(value, 16)
    return value


def main() -> None:
    args = parse_args()
    expected_root = f"sha256:{digest_hex(args.digest)}"
    root_body = crane_bytes(
        args.crane, "manifest", f"{args.image}@{expected_root}"
    )
    root = parse_verified_json(root_body, expected_root)
    if (
        root.get("schemaVersion") != 2
        or root.get("mediaType") != "application/vnd.oci.image.index.v1+json"
    ):
        raise ValueError("BuildKit result is not an OCI v1 image index")
    manifests = root.get("manifests")
    if not isinstance(manifests, list):
        raise ValueError("BuildKit result is not an OCI image index")

    runnable_digests = {
        descriptor.get("digest")
        for descriptor in manifests
        if descriptor.get("platform", {}).get("os") != "unknown"
    }
    if not runnable_digests or None in runnable_digests:
        raise ValueError("OCI index has no valid runnable manifest")

    subject_predicates: dict[str, set[str]] = {
        digest: set() for digest in runnable_digests
    }
    blobs: list[tuple[str, bytes]] = []
    for descriptor in manifests:
        annotations = descriptor.get("annotations", {})
        if annotations.get("vnd.docker.reference.type") != "attestation-manifest":
            continue
        target = annotations.get("vnd.docker.reference.digest")
        if target not in runnable_digests:
            raise ValueError("attestation descriptor does not target a runnable manifest")

        attestation_digest = descriptor.get("digest")
        digest_hex(attestation_digest)
        manifest_body = crane_bytes(
            args.crane, "manifest", f"{args.image}@{attestation_digest}"
        )
        manifest = parse_verified_json(manifest_body, attestation_digest)
        if (
            manifest.get("schemaVersion") != 2
            or manifest.get("mediaType")
            != "application/vnd.oci.image.manifest.v1+json"
        ):
            raise ValueError("attestation object is not an OCI v1 manifest")
        if manifest.get("artifactType") != ATTESTATION_ARTIFACT_TYPE:
            raise ValueError("attestation manifest does not use the OCI artifact type")
        if manifest.get("subject", {}).get("digest") != target:
            raise ValueError("attestation manifest subject digest mismatch")
        platform = descriptor.get("platform", {})
        if platform.get("os") != "unknown" or platform.get("architecture") != "unknown":
            raise ValueError("attestation descriptor is not unknown/unknown")

        layers = manifest.get("layers")
        if not isinstance(layers, list) or not layers:
            raise ValueError("attestation manifest has no layers")
        for layer in layers:
            if layer.get("mediaType") != IN_TOTO_MEDIA_TYPE:
                raise ValueError("attestation layer is not a full in-toto JSON blob")
            layer_digest = layer.get("digest")
            layer_hex = digest_hex(layer_digest)
            body = crane_bytes(
                args.crane, "blob", f"{args.image}@{layer_digest}"
            )
            if hashlib.sha256(body).hexdigest() != layer_hex:
                raise ValueError("attestation blob content digest mismatch")
            statement = json.loads(body)
            if statement.get("_type") != "https://in-toto.io/Statement/v1":
                raise ValueError("attestation blob is not an in-toto v1 statement")
            predicate_type = statement.get("predicateType")
            if layer.get("annotations", {}).get("in-toto.io/predicate-type") != predicate_type:
                raise ValueError("predicate type annotation mismatch")
            subjects = statement.get("subject")
            if not isinstance(subjects, list) or not any(
                subject.get("digest", {}).get("sha256") == digest_hex(target)
                for subject in subjects
            ):
                raise ValueError("in-toto statement does not bind the target digest")
            predicate = statement.get("predicate")
            if not isinstance(predicate, dict) or not predicate:
                raise ValueError("in-toto statement has no complete predicate object")
            if predicate_type == SBOM_PREDICATE and not predicate.get("spdxVersion"):
                raise ValueError("native SBOM predicate has no SPDX version")
            if predicate_type == "https://slsa.dev/provenance/v1" and (
                not isinstance(predicate.get("buildDefinition"), dict)
                or not isinstance(predicate.get("runDetails"), dict)
            ):
                raise ValueError("native SLSA v1 predicate is incomplete")
            if predicate_type == "https://slsa.dev/provenance/v0.2" and (
                not predicate.get("buildType")
                or not isinstance(predicate.get("builder"), dict)
            ):
                raise ValueError("native SLSA v0.2 predicate is incomplete")

            subject_predicates[target].add(predicate_type)
            blobs.append(
                (f"{digest_hex(target)[:12]}-{layer_hex[:12]}.intoto.json", body)
            )

    for target, predicates in subject_predicates.items():
        if SBOM_PREDICATE not in predicates:
            raise ValueError(f"native BuildKit SBOM is missing for {target}")
        if not predicates.intersection(SLSA_PREDICATES):
            raise ValueError(f"native BuildKit SLSA provenance is missing for {target}")

    args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    for filename, body in blobs:
        (args.output_dir / filename).write_bytes(body)
    verification = {
        "rootDigest": expected_root,
        "subjects": {
            target: sorted(predicates)
            for target, predicates in sorted(subject_predicates.items())
        },
        "verified": True,
    }
    (args.output_dir / "verification.json").write_text(
        json.dumps(verification, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
