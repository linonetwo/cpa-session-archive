import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import verify_buildkit_attestations as verifier


def encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


class VerifyBuildkitAttestationsTest(unittest.TestCase):
    def fixture(self, include_provenance: bool = True):
        runnable_body = b"unused runnable manifest"
        runnable_digest = "sha256:" + hashlib.sha256(runnable_body).hexdigest()
        statements = [
            {
                "_type": "https://in-toto.io/Statement/v1",
                "subject": [{"name": "_", "digest": {"sha256": runnable_digest[7:]}}],
                "predicateType": verifier.SBOM_PREDICATE,
                "predicate": {"spdxVersion": "SPDX-2.3"},
            }
        ]
        if include_provenance:
            statements.append(
                {
                    "_type": "https://in-toto.io/Statement/v1",
                    "subject": [
                        {"name": "_", "digest": {"sha256": runnable_digest[7:]}}
                    ],
                    "predicateType": "https://slsa.dev/provenance/v1",
                    "predicate": {
                        "buildDefinition": {},
                        "runDetails": {},
                    },
                }
            )
        statement_bodies = [encoded(statement) for statement in statements]
        layers = [
            {
                "mediaType": verifier.IN_TOTO_MEDIA_TYPE,
                "digest": "sha256:" + hashlib.sha256(body).hexdigest(),
                "annotations": {
                    "in-toto.io/predicate-type": statement["predicateType"]
                },
            }
            for statement, body in zip(statements, statement_bodies)
        ]
        attestation_manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": verifier.ATTESTATION_ARTIFACT_TYPE,
            "subject": {"digest": runnable_digest},
            "layers": layers,
        }
        attestation_body = encoded(attestation_manifest)
        attestation_digest = "sha256:" + hashlib.sha256(attestation_body).hexdigest()
        root = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "digest": runnable_digest,
                    "platform": {"os": "linux", "architecture": "amd64"},
                },
                {
                    "digest": attestation_digest,
                    "platform": {"os": "unknown", "architecture": "unknown"},
                    "annotations": {
                        "vnd.docker.reference.type": "attestation-manifest",
                        "vnd.docker.reference.digest": runnable_digest,
                    },
                },
            ]
        }
        root_body = encoded(root)
        root_digest = "sha256:" + hashlib.sha256(root_body).hexdigest()
        responses = {
            ("manifest", f"example.invalid/archive@{root_digest}"): root_body,
            ("manifest", f"example.invalid/archive@{attestation_digest}"):
                attestation_body,
        }
        responses.update(
            {
                ("blob", f"example.invalid/archive@{layer['digest']}"): body
                for layer, body in zip(layers, statement_bodies)
            }
        )
        return root_digest, responses

    def run_verifier(self, include_provenance: bool):
        root_digest, responses = self.fixture(include_provenance)

        def fake_run(arguments, **kwargs):
            self.assertEqual(arguments[0], "crane")
            return subprocess.CompletedProcess(
                arguments, 0, stdout=responses[tuple(arguments[1:])], stderr=b""
            )

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        output = pathlib.Path(temporary.name) / "evidence"
        arguments = [
            "verify_buildkit_attestations.py",
            "--crane",
            "crane",
            "--image",
            "example.invalid/archive",
            "--digest",
            root_digest,
            "--output-dir",
            str(output),
        ]
        with mock.patch.object(sys, "argv", arguments):
            with mock.patch.object(verifier.subprocess, "run", side_effect=fake_run):
                verifier.main()
        return output

    def test_writes_complete_verified_evidence(self):
        output = self.run_verifier(include_provenance=True)
        statements = list(output.glob("*.intoto.json"))
        self.assertEqual(len(statements), 2)
        verification = json.loads((output / "verification.json").read_text())
        self.assertTrue(verification["verified"])

    def test_fails_closed_without_slsa_provenance(self):
        with self.assertRaisesRegex(ValueError, "SLSA provenance is missing"):
            self.run_verifier(include_provenance=False)


if __name__ == "__main__":
    unittest.main()
