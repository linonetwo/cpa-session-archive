import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import {
  ATTESTATION_ARTIFACT_TYPE,
  IN_TOTO_MEDIA_TYPE,
  SBOM_PREDICATE,
  type CraneRunner,
  verifyBuildkitAttestations,
} from "./verify_buildkit_attestations.js";

const temporaryDirectories: string[] = [];
afterEach(() => {
  while (temporaryDirectories.length > 0) rmSync(temporaryDirectories.pop()!, { recursive: true, force: true });
});

function temporary(): string {
  const directory = mkdtempSync(join(tmpdir(), "archive-attestations-test-"));
  temporaryDirectories.push(directory);
  return directory;
}

function encoded(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value));
}

function digest(body: Buffer): string {
  return `sha256:${createHash("sha256").update(body).digest("hex")}`;
}

interface Fixture {
  rootDigest: string;
  runner: CraneRunner;
  responses: Map<string, Buffer>;
}

function fixture(options: { provenance?: boolean; sbom?: boolean } = {}): Fixture {
  const runnableBody = Buffer.from("unused runnable manifest");
  const runnableDigest = digest(runnableBody);
  const statements: Record<string, unknown>[] = [];
  if (options.sbom !== false) {
    statements.push({
      _type: "https://in-toto.io/Statement/v1",
      subject: [{ name: "_", digest: { sha256: runnableDigest.slice(7) } }],
      predicateType: SBOM_PREDICATE,
      predicate: { spdxVersion: "SPDX-2.3" },
    });
  }
  if (options.provenance !== false) {
    statements.push({
      _type: "https://in-toto.io/Statement/v1",
      subject: [{ name: "_", digest: { sha256: runnableDigest.slice(7) } }],
      predicateType: "https://slsa.dev/provenance/v1",
      predicate: { buildDefinition: {}, runDetails: {} },
    });
  }
  const statementBodies = statements.map(encoded);
  const layers = statements.map((statement, index) => ({
    mediaType: IN_TOTO_MEDIA_TYPE,
    digest: digest(statementBodies[index]),
    annotations: { "in-toto.io/predicate-type": statement.predicateType },
  }));
  const attestationManifest = {
    schemaVersion: 2,
    mediaType: "application/vnd.oci.image.manifest.v1+json",
    artifactType: ATTESTATION_ARTIFACT_TYPE,
    subject: { digest: runnableDigest },
    layers,
  };
  const attestationBody = encoded(attestationManifest);
  const attestationDigest = digest(attestationBody);
  const root = {
    schemaVersion: 2,
    mediaType: "application/vnd.oci.image.index.v1+json",
    manifests: [
      { digest: runnableDigest, platform: { os: "linux", architecture: "amd64" } },
      {
        digest: attestationDigest,
        platform: { os: "unknown", architecture: "unknown" },
        annotations: {
          "vnd.docker.reference.type": "attestation-manifest",
          "vnd.docker.reference.digest": runnableDigest,
        },
      },
    ],
  };
  const rootBody = encoded(root);
  const rootDigest = digest(rootBody);
  const responses = new Map<string, Buffer>([
    [`manifest example.invalid/archive@${rootDigest}`, rootBody],
    [`manifest example.invalid/archive@${attestationDigest}`, attestationBody],
    ...layers.map((layer, index) => [`blob example.invalid/archive@${layer.digest}`, statementBodies[index]] as const),
  ]);
  const runner: CraneRunner = (arguments_) => {
    const response = responses.get(arguments_.join(" "));
    if (!response) throw new Error(`unexpected crane arguments: ${arguments_.join(" ")}`);
    return response;
  };
  return { rootDigest, runner, responses };
}

test("writes complete digest-verified evidence with private permissions", () => {
  const data = fixture();
  const output = join(temporary(), "evidence");
  const verification = verifyBuildkitAttestations({
    crane: "crane",
    image: "example.invalid/archive",
    digest: data.rootDigest,
    outputDirectory: output,
    runner: data.runner,
  });
  assert.equal(verification.verified, true);
  assert.equal(statSync(output).mode & 0o777, 0o700);
  assert.equal(statSync(join(output, "verification.json")).mode & 0o777, 0o600);
  assert.equal(JSON.parse(readFileSync(join(output, "verification.json"), "utf8")).rootDigest, data.rootDigest);
  assert.equal(
    Object.keys(Object.fromEntries([...data.responses].filter(([key]) => key.startsWith("blob ")))).length,
    2,
  );
});

test("fails closed without either required predicate", () => {
  for (const options of [{ provenance: false }, { sbom: false }]) {
    const data = fixture(options);
    const output = join(temporary(), "evidence");
    assert.throws(
      () => verifyBuildkitAttestations({
        crane: "crane",
        image: "example.invalid/archive",
        digest: data.rootDigest,
        outputDirectory: output,
        runner: data.runner,
      }),
      options.provenance === false ? /SLSA provenance is missing/ : /SBOM is missing/,
    );
    assert.equal(existsSync(output), false);
  }
});

test("rejects digest mismatch and refuses an existing evidence directory", () => {
  const data = fixture();
  const badRunner: CraneRunner = (arguments_) => {
    const body = data.runner(arguments_);
    return arguments_[0] === "blob" ? Buffer.concat([body, Buffer.from("tampered")]) : body;
  };
  assert.throws(
    () => verifyBuildkitAttestations({
      crane: "crane",
      image: "example.invalid/archive",
      digest: data.rootDigest,
      outputDirectory: join(temporary(), "tampered"),
      runner: badRunner,
    }),
    /content digest mismatch/,
  );

  const existing = join(temporary(), "existing");
  mkdirSync(existing);
  assert.throws(
    () => verifyBuildkitAttestations({
      crane: "crane",
      image: "example.invalid/archive",
      digest: data.rootDigest,
      outputDirectory: existing,
      runner: data.runner,
    }),
    /exist/i,
  );
});
