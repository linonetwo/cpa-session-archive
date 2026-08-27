#!/usr/bin/env -S node --import tsx
/** Fail closed unless BuildKit published complete SBOM and SLSA in-toto blobs. */

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

export const IN_TOTO_MEDIA_TYPE = "application/vnd.in-toto+json";
export const ATTESTATION_ARTIFACT_TYPE = "application/vnd.docker.attestation.manifest.v1+json";
export const SBOM_PREDICATE = "https://spdx.dev/Document";
export const SLSA_PREDICATES = new Set([
  "https://slsa.dev/provenance/v0.2",
  "https://slsa.dev/provenance/v1",
]);

type JsonObject = Record<string, unknown>;
export type CraneRunner = (arguments_: string[]) => Buffer;

function object(value: unknown, message: string): JsonObject {
  if (value === null || Array.isArray(value) || typeof value !== "object") throw new Error(message);
  return value as JsonObject;
}

function array(value: unknown, message: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(message);
  return value;
}

function sha256(body: Buffer): string {
  return createHash("sha256").update(body).digest("hex");
}

export function digestHex(digest: unknown): string {
  if (typeof digest !== "string" || !/^sha256:[0-9a-f]{64}$/.test(digest)) {
    throw new Error(`unsupported digest: ${String(digest)}`);
  }
  return digest.slice("sha256:".length);
}

function parseVerifiedJson(body: Buffer, digest: string): JsonObject {
  if (sha256(body) !== digestHex(digest)) throw new Error("registry object content digest mismatch");
  return object(JSON.parse(body.toString("utf8")), "registry object is not a JSON object");
}

export function createCraneRunner(crane: string): CraneRunner {
  return (arguments_) => {
    const result = spawnSync(crane, arguments_, {
      encoding: "buffer",
      maxBuffer: 128 * 1024 * 1024,
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`crane exited unsuccessfully (${result.status ?? result.signal ?? "unknown"})`);
    }
    return result.stdout;
  };
}

function stringField(container: JsonObject, name: string, message: string): string {
  const value = container[name];
  if (typeof value !== "string") throw new Error(message);
  return value;
}

export interface VerificationOptions {
  crane: string;
  image: string;
  digest: string;
  outputDirectory: string;
  runner?: CraneRunner;
}

export function verifyBuildkitAttestations(options: VerificationOptions): JsonObject {
  const rootDigest = `sha256:${digestHex(options.digest)}`;
  const runner = options.runner ?? createCraneRunner(options.crane);
  const rootBody = runner(["manifest", `${options.image}@${rootDigest}`]);
  const root = parseVerifiedJson(rootBody, rootDigest);
  if (
    root.schemaVersion !== 2 ||
    root.mediaType !== "application/vnd.oci.image.index.v1+json"
  ) throw new Error("BuildKit result is not an OCI v1 image index");
  const descriptors = array(root.manifests, "BuildKit result is not an OCI image index").map((entry) =>
    object(entry, "OCI descriptor is not an object"),
  );
  const runnableDigests = new Set<string>();
  for (const descriptor of descriptors) {
    const platform = object(descriptor.platform, "OCI descriptor has no platform");
    if (platform.os !== "unknown") runnableDigests.add(`sha256:${digestHex(descriptor.digest)}`);
  }
  if (runnableDigests.size === 0) throw new Error("OCI index has no valid runnable manifest");

  const predicates = new Map([...runnableDigests].map((digest) => [digest, new Set<string>()]));
  const blobs: Array<{ filename: string; body: Buffer }> = [];
  for (const descriptor of descriptors) {
    const annotations = object(descriptor.annotations ?? {}, "descriptor annotations are invalid");
    if (annotations["vnd.docker.reference.type"] !== "attestation-manifest") continue;
    const target = stringField(
      annotations,
      "vnd.docker.reference.digest",
      "attestation descriptor has no target digest",
    );
    if (!runnableDigests.has(target)) {
      throw new Error("attestation descriptor does not target a runnable manifest");
    }
    const platform = object(descriptor.platform, "attestation descriptor has no platform");
    if (platform.os !== "unknown" || platform.architecture !== "unknown") {
      throw new Error("attestation descriptor is not unknown/unknown");
    }
    const attestationDigest = `sha256:${digestHex(descriptor.digest)}`;
    const manifestBody = runner(["manifest", `${options.image}@${attestationDigest}`]);
    const manifest = parseVerifiedJson(manifestBody, attestationDigest);
    if (
      manifest.schemaVersion !== 2 ||
      manifest.mediaType !== "application/vnd.oci.image.manifest.v1+json"
    ) throw new Error("attestation object is not an OCI v1 manifest");
    if (manifest.artifactType !== ATTESTATION_ARTIFACT_TYPE) {
      throw new Error("attestation manifest does not use the OCI artifact type");
    }
    const subject = object(manifest.subject, "attestation manifest has no subject");
    if (subject.digest !== target) throw new Error("attestation manifest subject digest mismatch");
    const layers = array(manifest.layers, "attestation manifest has no layers");
    if (layers.length === 0) throw new Error("attestation manifest has no layers");
    for (const entry of layers) {
      const layer = object(entry, "attestation layer is invalid");
      if (layer.mediaType !== IN_TOTO_MEDIA_TYPE) {
        throw new Error("attestation layer is not a full in-toto JSON blob");
      }
      const layerDigest = `sha256:${digestHex(layer.digest)}`;
      const body = runner(["blob", `${options.image}@${layerDigest}`]);
      if (sha256(body) !== digestHex(layerDigest)) {
        throw new Error("attestation blob content digest mismatch");
      }
      const statement = object(JSON.parse(body.toString("utf8")), "attestation blob is not JSON");
      if (statement._type !== "https://in-toto.io/Statement/v1") {
        throw new Error("attestation blob is not an in-toto v1 statement");
      }
      const predicateType = stringField(statement, "predicateType", "attestation has no predicate type");
      const layerAnnotations = object(layer.annotations, "attestation layer has no annotations");
      if (layerAnnotations["in-toto.io/predicate-type"] !== predicateType) {
        throw new Error("predicate type annotation mismatch");
      }
      const subjects = array(statement.subject, "in-toto statement has no subjects");
      if (
        !subjects.some((entry) => {
          const candidate = object(entry, "in-toto subject is invalid");
          const digest = object(candidate.digest, "in-toto subject has no digest");
          return digest.sha256 === digestHex(target);
        })
      ) throw new Error("in-toto statement does not bind the target digest");
      const predicate = object(statement.predicate, "in-toto statement has no complete predicate object");
      if (Object.keys(predicate).length === 0) throw new Error("in-toto statement has no complete predicate object");
      if (predicateType === SBOM_PREDICATE && typeof predicate.spdxVersion !== "string") {
        throw new Error("native SBOM predicate has no SPDX version");
      }
      if (
        predicateType === "https://slsa.dev/provenance/v1" &&
        (predicate.buildDefinition === null || typeof predicate.buildDefinition !== "object" ||
          predicate.runDetails === null || typeof predicate.runDetails !== "object")
      ) throw new Error("native SLSA v1 predicate is incomplete");
      if (
        predicateType === "https://slsa.dev/provenance/v0.2" &&
        (typeof predicate.buildType !== "string" || predicate.builder === null || typeof predicate.builder !== "object")
      ) throw new Error("native SLSA v0.2 predicate is incomplete");
      predicates.get(target)?.add(predicateType);
      blobs.push({
        filename: `${digestHex(target).slice(0, 12)}-${digestHex(layerDigest).slice(0, 12)}.intoto.json`,
        body,
      });
    }
  }
  for (const [target, types] of predicates) {
    if (!types.has(SBOM_PREDICATE)) throw new Error(`native BuildKit SBOM is missing for ${target}`);
    if (![...SLSA_PREDICATES].some((type) => types.has(type))) {
      throw new Error(`native BuildKit SLSA provenance is missing for ${target}`);
    }
  }

  mkdirSync(options.outputDirectory, { recursive: false, mode: 0o700 });
  for (const { filename, body } of blobs) writeFileSync(join(options.outputDirectory, filename), body, { mode: 0o600 });
  const subjects = Object.fromEntries(
    [...predicates.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([target, types]) => [target, [...types].sort()]),
  );
  const verification = { rootDigest, subjects, verified: true };
  writeFileSync(
    join(options.outputDirectory, "verification.json"),
    `${JSON.stringify(verification, null, 2)}\n`,
    { mode: 0o600 },
  );
  return verification;
}

function parseArguments(argv: string[]): VerificationOptions {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    const value = argv[index + 1];
    if (!name?.startsWith("--") || value === undefined) throw new Error(`invalid argument: ${name ?? "<missing>"}`);
    values.set(name, value);
  }
  const crane = values.get("--crane");
  const image = values.get("--image");
  const digest = values.get("--digest");
  const outputDirectory = values.get("--output-dir");
  if (!crane || !image || !digest || !outputDirectory) {
    throw new Error("required: --crane PATH --image IMAGE --digest sha256:... --output-dir PATH");
  }
  return { crane, image, digest, outputDirectory };
}

export function main(argv = process.argv.slice(2)): void {
  verifyBuildkitAttestations(parseArguments(argv));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
