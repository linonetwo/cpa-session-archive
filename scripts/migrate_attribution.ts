#!/usr/bin/env -S node --import tsx
/** Transactionally canonicalize caller identities and model names in SQLite. */

import { existsSync, readFileSync, statSync } from "node:fs";
import { backup, DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";
import {
  isLosslessNumber,
  parse as parseLosslessJson,
  stringify as stringifyLosslessJson,
} from "lossless-json";

export type Mapping = Record<string, string>;
type Row = Record<string, unknown>;

const KEY_FIELDS = new Set(["api_key_hash", "key_hash", "key_id", "caller_scope"]);
const MODEL_FIELDS = new Set([
  "model",
  "requested_model",
  "resolved_model",
  "target_model",
  "model.requested",
  "model.resolved",
  "model.target",
]);

export function parseMapping(value: string): Mapping {
  const text = value.startsWith("@") ? readFileSync(value.slice(1), "utf8") : value;
  const parsed: unknown = JSON.parse(text);
  if (
    parsed === null ||
    Array.isArray(parsed) ||
    typeof parsed !== "object" ||
    !Object.entries(parsed).every(
      ([key, target]) => key.length > 0 && typeof target === "string" && target.length > 0,
    )
  ) {
    throw new Error("mapping must be a JSON string-to-string object with non-empty keys and values");
  }
  for (const [source, target] of Object.entries(parsed)) {
    if (source === target) throw new Error(`mapping source and target must differ: ${source}`);
  }
  return parsed as Mapping;
}

function one(db: DatabaseSync, sql: string, ...parameters: (string | number)[]): Row | undefined {
  return db.prepare(sql).get(...parameters) as Row | undefined;
}

function all(db: DatabaseSync, sql: string, ...parameters: (string | number)[]): Row[] {
  return db.prepare(sql).all(...parameters) as Row[];
}

function run(db: DatabaseSync, sql: string, ...parameters: (string | number | null)[]): number {
  return Number(db.prepare(sql).run(...parameters).changes);
}

function transaction<T>(db: DatabaseSync, operation: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = operation();
    db.exec("COMMIT");
    return result;
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

function sleep(milliseconds: number): void {
  if (milliseconds <= 0) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

export function tableExists(db: DatabaseSync, table: string): boolean {
  return one(
    db,
    "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
    table,
  ) !== undefined;
}

export function columns(db: DatabaseSync, table: string): Set<string> {
  return new Set(all(db, `PRAGMA table_info("${table}")`).map((row) => String(row.name)));
}

function walkJson(node: unknown, keyMap: Mapping, modelMap: Mapping, parentKey = ""): unknown {
  if (isLosslessNumber(node)) return node;
  if (Array.isArray(node)) return node.map((item) => walkJson(item, keyMap, modelMap, parentKey));
  if (node !== null && typeof node === "object") {
    return Object.fromEntries(
      Object.entries(node).map(([key, item]) => [key, walkJson(item, keyMap, modelMap, key)]),
    );
  }
  if (typeof node === "string") {
    if (KEY_FIELDS.has(parentKey)) return keyMap[node] ?? node;
    if (MODEL_FIELDS.has(parentKey)) return modelMap[node] ?? node;
  }
  return node;
}

export function replaceJson(
  value: string | null,
  keyMap: Mapping,
  modelMap: Mapping,
): string | null {
  if (!value) return value;
  let document: unknown;
  try {
    document = parseLosslessJson(value);
  } catch {
    return value;
  }
  const updated = walkJson(document, keyMap, modelMap);
  const encoded = stringifyLosslessJson(updated);
  if (encoded === undefined) return value;
  return encoded === stringifyLosslessJson(document) ? value : encoded;
}

function updateScalar(db: DatabaseSync, table: string, column: string, mapping: Mapping): number {
  if (Object.keys(mapping).length === 0 || !tableExists(db, table) || !columns(db, table).has(column)) {
    return 0;
  }
  return Object.entries(mapping).reduce(
    (changed, [source, target]) =>
      changed + run(db, `UPDATE "${table}" SET "${column}"=? WHERE "${column}"=?`, target, source),
    0,
  );
}

function updateJsonColumn(
  db: DatabaseSync,
  table: string,
  idColumn: string,
  jsonColumn: string,
  keyMap: Mapping,
  modelMap: Mapping,
): number {
  if (!tableExists(db, table)) return 0;
  const available = columns(db, table);
  if (!available.has(idColumn) || !available.has(jsonColumn)) return 0;
  let changed = 0;
  for (const row of all(
    db,
    `SELECT "${idColumn}" AS identity,"${jsonColumn}" AS value FROM "${table}" WHERE COALESCE("${jsonColumn}",'')<>''`,
  )) {
    const original = row.value === null ? null : String(row.value);
    const updated = replaceJson(original, keyMap, modelMap);
    if (updated !== original) {
      changed += run(
        db,
        `UPDATE "${table}" SET "${jsonColumn}"=? WHERE "${idColumn}"=?`,
        updated,
        String(row.identity),
      );
    }
  }
  return changed;
}

function addCount(target: Record<string, number>, key: string, value: number): void {
  target[key] = (target[key] ?? 0) + value;
}

/** Single-transaction compatibility helper retained for tests and offline callers. */
export function migrateArchive(db: DatabaseSync, keyMap: Mapping, modelMap: Mapping): Record<string, number> {
  if (Object.keys(keyMap).length > 0) {
    throw new Error(
      "archive key identity rewrites are forbidden; use cpa-session-identity-migrate to preserve raw key_id audit fields",
    );
  }
  const result: Record<string, number> = {};
  if (tableExists(db, "records")) {
    for (const row of all(db, "SELECT id,metadata_json,facets_json FROM records")) {
      const metadata = row.metadata_json === null ? null : String(row.metadata_json);
      const facets = row.facets_json === null ? null : String(row.facets_json);
      const updatedMetadata = replaceJson(metadata, keyMap, modelMap);
      const updatedFacets = replaceJson(facets, keyMap, modelMap);
      if (updatedMetadata !== metadata || updatedFacets !== facets) {
        run(
          db,
          "UPDATE records SET metadata_json=?,facets_json=? WHERE id=?",
          updatedMetadata,
          updatedFacets,
          Number(row.id),
        );
        addCount(result, "records.derived_json", 1);
      }
    }
  }
  for (const table of ["records", "turn_records", "session_summaries"]) {
    result[`${table}.key_id`] = updateScalar(db, table, "key_id", keyMap);
  }
  for (const table of ["records", "turn_records"]) {
    for (const column of ["requested_model", "model"]) {
      result[`${table}.${column}`] = updateScalar(db, table, column, modelMap);
    }
  }
  result["session_summaries.model"] = updateScalar(db, "session_summaries", "model", modelMap);
  migrateFacetsUnbatched(db, "record_facets", "request_id", keyMap, modelMap, result);
  migrateFacetsUnbatched(db, "session_facets", "session_id", keyMap, modelMap, result);
  return result;
}

function migrateFacetsUnbatched(
  db: DatabaseSync,
  table: string,
  idColumn: string,
  keyMap: Mapping,
  modelMap: Mapping,
  result: Record<string, number>,
): void {
  if (!tableExists(db, table)) return;
  const prefix = table === "record_facets" ? "record_facets" : "session_facets";
  for (const [source, target] of Object.entries(keyMap)) {
    addCount(
      result,
      `${prefix}.key`,
      run(db, `UPDATE OR IGNORE "${table}" SET value=? WHERE name='key.id' AND value=?`, target, source),
    );
    run(db, `DELETE FROM "${table}" WHERE name='key.id' AND value=?`, source);
  }
  for (const [source, target] of Object.entries(modelMap)) {
    addCount(
      result,
      `${prefix}.model`,
      run(
        db,
        `UPDATE OR IGNORE "${table}" SET value=? WHERE name IN ('model.requested','model.resolved','model.target') AND value=?`,
        target,
        source,
      ),
    );
    run(
      db,
      `DELETE FROM "${table}" WHERE name IN ('model.requested','model.resolved','model.target') AND value=?`,
      source,
    );
  }
}

function candidateWhere(keyMap: Mapping, modelMap: Mapping): { sql: string; values: string[] } {
  const clauses: string[] = [];
  const values: string[] = [];
  const keys = Object.keys(keyMap);
  const models = Object.keys(modelMap);
  if (keys.length > 0) {
    clauses.push(`key_id IN (${keys.map(() => "?").join(",")})`);
    values.push(...keys);
  }
  if (models.length > 0) {
    const marks = models.map(() => "?").join(",");
    clauses.push(`requested_model IN (${marks})`, `model IN (${marks})`);
    values.push(...models, ...models);
  }
  return { sql: clauses.join(" OR ") || "0", values };
}

function migrateArchiveRowsBatched(
  db: DatabaseSync,
  table: string,
  jsonColumns: string[],
  keyMap: Mapping,
  modelMap: Mapping,
  batchSize: number,
  pauseMilliseconds: number,
): Record<string, number> {
  if (!tableExists(db, table)) return {};
  const available = columns(db, table);
  if (!["key_id", "requested_model", "model"].every((column) => available.has(column))) return {};
  const selectedJsonColumns = jsonColumns.filter((column) => available.has(column));
  const { sql: where, values } = candidateWhere(keyMap, modelMap);
  const result: Record<string, number> = {};
  for (;;) {
    const selected = [
      "rowid AS _migration_rowid",
      ...["key_id", "requested_model", "model", ...selectedJsonColumns].map(
        (column) => `"${column}"`,
      ),
    ].join(",");
    const rows = all(db, `SELECT ${selected} FROM "${table}" WHERE ${where} LIMIT ?`, ...values, batchSize);
    if (rows.length === 0) break;
    transaction(db, () => {
      for (const row of rows) {
        const key = String(row.key_id ?? "");
        const requested = String(row.requested_model ?? "");
        const model = String(row.model ?? "");
        const nextKey = keyMap[key] ?? key;
        const nextRequested = modelMap[requested] ?? requested;
        const nextModel = modelMap[model] ?? model;
        const assignments = ["key_id=?", "requested_model=?", "model=?"];
        const parameters: (string | number | null)[] = [nextKey, nextRequested, nextModel];
        let derivedChanged = false;
        for (const column of selectedJsonColumns) {
          const original = row[column] === null ? null : String(row[column]);
          const updated = replaceJson(original, keyMap, modelMap);
          assignments.push(`"${column}"=?`);
          parameters.push(updated);
          derivedChanged ||= updated !== original;
        }
        parameters.push(Number(row._migration_rowid));
        run(db, `UPDATE "${table}" SET ${assignments.join(",")} WHERE rowid=?`, ...parameters);
        if (nextKey !== key) addCount(result, `${table}.key_id`, 1);
        if (nextRequested !== requested) addCount(result, `${table}.requested_model`, 1);
        if (nextModel !== model) addCount(result, `${table}.model`, 1);
        if (derivedChanged) addCount(result, `${table}.derived_json`, 1);
      }
    });
    sleep(pauseMilliseconds);
  }
  return result;
}

function updateScalarBatched(
  db: DatabaseSync,
  table: string,
  column: string,
  mapping: Mapping,
  batchSize: number,
  pauseMilliseconds: number,
): number {
  if (Object.keys(mapping).length === 0 || !tableExists(db, table) || !columns(db, table).has(column)) {
    return 0;
  }
  let changed = 0;
  for (const [source, target] of Object.entries(mapping)) {
    for (;;) {
      const rowids = all(
        db,
        `SELECT rowid AS _migration_rowid FROM "${table}" WHERE "${column}"=? LIMIT ?`,
        source,
        batchSize,
      ).map((row) => Number(row._migration_rowid));
      if (rowids.length === 0) break;
      changed += transaction(db, () =>
        run(
          db,
          `UPDATE "${table}" SET "${column}"=? WHERE rowid IN (${rowids.map(() => "?").join(",")})`,
          target,
          ...rowids,
        ),
      );
      sleep(pauseMilliseconds);
    }
  }
  return changed;
}

function replaceFacetsBatched(
  db: DatabaseSync,
  table: string,
  idColumn: string,
  names: string[],
  mapping: Mapping,
  batchSize: number,
  pauseMilliseconds: number,
): number {
  if (Object.keys(mapping).length === 0 || !tableExists(db, table)) return 0;
  let changed = 0;
  const nameMarks = names.map(() => "?").join(",");
  for (const [source, target] of Object.entries(mapping)) {
    for (;;) {
      const rows = all(
        db,
        `SELECT "${idColumn}" AS identity,name FROM "${table}" WHERE name IN (${nameMarks}) AND value=? LIMIT ?`,
        ...names,
        source,
        batchSize,
      );
      if (rows.length === 0) break;
      transaction(db, () => {
        for (const row of rows) {
          const identity = String(row.identity);
          const name = String(row.name);
          run(
            db,
            `DELETE FROM "${table}" WHERE "${idColumn}"=? AND name=? AND value=?`,
            identity,
            name,
            source,
          );
          run(
            db,
            `INSERT OR IGNORE INTO "${table}"("${idColumn}",name,value) VALUES(?,?,?)`,
            identity,
            name,
            target,
          );
          changed += 1;
        }
      });
      sleep(pauseMilliseconds);
    }
  }
  return changed;
}

export function migrateArchiveBatched(
  db: DatabaseSync,
  keyMap: Mapping,
  modelMap: Mapping,
  batchSize = 64,
  pauseMilliseconds = 25,
): Record<string, number> {
  if (!Number.isInteger(batchSize) || batchSize < 1) throw new Error("batch size must be a positive integer");
  if (Object.keys(keyMap).length > 0) {
    throw new Error(
      "archive key identity rewrites are forbidden; use cpa-session-identity-migrate to preserve raw key_id audit fields",
    );
  }
  const result: Record<string, number> = {};
  for (const [key, value] of Object.entries(
    migrateArchiveRowsBatched(
      db,
      "records",
      ["metadata_json", "facets_json"],
      keyMap,
      modelMap,
      batchSize,
      pauseMilliseconds,
    ),
  )) addCount(result, key, value);
  for (const [key, value] of Object.entries(
    migrateArchiveRowsBatched(
      db,
      "turn_records",
      ["facets_json"],
      keyMap,
      modelMap,
      batchSize,
      pauseMilliseconds,
    ),
  )) addCount(result, key, value);
  result["session_summaries.key_id"] = updateScalarBatched(
    db, "session_summaries", "key_id", keyMap, batchSize, pauseMilliseconds,
  );
  result["session_summaries.model"] = updateScalarBatched(
    db, "session_summaries", "model", modelMap, batchSize, pauseMilliseconds,
  );
  result["record_facets.key"] = replaceFacetsBatched(
    db, "record_facets", "request_id", ["key.id", "caller.scope"], keyMap, batchSize, pauseMilliseconds,
  );
  result["record_facets.model"] = replaceFacetsBatched(
    db,
    "record_facets",
    "request_id",
    ["model.requested", "model.resolved", "model.target"],
    modelMap,
    batchSize,
    pauseMilliseconds,
  );
  result["session_facets.key"] = replaceFacetsBatched(
    db, "session_facets", "session_id", ["key.id", "caller.scope"], keyMap, batchSize, pauseMilliseconds,
  );
  result["session_facets.model"] = replaceFacetsBatched(
    db,
    "session_facets",
    "session_id",
    ["model.requested", "model.resolved", "model.target"],
    modelMap,
    batchSize,
    pauseMilliseconds,
  );
  return result;
}

export function migrateCpamp(db: DatabaseSync, keyMap: Mapping, modelMap: Mapping): Record<string, number> {
  const result: Record<string, number> = {};
  result["usage_events.api_key_hash"] = updateScalar(db, "usage_events", "api_key_hash", keyMap);
  for (const column of ["model", "requested_model", "resolved_model"]) {
    result[`usage_events.${column}`] = updateScalar(db, "usage_events", column, modelMap);
  }
  for (const column of ["raw_json", "response_metadata_json"]) {
    result[`usage_events.${column}`] = updateJsonColumn(
      db, "usage_events", "id", column, keyMap, modelMap,
    );
  }
  if (tableExists(db, "api_key_aliases")) {
    for (const source of Object.keys(keyMap)) {
      addCount(
        result,
        "api_key_aliases.deleted",
        run(db, "DELETE FROM api_key_aliases WHERE api_key_hash=?", source),
      );
    }
  }
  return result;
}

interface Arguments {
  database: string;
  kind: "archive" | "cpamp";
  keyMap: Mapping;
  modelMap: Mapping;
  backupPath?: string;
  externalBackupRef?: string;
  skipIntegrityCheck: boolean;
  batchSize: number;
  batchPauseMilliseconds: number;
}

function parseArguments(argv: string[]): Arguments {
  const values = new Map<string, string>();
  const flags = new Set<string>();
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    if (name === "--skip-integrity-check") {
      flags.add(name);
      continue;
    }
    if (!name?.startsWith("--") || argv[index + 1] === undefined) {
      throw new Error(`invalid argument: ${name ?? "<missing>"}`);
    }
    values.set(name, argv[index + 1]);
    index += 1;
  }
  const database = values.get("--database");
  const kind = values.get("--kind");
  const keyMap = values.get("--key-map");
  if (!database || (kind !== "archive" && kind !== "cpamp") || !keyMap) {
    throw new Error("required: --database PATH --kind archive|cpamp --key-map JSON|@FILE");
  }
  return {
    database,
    kind,
    keyMap: parseMapping(keyMap),
    modelMap: parseMapping(values.get("--model-map") ?? "{}"),
    backupPath: values.get("--backup"),
    externalBackupRef: values.get("--external-backup-ref"),
    skipIntegrityCheck: flags.has("--skip-integrity-check"),
    batchSize: Number(values.get("--batch-size") ?? "64"),
    batchPauseMilliseconds: Number(values.get("--batch-pause-ms") ?? "25"),
  };
}

function timestamp(): string {
  const now = new Date();
  const part = (value: number) => String(value).padStart(2, "0");
  return `${now.getUTCFullYear()}${part(now.getUTCMonth() + 1)}${part(now.getUTCDate())}${part(now.getUTCHours())}${part(now.getUTCMinutes())}${part(now.getUTCSeconds())}`;
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const args = parseArguments(argv);
  if (!Number.isFinite(args.batchPauseMilliseconds)) throw new Error("--batch-pause-ms must be a number");
  if (!existsSync(args.database) || !statSync(args.database).isFile()) {
    throw new Error("source database must be an existing regular file");
  }
  if (args.skipIntegrityCheck && !args.externalBackupRef) {
    throw new Error("--skip-integrity-check requires --external-backup-ref");
  }
  const source = new DatabaseSync(args.database, { timeout: 60_000 });
  source.exec("PRAGMA busy_timeout=60000");
  let backupReference = args.externalBackupRef;
  try {
    if (!args.externalBackupRef) {
      const backupPath = args.backupPath ?? `${args.database}.bak-attribution-${timestamp()}`;
      if (existsSync(backupPath)) throw new Error("backup destination already exists");
      await backup(source, backupPath);
      backupReference = backupPath;
    }
    if (!args.skipIntegrityCheck) {
      const before = String(one(source, "PRAGMA integrity_check")?.integrity_check ?? "");
      if (before !== "ok") throw new Error(`source integrity check failed before migration: ${before}`);
    }
    let changed: Record<string, number>;
    if (args.kind === "archive") {
      changed = migrateArchiveBatched(
        source,
        args.keyMap,
        args.modelMap,
        args.batchSize,
        Math.max(0, args.batchPauseMilliseconds),
      );
    } else {
      changed = transaction(source, () => migrateCpamp(source, args.keyMap, args.modelMap));
    }
    let integrity = "skipped";
    if (!args.skipIntegrityCheck) {
      integrity = String(one(source, "PRAGMA integrity_check")?.integrity_check ?? "");
      if (integrity !== "ok") throw new Error(`source integrity check failed after migration: ${integrity}`);
    }
    process.stdout.write(`${JSON.stringify({ backup: backupReference, integrity, changed })}\n`);
  } finally {
    source.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
