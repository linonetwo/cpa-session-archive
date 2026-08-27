import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import { migrateArchive, migrateArchiveBatched, migrateCpamp, parseMapping } from "./migrate_attribution.js";

const databases: DatabaseSync[] = [];
const temporaryDirectories: string[] = [];
afterEach(() => {
  while (databases.length > 0) databases.pop()?.close();
  while (temporaryDirectories.length > 0) rmSync(temporaryDirectories.pop()!, { recursive: true, force: true });
});

function database(schema: string): DatabaseSync {
  const db = new DatabaseSync(":memory:");
  db.exec(schema);
  databases.push(db);
  return db;
}

const archiveSchema = `
  CREATE TABLE records(
    id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
    metadata_json TEXT, facets_json TEXT
  );
  CREATE TABLE turn_records(
    id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
    facets_json TEXT
  );
  CREATE TABLE session_summaries(session_id TEXT PRIMARY KEY, key_id TEXT, model TEXT);
  CREATE TABLE record_facets(
    request_id TEXT, name TEXT, value TEXT, PRIMARY KEY(request_id,name,value)
  );
  CREATE TABLE session_facets(
    session_id TEXT, name TEXT, value TEXT, PRIMARY KEY(session_id,name,value)
  );
`;

test("archive model fields remain consistent without rewriting raw key audit values", () => {
  const db = database(archiveSchema);
  const metadata = JSON.stringify({ key_id: "old-key", target_model: "codex-csil-gpt" });
  const facets = JSON.stringify({ "key.id": ["old-key"], "model.requested": ["codex-csil-gpt"] });
  db.prepare("INSERT INTO records VALUES(1,'old-key','codex-csil-gpt','codex-csil-gpt',?,?)").run(
    metadata,
    facets,
  );
  db.prepare("INSERT INTO turn_records VALUES(1,'old-key','codex-csil-gpt','codex-csil-gpt',?)").run(facets);
  db.exec(`
    INSERT INTO session_summaries VALUES('s','old-key','codex-csil-gpt');
    INSERT INTO record_facets VALUES('r','key.id','old-key');
    INSERT INTO session_facets VALUES('s','model.requested','codex-csil-gpt');
  `);

  migrateArchive(db, {}, { "codex-csil-gpt": "codex-csil/gpt" });
  assert.deepEqual(
    { ...db.prepare("SELECT key_id,requested_model,model FROM records").get() },
    { key_id: "old-key", requested_model: "codex-csil/gpt", model: "codex-csil/gpt" },
  );
  assert.match(String(db.prepare("SELECT metadata_json FROM records").get()?.metadata_json), /"key_id":"old-key"/);
  assert.equal(db.prepare("SELECT value FROM record_facets").get()?.value, "old-key");
  assert.equal(db.prepare("SELECT value FROM session_facets").get()?.value, "codex-csil/gpt");
});

test("CPAMP usage and alias cleanup preserve the target alias", () => {
  const db = database(`
    CREATE TABLE usage_events(
      id INTEGER PRIMARY KEY, api_key_hash TEXT, model TEXT,
      requested_model TEXT, resolved_model TEXT, raw_json TEXT,
      response_metadata_json TEXT
    );
    CREATE TABLE api_key_aliases(api_key_hash TEXT PRIMARY KEY, alias TEXT, updated_at_ms INTEGER);
  `);
  db.prepare("INSERT INTO usage_events VALUES(1,'old-key','old-model','old-model','old-model',?,NULL)").run(
    JSON.stringify({ api_key_hash: "old-key", model: "old-model" }),
  );
  db.exec(`
    INSERT INTO api_key_aliases VALUES('old-key','old',0);
    INSERT INTO api_key_aliases VALUES('new-key','current',0);
  `);
  migrateCpamp(db, { "old-key": "new-key" }, { "old-model": "new-model" });
  assert.deepEqual(
    { ...db.prepare("SELECT api_key_hash,model,requested_model,resolved_model FROM usage_events").get() },
    {
      api_key_hash: "new-key",
      model: "new-model",
      requested_model: "new-model",
      resolved_model: "new-model",
    },
  );
  assert.equal(db.prepare("SELECT COUNT(*) AS count FROM api_key_aliases").get()?.count, 1);
});

test("batched archive migration updates every projection in bounded batches", () => {
  const db = database(archiveSchema);
  const facets = JSON.stringify({ "key.id": ["old-key"], "model.requested": ["old-model"] });
  const insertRecord = db.prepare("INSERT INTO records VALUES(?,?,?,?,?,?)");
  const insertTurn = db.prepare("INSERT INTO turn_records VALUES(?,?,?,?,?)");
  for (let identity = 1; identity <= 3; identity += 1) {
    insertRecord.run(identity, "old-key", "old-model", "old-model", JSON.stringify({ key_hash: "old-key" }), facets);
    insertTurn.run(identity, "old-key", "old-model", "old-model", facets);
  }
  db.exec(`
    INSERT INTO session_summaries VALUES('s','old-key','old-model');
    INSERT INTO record_facets VALUES('r','key.id','old-key');
    INSERT INTO session_facets VALUES('s','model.requested','old-model');
  `);
  const changed = migrateArchiveBatched(
    db,
    {},
    { "old-model": "new-model" },
    1,
    0,
  );
  assert.equal(changed["records.key_id"] ?? 0, 0);
  assert.equal(
    db.prepare("SELECT COUNT(*) AS count FROM records WHERE key_id='old-key' AND requested_model='new-model'").get()?.count,
    3,
  );
  assert.match(String(db.prepare("SELECT metadata_json FROM records LIMIT 1").get()?.metadata_json), /"key_hash":"old-key"/);
  assert.equal(db.prepare("SELECT value FROM record_facets").get()?.value, "old-key");
});

test("mapping rejects identity values that would make a batched migration loop forever", () => {
  assert.throws(() => parseMapping('{"same":"same"}'), /must differ/);
});

test("archive key rewrites fail closed so stable identity remains an additive projection", () => {
  const db = database(archiveSchema);
  assert.throws(
    () => migrateArchiveBatched(db, { "raw-key": "different-key" }, {}, 1, 0),
    /cpa-session-identity-migrate/,
  );
});

test("JSON normalization preserves integers outside JavaScript's safe range", () => {
  const db = database(archiveSchema);
  db.prepare("INSERT INTO records VALUES(1,'raw','old','old',?,NULL)").run(
    '{"model":"old","counter":9007199254740993123456789}',
  );
  migrateArchive(db, {}, { old: "new" });
  assert.equal(
    db.prepare("SELECT metadata_json FROM records").get()?.metadata_json,
    '{"model":"new","counter":9007199254740993123456789}',
  );
});

test("CLI creates an online backup before changing the source database", () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-attribution-test-"));
  temporaryDirectories.push(directory);
  const sourcePath = join(directory, "source.sqlite");
  const backupPath = join(directory, "backup.sqlite");
  const source = new DatabaseSync(sourcePath);
  source.exec(`
    CREATE TABLE usage_events(
      id INTEGER PRIMARY KEY, api_key_hash TEXT, model TEXT,
      requested_model TEXT, resolved_model TEXT, raw_json TEXT,
      response_metadata_json TEXT
    );
    INSERT INTO usage_events VALUES(1,'old-key','old-model','old-model','old-model',NULL,NULL);
  `);
  source.close();
  const command = spawnSync(
    process.execPath,
    [
      "--import",
      "tsx",
      join(import.meta.dirname, "migrate_attribution.ts"),
      "--database",
      sourcePath,
      "--kind",
      "cpamp",
      "--key-map",
      '{"old-key":"new-key"}',
      "--model-map",
      '{"old-model":"new-model"}',
      "--backup",
      backupPath,
    ],
    { encoding: "utf8", timeout: 15_000 },
  );
  assert.equal(command.status, 0, command.stderr);
  assert.equal(JSON.parse(command.stdout).integrity, "ok");
  const backupDatabase = new DatabaseSync(backupPath, { readOnly: true });
  const migratedDatabase = new DatabaseSync(sourcePath, { readOnly: true });
  assert.equal(backupDatabase.prepare("SELECT api_key_hash FROM usage_events").get()?.api_key_hash, "old-key");
  assert.equal(migratedDatabase.prepare("SELECT api_key_hash FROM usage_events").get()?.api_key_hash, "new-key");
  backupDatabase.close();
  migratedDatabase.close();
});

test("CLI refuses to replace an existing backup destination", () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-attribution-test-"));
  temporaryDirectories.push(directory);
  const sourcePath = join(directory, "source.sqlite");
  const backupPath = join(directory, "existing.sqlite");
  const source = new DatabaseSync(sourcePath);
  source.exec("CREATE TABLE usage_events(id INTEGER PRIMARY KEY)");
  source.close();
  const existing = new DatabaseSync(backupPath);
  existing.exec("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('preserve')");
  existing.close();
  const command = spawnSync(
    process.execPath,
    [
      "--import",
      "tsx",
      join(import.meta.dirname, "migrate_attribution.ts"),
      "--database",
      sourcePath,
      "--kind",
      "cpamp",
      "--key-map",
      "{}",
      "--backup",
      backupPath,
    ],
    { encoding: "utf8", timeout: 15_000 },
  );
  assert.equal(command.status, 1);
  assert.match(command.stderr, /already exists/);
  const unchanged = new DatabaseSync(backupPath, { readOnly: true });
  assert.equal(unchanged.prepare("SELECT value FROM sentinel").get()?.value, "preserve");
  unchanged.close();
});
