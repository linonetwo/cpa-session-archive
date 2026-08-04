import json
import sqlite3
import unittest

from migrate_attribution import migrate_archive, migrate_archive_batched, migrate_cpamp


class MigrationTests(unittest.TestCase):
    def test_archive_identity_and_model_fields(self):
        db = sqlite3.connect(":memory:")
        db.executescript(
            """
            CREATE TABLE records(
              id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
              metadata_json TEXT, facets_json TEXT
            );
            CREATE TABLE turn_records(
              id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
              facets_json TEXT
            );
            CREATE TABLE session_summaries(
              session_id TEXT PRIMARY KEY, key_id TEXT, model TEXT
            );
            CREATE TABLE record_facets(
              request_id TEXT, name TEXT, value TEXT,
              PRIMARY KEY(request_id,name,value)
            );
            CREATE TABLE session_facets(
              session_id TEXT, name TEXT, value TEXT,
              PRIMARY KEY(session_id,name,value)
            );
            """
        )
        metadata = json.dumps(
            {"key_id": "old-key", "target_model": "codex-csil-gpt"}
        )
        facets = json.dumps(
            {"key.id": ["old-key"], "model.requested": ["codex-csil-gpt"]}
        )
        db.execute(
            "INSERT INTO records VALUES(1,'old-key','codex-csil-gpt','codex-csil-gpt',?,?)",
            (metadata, facets, ),
        )
        db.execute(
            "INSERT INTO turn_records VALUES(1,'old-key','codex-csil-gpt','codex-csil-gpt',?)",
            (facets,),
        )
        db.execute(
            "INSERT INTO session_summaries VALUES('s','old-key','codex-csil-gpt')"
        )
        db.execute(
            "INSERT INTO record_facets VALUES('r','key.id','old-key')"
        )
        db.execute(
            "INSERT INTO session_facets VALUES('s','model.requested','codex-csil-gpt')"
        )

        migrate_archive(
            db, {"old-key": "new-key"}, {"codex-csil-gpt": "codex-csil/gpt"}
        )

        self.assertEqual(
            db.execute(
                "SELECT key_id,requested_model,model FROM records"
            ).fetchone(),
            ("new-key", "codex-csil/gpt", "codex-csil/gpt"),
        )
        self.assertIn(
            '"key_id":"new-key"',
            db.execute("SELECT metadata_json FROM records").fetchone()[0],
        )
        self.assertEqual(
            db.execute("SELECT value FROM record_facets").fetchone()[0], "new-key"
        )
        self.assertEqual(
            db.execute("SELECT value FROM session_facets").fetchone()[0],
            "codex-csil/gpt",
        )

    def test_cpamp_usage_and_alias_cleanup(self):
        db = sqlite3.connect(":memory:")
        db.executescript(
            """
            CREATE TABLE usage_events(
              id INTEGER PRIMARY KEY, api_key_hash TEXT, model TEXT,
              requested_model TEXT, resolved_model TEXT, raw_json TEXT,
              response_metadata_json TEXT
            );
            CREATE TABLE api_key_aliases(
              api_key_hash TEXT PRIMARY KEY, alias TEXT, updated_at_ms INTEGER
            );
            """
        )
        db.execute(
            "INSERT INTO usage_events VALUES(1,'old-key','old-model','old-model',"
            "'old-model',?,NULL)",
            (json.dumps({"api_key_hash": "old-key", "model": "old-model"}),),
        )
        db.execute("INSERT INTO api_key_aliases VALUES('old-key','old',0)")
        db.execute("INSERT INTO api_key_aliases VALUES('new-key','current',0)")

        migrate_cpamp(db, {"old-key": "new-key"}, {"old-model": "new-model"})

        self.assertEqual(
            db.execute(
                "SELECT api_key_hash,model,requested_model,resolved_model "
                "FROM usage_events"
            ).fetchone(),
            ("new-key", "new-model", "new-model", "new-model"),
        )
        self.assertEqual(
            db.execute("SELECT COUNT(*) FROM api_key_aliases").fetchone()[0], 1
        )

    def test_archive_batched_migration_updates_json_and_facets(self):
        db = sqlite3.connect(":memory:")
        db.executescript(
            """
            CREATE TABLE records(
              id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
              metadata_json TEXT, facets_json TEXT
            );
            CREATE TABLE turn_records(
              id INTEGER PRIMARY KEY, key_id TEXT, requested_model TEXT, model TEXT,
              facets_json TEXT
            );
            CREATE TABLE session_summaries(
              session_id TEXT PRIMARY KEY, key_id TEXT, model TEXT
            );
            CREATE TABLE record_facets(
              request_id TEXT, name TEXT, value TEXT,
              PRIMARY KEY(request_id,name,value)
            );
            CREATE TABLE session_facets(
              session_id TEXT, name TEXT, value TEXT,
              PRIMARY KEY(session_id,name,value)
            );
            """
        )
        facets = json.dumps(
            {"key.id": ["old-key"], "model.requested": ["old-model"]}
        )
        for identity in range(1, 4):
            db.execute(
                "INSERT INTO records VALUES(?,?,?,?,?,?)",
                (
                    identity,
                    "old-key",
                    "old-model",
                    "old-model",
                    json.dumps({"key_hash": "old-key"}),
                    facets,
                ),
            )
            db.execute(
                "INSERT INTO turn_records VALUES(?,?,?,?,?)",
                (identity, "old-key", "old-model", "old-model", facets),
            )
        db.execute(
            "INSERT INTO session_summaries VALUES('s','old-key','old-model')"
        )
        db.execute("INSERT INTO record_facets VALUES('r','key.id','old-key')")
        db.execute(
            "INSERT INTO session_facets VALUES('s','model.requested','old-model')"
        )
        db.commit()

        changed = migrate_archive_batched(
            db,
            {"old-key": "new-key"},
            {"old-model": "new-model"},
            batch_size=1,
            pause_seconds=0,
        )

        self.assertEqual(changed["records.key_id"], 3)
        self.assertEqual(
            db.execute(
                "SELECT COUNT(*) FROM records "
                "WHERE key_id='new-key' AND requested_model='new-model'"
            ).fetchone()[0],
            3,
        )
        self.assertIn(
            '"key_hash":"new-key"',
            db.execute("SELECT metadata_json FROM records LIMIT 1").fetchone()[0],
        )
        self.assertEqual(
            db.execute("SELECT value FROM record_facets").fetchone()[0], "new-key"
        )


if __name__ == "__main__":
    unittest.main()
