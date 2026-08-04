#!/usr/bin/env python3
"""Transactionally canonicalize caller identities and model names in SQLite.

The script understands both cpa-session-archive and CPA-Manager-Plus databases.
It creates an online SQLite backup before changing data and is safe to run
while the owning service continues using WAL mode.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


def parse_mapping(value: str) -> dict[str, str]:
    if value.startswith("@"):
        value = Path(value[1:]).read_text(encoding="utf-8")
    result = json.loads(value)
    if not isinstance(result, dict) or not all(
        isinstance(key, str) and isinstance(target, str)
        for key, target in result.items()
    ):
        raise argparse.ArgumentTypeError("mapping must be a JSON string-to-string object")
    return result


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f'PRAGMA table_info("{table}")')}


def replace_json(value: str | None, key_map: dict[str, str], model_map: dict[str, str]) -> str | None:
    if not value:
        return value
    try:
        document = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value

    key_fields = {"api_key_hash", "key_hash", "key_id", "caller_scope"}
    model_fields = {
        "model",
        "requested_model",
        "resolved_model",
        "target_model",
        "model.requested",
        "model.resolved",
        "model.target",
    }

    def walk(node: Any, parent_key: str = "") -> Any:
        if isinstance(node, dict):
            return {key: walk(item, key) for key, item in node.items()}
        if isinstance(node, list):
            return [walk(item, parent_key) for item in node]
        if isinstance(node, str):
            if parent_key in key_fields:
                return key_map.get(node, node)
            if parent_key in model_fields:
                return model_map.get(node, node)
        return node

    updated = walk(document)
    if updated == document:
        return value
    return json.dumps(updated, ensure_ascii=False, separators=(",", ":"))


def update_scalar(
    db: sqlite3.Connection, table: str, column: str, mapping: dict[str, str]
) -> int:
    if not mapping or not table_exists(db, table) or column not in columns(db, table):
        return 0
    changed = 0
    for source, target in mapping.items():
        changed += db.execute(
            f'UPDATE "{table}" SET "{column}"=? WHERE "{column}"=?', (target, source)
        ).rowcount
    return changed


def update_json_column(
    db: sqlite3.Connection,
    table: str,
    id_column: str,
    json_column: str,
    key_map: dict[str, str],
    model_map: dict[str, str],
) -> int:
    if not table_exists(db, table):
        return 0
    available = columns(db, table)
    if id_column not in available or json_column not in available:
        return 0
    changed = 0
    cursor = db.execute(
        f'SELECT "{id_column}","{json_column}" FROM "{table}" '
        f'WHERE COALESCE("{json_column}",\'\')<>\'\''
    )
    for identity, value in cursor:
        updated = replace_json(value, key_map, model_map)
        if updated != value:
            db.execute(
                f'UPDATE "{table}" SET "{json_column}"=? WHERE "{id_column}"=?',
                (updated, identity),
            )
            changed += 1
    return changed


def migrate_archive(
    db: sqlite3.Connection, key_map: dict[str, str], model_map: dict[str, str]
) -> dict[str, int]:
    result: dict[str, int] = {}
    if table_exists(db, "records"):
        candidates = tuple(key_map) + tuple(model_map)
        if candidates:
            marks = ",".join("?" for _ in candidates)
            rows = db.execute(
                f"SELECT id,metadata_json,facets_json FROM records WHERE "
                f"key_id IN ({marks}) OR requested_model IN ({marks}) OR model IN ({marks})",
                candidates * 3,
            ).fetchall()
            for identity, metadata, facets in rows:
                updated_metadata = replace_json(metadata, key_map, model_map)
                updated_facets = replace_json(facets, key_map, model_map)
                if updated_metadata != metadata or updated_facets != facets:
                    db.execute(
                        "UPDATE records SET metadata_json=?,facets_json=? WHERE id=?",
                        (updated_metadata, updated_facets, identity),
                    )
                    result["records.derived_json"] = result.get(
                        "records.derived_json", 0
                    ) + 1
    for table in ("records", "turn_records", "session_summaries"):
        result[f"{table}.key_id"] = update_scalar(db, table, "key_id", key_map)
    for table in ("records", "turn_records"):
        for column in ("requested_model", "model"):
            result[f"{table}.{column}"] = update_scalar(
                db, table, column, model_map
            )
    result["session_summaries.model"] = update_scalar(
        db, "session_summaries", "model", model_map
    )

    if table_exists(db, "record_facets"):
        for source, target in key_map.items():
            result["record_facets.key"] = result.get("record_facets.key", 0) + db.execute(
                "UPDATE OR IGNORE record_facets SET value=? "
                "WHERE name='key.id' AND value=?",
                (target, source),
            ).rowcount
            db.execute(
                "DELETE FROM record_facets WHERE name='key.id' AND value=?", (source,)
            )
        for source, target in model_map.items():
            placeholders = ("model.requested", "model.resolved", "model.target")
            result["record_facets.model"] = result.get("record_facets.model", 0) + db.execute(
                "UPDATE OR IGNORE record_facets SET value=? "
                "WHERE name IN (?,?,?) AND value=?",
                (target, *placeholders, source),
            ).rowcount
            db.execute(
                "DELETE FROM record_facets WHERE name IN (?,?,?) AND value=?",
                (*placeholders, source),
            )

    if table_exists(db, "session_facets"):
        for source, target in key_map.items():
            result["session_facets.key"] = result.get("session_facets.key", 0) + db.execute(
                "UPDATE OR IGNORE session_facets SET value=? "
                "WHERE name='key.id' AND value=?",
                (target, source),
            ).rowcount
            db.execute(
                "DELETE FROM session_facets WHERE name='key.id' AND value=?", (source,)
            )
        for source, target in model_map.items():
            result["session_facets.model"] = result.get("session_facets.model", 0) + db.execute(
                "UPDATE OR IGNORE session_facets SET value=? "
                "WHERE name IN ('model.requested','model.resolved','model.target') AND value=?",
                (target, source),
            ).rowcount
            db.execute(
                "DELETE FROM session_facets "
                "WHERE name IN ('model.requested','model.resolved','model.target') AND value=?",
                (source,),
            )

    return result


def _merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + value


def _archive_candidate_where(
    key_map: dict[str, str], model_map: dict[str, str]
) -> tuple[str, tuple[str, ...]]:
    clauses: list[str] = []
    values: list[str] = []
    if key_map:
        marks = ",".join("?" for _ in key_map)
        clauses.append(f"key_id IN ({marks})")
        values.extend(key_map)
    if model_map:
        marks = ",".join("?" for _ in model_map)
        clauses.extend(
            (f"requested_model IN ({marks})", f"model IN ({marks})")
        )
        values.extend(model_map)
        values.extend(model_map)
    return " OR ".join(clauses) or "0", tuple(values)


def _migrate_archive_rows_batched(
    db: sqlite3.Connection,
    table: str,
    json_columns: tuple[str, ...],
    key_map: dict[str, str],
    model_map: dict[str, str],
    batch_size: int,
    pause_seconds: float,
) -> dict[str, int]:
    if not table_exists(db, table):
        return {}
    available = columns(db, table)
    required = {"key_id", "requested_model", "model"}
    if not required.issubset(available):
        return {}
    json_columns = tuple(column for column in json_columns if column in available)
    where, values = _archive_candidate_where(key_map, model_map)
    selected = ("rowid", "key_id", "requested_model", "model", *json_columns)
    result: dict[str, int] = {}

    while True:
        rows = db.execute(
            f'SELECT {",".join(selected)} FROM "{table}" '
            f"WHERE {where} LIMIT ?",
            (*values, batch_size),
        ).fetchall()
        if not rows:
            break
        db.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                rowid, key_id, requested_model, model, *json_values = row
                next_key = key_map.get(key_id, key_id)
                next_requested = model_map.get(requested_model, requested_model)
                next_model = model_map.get(model, model)
                assignments = ["key_id=?", "requested_model=?", "model=?"]
                parameters: list[Any] = [next_key, next_requested, next_model]
                for column, value in zip(json_columns, json_values):
                    assignments.append(f'"{column}"=?')
                    parameters.append(replace_json(value, key_map, model_map))
                parameters.append(rowid)
                db.execute(
                    f'UPDATE "{table}" SET {",".join(assignments)} WHERE rowid=?',
                    parameters,
                )
                if next_key != key_id:
                    result[f"{table}.key_id"] = result.get(
                        f"{table}.key_id", 0
                    ) + 1
                if next_requested != requested_model:
                    result[f"{table}.requested_model"] = result.get(
                        f"{table}.requested_model", 0
                    ) + 1
                if next_model != model:
                    result[f"{table}.model"] = result.get(
                        f"{table}.model", 0
                    ) + 1
                if any(
                    replace_json(value, key_map, model_map) != value
                    for value in json_values
                ):
                    result[f"{table}.derived_json"] = result.get(
                        f"{table}.derived_json", 0
                    ) + 1
            db.commit()
        except Exception:
            db.rollback()
            raise
        if pause_seconds:
            time.sleep(pause_seconds)
    return result


def _replace_facets_batched(
    db: sqlite3.Connection,
    table: str,
    id_column: str,
    names: tuple[str, ...],
    mapping: dict[str, str],
    label: str,
    batch_size: int,
    pause_seconds: float,
) -> int:
    if not mapping or not table_exists(db, table):
        return 0
    changed = 0
    name_marks = ",".join("?" for _ in names)
    for source, target in mapping.items():
        while True:
            rows = db.execute(
                f'SELECT "{id_column}",name FROM "{table}" '
                f"WHERE name IN ({name_marks}) AND value=? LIMIT ?",
                (*names, source, batch_size),
            ).fetchall()
            if not rows:
                break
            db.execute("BEGIN IMMEDIATE")
            try:
                for identity, name in rows:
                    db.execute(
                        f'DELETE FROM "{table}" '
                        f'WHERE "{id_column}"=? AND name=? AND value=?',
                        (identity, name, source),
                    )
                    db.execute(
                        f'INSERT OR IGNORE INTO "{table}"'
                        f'("{id_column}",name,value) VALUES(?,?,?)',
                        (identity, name, target),
                    )
                    changed += 1
                db.commit()
            except Exception:
                db.rollback()
                raise
            if pause_seconds:
                time.sleep(pause_seconds)
    return changed


def _update_scalar_batched(
    db: sqlite3.Connection,
    table: str,
    column: str,
    mapping: dict[str, str],
    batch_size: int,
    pause_seconds: float,
) -> int:
    if not mapping or not table_exists(db, table) or column not in columns(db, table):
        return 0
    changed = 0
    for source, target in mapping.items():
        while True:
            rowids = [
                row[0]
                for row in db.execute(
                    f'SELECT rowid FROM "{table}" WHERE "{column}"=? LIMIT ?',
                    (source, batch_size),
                )
            ]
            if not rowids:
                break
            marks = ",".join("?" for _ in rowids)
            db.execute("BEGIN IMMEDIATE")
            try:
                changed += db.execute(
                    f'UPDATE "{table}" SET "{column}"=? '
                    f"WHERE rowid IN ({marks})",
                    (target, *rowids),
                ).rowcount
                db.commit()
            except Exception:
                db.rollback()
                raise
            if pause_seconds:
                time.sleep(pause_seconds)
    return changed


def migrate_archive_batched(
    db: sqlite3.Connection,
    key_map: dict[str, str],
    model_map: dict[str, str],
    batch_size: int = 64,
    pause_seconds: float = 0.025,
) -> dict[str, int]:
    """Migrate the live Longhorn archive using short, resumable transactions."""
    result: dict[str, int] = {}
    _merge_counts(
        result,
        _migrate_archive_rows_batched(
            db,
            "records",
            ("metadata_json", "facets_json"),
            key_map,
            model_map,
            batch_size,
            pause_seconds,
        ),
    )
    _merge_counts(
        result,
        _migrate_archive_rows_batched(
            db,
            "turn_records",
            ("facets_json",),
            key_map,
            model_map,
            batch_size,
            pause_seconds,
        ),
    )
    result["session_summaries.key_id"] = _update_scalar_batched(
        db,
        "session_summaries",
        "key_id",
        key_map,
        batch_size,
        pause_seconds,
    )
    result["session_summaries.model"] = _update_scalar_batched(
        db,
        "session_summaries",
        "model",
        model_map,
        batch_size,
        pause_seconds,
    )
    result["record_facets.key"] = _replace_facets_batched(
        db,
        "record_facets",
        "request_id",
        ("key.id", "caller.scope"),
        key_map,
        "key",
        batch_size,
        pause_seconds,
    )
    result["record_facets.model"] = _replace_facets_batched(
        db,
        "record_facets",
        "request_id",
        ("model.requested", "model.resolved", "model.target"),
        model_map,
        "model",
        batch_size,
        pause_seconds,
    )
    result["session_facets.key"] = _replace_facets_batched(
        db,
        "session_facets",
        "session_id",
        ("key.id", "caller.scope"),
        key_map,
        "key",
        batch_size,
        pause_seconds,
    )
    result["session_facets.model"] = _replace_facets_batched(
        db,
        "session_facets",
        "session_id",
        ("model.requested", "model.resolved", "model.target"),
        model_map,
        "model",
        batch_size,
        pause_seconds,
    )
    return result


def migrate_cpamp(
    db: sqlite3.Connection, key_map: dict[str, str], model_map: dict[str, str]
) -> dict[str, int]:
    result: dict[str, int] = {}
    for column in ("api_key_hash",):
        result[f"usage_events.{column}"] = update_scalar(
            db, "usage_events", column, key_map
        )
    for column in ("model", "requested_model", "resolved_model"):
        result[f"usage_events.{column}"] = update_scalar(
            db, "usage_events", column, model_map
        )
    for json_column in ("raw_json", "response_metadata_json"):
        result[f"usage_events.{json_column}"] = update_json_column(
            db, "usage_events", "id", json_column, key_map, model_map
        )
    if table_exists(db, "api_key_aliases"):
        for source in key_map:
            result["api_key_aliases.deleted"] = result.get(
                "api_key_aliases.deleted", 0
            ) + db.execute(
                "DELETE FROM api_key_aliases WHERE api_key_hash=?", (source,)
            ).rowcount
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--kind", required=True, choices=("archive", "cpamp"))
    parser.add_argument("--key-map", required=True, type=parse_mapping)
    parser.add_argument("--model-map", default="{}", type=parse_mapping)
    parser.add_argument("--backup")
    parser.add_argument(
        "--external-backup-ref",
        help="Skip the SQLite file backup only when this names an existing external snapshot.",
    )
    parser.add_argument("--skip-integrity-check", action="store_true")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--batch-pause-ms", type=float, default=25)
    args = parser.parse_args()

    source = sqlite3.connect(args.database, timeout=60)
    source.execute("PRAGMA busy_timeout=60000")
    backup_path: Path | None = None
    if not args.external_backup_ref:
        backup_path = Path(
            args.backup
            or f"{args.database}.bak-attribution-{time.strftime('%Y%m%d%H%M%S')}"
        )
        backup = sqlite3.connect(backup_path)
        source.backup(backup)
        backup.close()

    before = "skipped"
    if not args.skip_integrity_check:
        before = source.execute("PRAGMA integrity_check").fetchone()[0]
        if before != "ok":
            raise RuntimeError(f"source integrity check failed before migration: {before}")
    elif not args.external_backup_ref:
        raise RuntimeError("--skip-integrity-check requires --external-backup-ref")

    if args.kind == "archive":
        if args.batch_size < 1:
            raise RuntimeError("--batch-size must be positive")
        result = migrate_archive_batched(
            source,
            args.key_map,
            args.model_map,
            batch_size=args.batch_size,
            pause_seconds=max(0, args.batch_pause_ms) / 1000,
        )
    else:
        source.execute("BEGIN IMMEDIATE")
        try:
            result = migrate_cpamp(source, args.key_map, args.model_map)
            source.commit()
        except Exception:
            source.rollback()
            raise

    after = "skipped"
    if not args.skip_integrity_check:
        after = source.execute("PRAGMA integrity_check").fetchone()[0]
        if after != "ok":
            raise RuntimeError(f"source integrity check failed after migration: {after}")
    print(
        json.dumps(
            {
                "backup": str(backup_path) if backup_path else args.external_backup_ref,
                "integrity": after,
                "changed": result,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
