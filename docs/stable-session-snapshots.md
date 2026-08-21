# Stable session snapshot export

Version 0.8 implements the `session-snapshot-cursor-v1` source contract used by
incremental archive migration. The legacy `GET /v1/sessions` list and legacy
export tickets remain unchanged unless the stable protocol is explicitly
requested.

## Token Center collector-direct integration

Token Center must connect to this standalone collector with its
`--collector-direct <private-base-url>` adapter. The base URL is the collector
origin itself; do not add a CPA management compatibility alias or an Nginx
rewrite. Keep the collector private because it has no bearer-token middleware.
The opaque token in a returned `/archive-api/v1/exports/<token>` URL is the
short-lived download capability and must not be logged or exposed publicly.

The direct adapter discovers support through `GET /v1/stats`, creates or
continues snapshots through `GET /v1/sessions`, creates snapshot-bound
tickets through `GET /v1/export-tickets`, and downloads the returned relative
URL from the same origin. It must preserve the exact query bounds and
`after_ingest_fence` until a complete projection and every per-session digest
are verified. HTTP 503 means retry digest preparation, 410 means restart from
the last committed target checkpoint, and 429 means wait for snapshot
capacity. Never advance the target checkpoint on any of those responses.

The only full-snapshot switch is
`ARCHIVE_ALLOW_OFFLINE_FULL_SNAPSHOT=true`. It is for an isolated same-storage
clone, never the actively written source. Normal online runs leave it false and
use a prior ingest fence plus an explicit timestamp overlap.

## HTTP contract

Create a delta snapshot with:

```text
GET /v1/sessions?cursor_protocol=session-snapshot-cursor-v1
  &lower_bound_completed_at=2026-08-21T00:00:00Z
  &after_ingest_fence=12345
  &limit=1000
```

A successful response contains:

- `cursor_protocol`, opaque `snapshot`, and decimal `ingest_fence`;
- `session_count`, `request_count`, and `session_set_sha256`;
- `sessions`, each with `session_id`, `requests`, canonical UTC
  `first_at`/`last_at`, and `records_sha256`;
- `complete` and an opaque `next_cursor`.

Repeat the same lower bound, prior fence and limit with the returned
`snapshot` and `cursor`. Pages are ordered by
`(last_at DESC, session_id ASC bytewise)`. Retrying a page returns identical
items, metadata, digest, and next cursor. Cursors contain only an HMAC token;
they contain no session id, credential, payload, secret, or filesystem path.

Create a snapshot-bound archive ticket with:

```text
GET /v1/export-tickets?session_id=SESSION
  &scope=session&format=archive&snapshot=OPAQUE_SNAPSHOT
```

The ticket response repeats `cursor_protocol`, `snapshot`, and the selected
session's `records_sha256`. Its download streams the exact record versions
visible to the snapshot, ordered by request id. Each canonical JSON line has
sorted object keys, UTF-8 text, no insignificant whitespace, and one newline.

Altered, cross-snapshot, expired, wrong-limit, or wrong-bound cursors fail
closed. Expired snapshot requests return HTTP 410, capacity returns HTTP 429,
and a projection still being prepared returns HTTP 503. Unknown stable query
parameters never fall through to legacy facet filtering.

## Snapshot and WAL safety

A snapshot owns one SQLite read-only transaction. Its first clock read fixes
the WAL view; all pages, record references, CAS manifests, CAS blobs, digests,
and exports reuse that transaction. Inserts, updates, and deletes committed
after its high-water fence therefore cannot change a replay or export.

Long-lived readers delay WAL checkpointing, so the collector enforces:

- one active snapshot per process;
- a 15-minute absolute TTL and two-minute idle TTL for live delta snapshots;
- at most 1,000 summaries per page and bounded cursor length;
- cleanup every 30 seconds and opportunistically on every registry operation;
- an export deadline no later than the snapshot's absolute expiry.

Cleanup rolls back the read transaction before removing the registry entry.
Process shutdown or restart closes the database and invalidates all in-memory
snapshot and cursor identities. Clients must treat HTTP 410 as an expired
checkpoint attempt and create a new snapshot.

`GET /v1/stats` exposes `active_snapshots`,
`oldest_snapshot_age_seconds`, `max_active_snapshots`, both TTLs,
`pending_session_digests`, and `pending_session_digest_capacity`. It never
exposes opaque snapshot identities or archived metadata.

## Ingest fence and upgrade behavior

Upgrade does not backfill or rebuild the existing records table:

1. Empty ingest-event and digest tables are created.
2. A one-row clock is initialized from `MAX(records.id)`, which reads the end
   of the INTEGER PRIMARY KEY B-tree rather than payload rows.
3. INSERT, UPDATE, and DELETE triggers advance the clock and append a narrow
   event in the writer's existing transaction.

The DDL needs only a short SQLite schema write lock. It performs no full-table
UPDATE, no payload scan, and no blob rewrite. The old binary ignores the new
tables and can be restored without a data downgrade; the triggers continue to
capture mutations while that binary runs.

The first snapshot has no prior fence and enumerates all records in its fixed
transaction, so historical rows do not need event rows. Later snapshots select
the union of the timestamp overlap and sessions changed in
`(after_ingest_fence, ingest_fence]`, including old-timestamp late arrivals.

On a live collector, digest preparation is request-driven and de-duplicated in
a bounded 10,000-session queue. A request returns 503 until its selected
sessions are ready. The live background worker never walks every archived
payload. Queue saturation is visible in `/v1/stats`; retries enqueue later
waves without unbounded memory growth.

## Full migration runbook

Never create an unbounded first/full snapshot against the actively written CPA
database. The default collector rejects it with HTTP 403.

For an initial migration:

1. Create a storage-level clone or quiet copy on the same cluster storage.
   For a large archive, keep it on Longhorn/MinIO-adjacent infrastructure; do
   not copy the 10.7 GB database between machines.
2. Start an isolated collector against that clone with
   `ARCHIVE_ALLOW_OFFLINE_FULL_SNAPSHOT=true`. This is the only stable
   snapshot configuration switch.
3. Wait until a stable sessions request no longer returns projection-preparing
   HTTP 503. Offline mode performs bounded full digest preparation and permits
   one six-hour snapshot.
4. Export and verify every page, session count, request count, set digest,
   per-session digest, and snapshot-bound download.
5. Persist the returned ingest fence in the private target checkpoint.
6. Against the active source, run only delta snapshots with that
   `after_ingest_fence` and an explicit overlap lower bound.

If the collector is restarted, a ticket expires, a snapshot returns 410, or a
digest projection returns 503, discard only that attempt and retry from the
last committed target checkpoint. Never advance the checkpoint before the
complete projection and every archive digest have been verified.
