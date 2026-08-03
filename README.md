# cpa-session-archive

A fail-open, content-addressed session archive plugin for [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI).

It captures original requests and complete streaming/non-streaming responses through CPA's native plugin lifecycle, groups them by session, and stores a compact training-data archive in SQLite WAL.

## Why

Codex and other agent clients resend large instructions, tool schemas, conversation context and attachments. Naively storing every request duplicates the same image, audio, video or prompt fragments many times and can grow to tens of gigabytes quickly.

This project uses structural content-addressed storage:

- Large UTF-8 strings are stored once by SHA-256.
- data:*;base64 attachments are decoded and stored once as their original binary bytes.
- Large JSON subtrees are stored once and referenced from compact manifests.
- Exact duplicate payloads reuse the same manifest blob.
- encrypted_content and internal passthrough metadata are discarded because they are not useful training text.
- Upstream translated requests are disabled by default because they usually duplicate the original request.
- Blobs are gzip-compressed; records contain only hashes and minimal searchable metadata.
- Successful Responses API SSE streams retain the authoritative
  `response.completed` event instead of duplicating created, delta and
  in-progress state. Incomplete and failed streams remain intact for diagnosis.

Exports transparently rehydrate the JSON structure, including repeated references to one attachment blob.

## Architecture

    CPA native plugin -> bounded memory queue -> collector -> SQLite WAL + CAS blobs

The request path never waits for disk I/O. A full queue or unavailable
collector does not fail the model request and is retried with bounded
backpressure. Accepted collector batches wait through transient SQLite locks
instead of being silently discarded.

## Build

Tagged releases are built and published to GHCR by GitHub Actions. The same
workflow runs Go tests, browser end-to-end tests, native plugin builds, and an
image build before publishing; local or cluster nodes do not need to run
Docker builds.

The image contains both /plugin/cpa-session-archive.so and the collector executable.

## CPA configuration

~~~yaml
plugins:
  enabled: true
  dir: /root/.cli-proxy-api/plugins
  configs:
    cpa-session-archive:
      enabled: true
      endpoint: http://cpa-session-collector:8080
      queue_size: 64
      max_body_bytes: 67108864
      timeout: 30s
      store_upstream_request: false
~~~

## Collector

~~~bash
ARCHIVE_DB=/data/archive.sqlite STORE_UPSTREAM_REQUEST=false cpa-session-collector
~~~

Endpoints:

- GET /healthz
- GET /v1/stats
- GET /v1/facets
- GET /v1/sessions?limit=100
- GET /v1/sessions/{session-id}
- GET /v1/requests/{request-id}
- GET /v1/request-view?id={request-id}
- GET /v1/turns?session_id={session-id}&order=asc|desc
- GET /v1/turns?session_id={session-id}&turn_id={turn-id}&limit=20&offset=0
- GET /v1/request-context?id={request-id}&before=0&limit=16
- GET /v1/sessions/{session-id}/export
- GET /v1/export-tickets?scope=session|all&format=archive|sft&session_id={session-id} (management proxy only)
- GET /archive-api/v1/exports/{ticket}
- POST /v1/maintenance/gc

## Management UI and faceted search

With CPA management API support enabled, CPA-Manager-Plus shows a session archive plugin page. The page provides storage statistics, dynamic facet selectors and routed session, turn, request, tool-call, and raw-diagnostic views. A session first shows only each user command and the corresponding final/latest natural-language response. Expanding a turn reveals intermediate assistant explanations, compaction points, and compact tool summaries; opening a tool summary is the third level that materializes its complete arguments and result. Both the turn index and long internal-request sequences are paginated. System instructions, raw diagnostics, tool bodies, and long fields are materialized only after a click, so a large coding session does not freeze the page.

Responses clients commonly resend the complete conversation in every stateless request. The archive retains those lossless protocol records, while the human timeline compares each request with its predecessor and renders only newly added messages, tool results, and assistant output. Codex records are reconstructed with their durable `turn.id`; Kimi K3, Kimi K2.7, and historical clients without a turn identifier use consecutive normalized user-command runs. Compaction, retries, and tool-result continuations remain inside the same visible turn. A compaction is therefore an inline process marker, never a new session card. Background/system threads remain identifiable through the `thread.source` facet.

Session and whole-database downloads first obtain a random ticket through the authenticated management API, then stream newline-delimited JSON directly from the collector-only download path with a server-controlled `.jsonl` filename. A ticket remains reusable for 30 minutes, allowing the visible fallback link to work after an embedded browser or download manager has already probed it. `HEAD` checks return the download headers without scanning or materializing the database. Two formats are deliberately separate:

- `archive` is the lossless normalized record stream. Request and response payloads remain structured JSON values instead of Base64 fields.
- `sft` emits one deduplicated conversational sample per durable session in the broadly compatible `{"messages": [...], "tools": [...]}` shape used by OpenAI SFT and Hugging Face TRL. Function calls and tool results are preserved; inline media is replaced by a media type plus SHA-256 reference rather than copied into the training file. Preference/DPO data is not fabricated because the archive has no chosen/rejected labels.

Format references: [OpenAI supervised fine-tuning](https://developers.openai.com/api/docs/guides/supervised-fine-tuning) and [Hugging Face TRL dataset formats](https://huggingface.co/docs/trl/dataset_formats).

Neither CPA's plugin ABI nor the browser buffers an export. The embedded HTML/CSS/JavaScript source follows the host panel language and includes complete Simplified Chinese and English translations. The current CPA resource ABI exposes only one static host-menu label, so the registered sidebar label is Chinese (`会话归档`) while the embedded page itself switches languages dynamically.

Codex Desktop grouping uses the durable thread/session identity. Transient `execution_session_id` values remain searchable diagnostic facets but no longer split retries, remote executions, or compaction attempts into separate visible sessions. At startup, the collector repairs older projections transactionally; archived CAS payloads are not rewritten.

Facets are intentionally client-agnostic. The collector indexes metadata when present for:

- project and workspace name/path;
- Git remote or repository;
- client tool and originator;
- session, conversation, thread, turn and window identifiers plus user/system thread source;
- requested and resolved model;
- CPA key identifier, source format, request kind, stream mode, status and outcome;
- safe project/session/workspace headers and generic request metadata.

Multiple selected facets are combined with AND semantics. Values inside the same metadata dimension are normalized and deduplicated. Authorization, cookies and arbitrary raw headers are never indexed. Existing records remain readable after an upgrade; rich facets are populated for newly archived requests because older records may not contain the original transport metadata.

The same data is available through `GET /v1/facets` and arbitrary facet query parameters on `GET /v1/sessions`, for example:

~~~text
/v1/sessions?project.name=my-repo&client=codex&model.requested=gpt-5.6-sol
~~~

The collector migrates the v0.1 inline-gzip schema online. Old payload columns are nulled after their CAS manifests are committed. SQLite can reuse freed pages without an immediate blocking VACUUM.

## Interaction contract and E2E test tree

The following tree is the product contract for every expected user-visible
interaction. Similar facet dimensions are grouped because the same selector
behavior applies to every facet value.

~~~text
Session archive / 会话归档
├── Host integration and locale
│   ├── Open from the CPA-Manager-Plus "会话归档" plugin menu
│   ├── Follow the host zh-CN / en language without a reload
│   ├── Render timestamps in the browser's locale and timezone
│   └── Resolve a caller-key hash to its CPA alias when an alias exists
├── Overview
│   ├── Show actual archive storage used, session count, and record count
│   ├── Select archive or SFT format and export the complete database
│   └── Expose a reusable server-streamed .jsonl download link
├── Collapsible faceted-search sidebar
│   ├── Collapse or expand the entire sidebar
│   ├── Search facet dimension names
│   ├── Collapse or expand a facet group
│   ├── Select one value in any available dimension
│   │   ├── project / workspace / path / Git repository
│   │   ├── client / originator / SDK / runtime / operating system
│   │   ├── session / conversation / thread / turn / window
│   │   ├── requested/resolved model / provider / request kind / stream
│   │   ├── tool / input / content / message role
│   │   └── caller key / upstream credential / outcome / HTTP status
│   ├── Review active filters and their count
│   ├── Set the result limit and apply the selected facets
│   └── Reset every facet and restore the unfiltered list
├── Session list
│   ├── Show a useful title/first-user-command preview, never a UUID as title
│   ├── Show project, source badges, caller-key alias, model, requests, last use
│   └── Click a row to open its routed Session page
├── Session page
│   ├── Return to the session list
│   ├── Switch newest-first / oldest-first
│   ├── Select archive or SFT and download the complete Session
│   ├── Show one card per durable user turn
│   │   ├── Preview the user command and final/latest natural-language reply
│   │   ├── Keep retries, compactions, prewarms, and tool continuations together
│   │   └── Click the card to open its routed Turn page
│   └── Navigate first / previous / page number / next / last
├── Turn page
│   ├── Return to the parent Session
│   ├── Show caller-key alias, model, and localized activity time
│   ├── Preview long user/final text and expand each to its complete content
│   ├── Expand an intermediate process step
│   │   ├── Read intermediate natural-language assistant messages
│   │   ├── See compaction as an inline marker
│   │   ├── Open a summarized tool call in its routed detail page
│   │   └── Open the request-level diagnostic page
│   └── Paginate large internal-request sequences
├── Request page
│   ├── Return to its Turn (or directly to Session for legacy records)
│   ├── Show history/tool-definition counts without materializing their bodies
│   ├── Open complete system instructions on demand
│   ├── Expand long readable user/assistant text
│   ├── Open each tool call/result
│   ├── Expand normalized metadata (not an undifferentiated JSON wall)
│   └── Open original request, translated request, or response diagnostics
├── Tool-call page
│   ├── Return to the parent Request
│   ├── Show complete arguments and complete result
│   ├── Look across nearby records when a result arrived in a continuation
│   └── Lazily expand exceptionally large bodies
├── Raw-diagnostic/system-instruction page
│   ├── Return to the parent Request
│   ├── Pretty-print JSON where possible
│   ├── Copy the complete body
│   └── Lazily expand content beyond the safe initial render limit
└── Export and performance invariants
    ├── Never buffer a Session or full-database export in browser memory
    ├── Preserve messages, tools, calls/results, and CAS media references
    ├── Keep list APIs on compact projections; rehydrate full text only in detail
    ├── Keep original request/response and tool bodies lazy
    ├── Use stable deep links and browser back/forward navigation at every level
    └── Never duplicate secrets, media bytes, or long user text just for the UI
~~~

The Playwright suite in `e2e/` serves the real embedded management page against
a mocked management API. Its fixtures deliberately include a long ambient
suggestion command, a 17,730-character system instruction, a long assistant
answer, a long tool result, rich facets, a key alias, and a streamed JSONL
ticket. The tests assert the important boundary: previews may be truncated for
performance, but every detail route must provide an explicit path to the
complete content.

Run the browser contract locally:

~~~bash
pnpm install
pnpm exec playwright install chromium
pnpm test:e2e
~~~

GitHub Actions runs the same suite with a mocked backend before building the
plugin, collector, or container image. No production credentials or archived
content are used by the tests.

## Storage backend

SQLite WAL on a persistent volume is intentional for the authoritative archive:
there is one asynchronous writer, while payload bodies are large immutable CAS
objects. Putting those bodies in an HA PostgreSQL cluster would replicate them
through WAL, replicas and backups without improving this write path. Small
`session_summaries` and `session_facets` projections keep the management UI fast
without scanning payload pages.

For a much larger multi-writer installation, PostgreSQL is a reasonable home for
searchable metadata and CAS hashes. Large binary/text blobs should still live in
content-addressed object or volume storage rather than ordinary replicated rows.
Maintenance projections and historical normalization are resumable and retry
transient SQLite lock contention in the background. SQLite free pages are reused automatically; a large database file after a
migration does not by itself mean that the same amount of live data remains.

## Privacy

Authorization headers and raw API keys are never archived. Request/response bodies can still contain user-provided secrets or personal data; protect the database and export endpoints accordingly. No Ingress is included.

## License

Apache-2.0.
