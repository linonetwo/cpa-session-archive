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

Exports transparently rehydrate the JSON structure, including repeated references to one attachment blob.

## Architecture

    CPA native plugin -> bounded memory queue -> collector -> SQLite WAL + CAS blobs

The request path never waits for disk I/O. A full queue, unavailable collector or storage error does not fail the model request.

## Build

~~~bash
docker build -t cpa-session-archive:0.2.0 .
~~~

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
      queue_size: 2048
      max_body_bytes: 67108864
      timeout: 5s
      store_upstream_request: false
~~~

## Collector

~~~bash
ARCHIVE_DB=/data/archive.sqlite STORE_UPSTREAM_REQUEST=false cpa-session-collector
~~~

Endpoints:

- GET /healthz
- GET /v1/stats
- GET /v1/sessions?limit=100
- GET /v1/sessions/{session-id}
- POST /v1/maintenance/gc

The collector migrates the v0.1 inline-gzip schema online. Old payload columns are nulled after their CAS manifests are committed. SQLite can reuse freed pages without an immediate blocking VACUUM.

## Privacy

Authorization headers and raw API keys are never archived. Request/response bodies can still contain user-provided secrets or personal data; protect the database and export endpoints accordingly. No Ingress is included.

## License

Apache-2.0.
