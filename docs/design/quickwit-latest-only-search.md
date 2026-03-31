# Quickwit Latest-Only Search

## Overview

This document describes a new full-text search system built on Quickwit, deployed on a machine outside the Cloudflare runtime stack.

The design assumes:

- Cloudflare remains the source of truth for ingest, metadata, and blob storage.
- Search indexes only the latest active version of each source.
- Search indexes one full document per node.
- Search does not chunk documents unless measured corpus behavior forces a later change.
- The ingest side pushes a notification to the Quickwit machine when an export blob is ready, and includes a signed URL for download.

This is a fresh design. It does not attempt to preserve or extend the current web search implementation.

---

## Goals

- Provide fast full-text search over the latest active corpus.
- Index all nodes, not just sections.
- Keep Quickwit operationally separate from Cloudflare workers and services.
- Avoid direct live reads from D1 by the Quickwit machine.
- Make indexing deterministic, replayable, and easy to recover.
- Keep the first version simple enough to ship without Kafka, streaming CDC, or semantic search.

## Non-Goals

- Historical search across multiple source versions in the initial rollout.
- Near-real-time per-row indexing.
- Partial in-place mutation of the live Quickwit corpus.
- Document chunking in v1.
- Coupling search indexing to request-time app behavior.

---

## Core Decision

The indexing boundary is an export artifact, not the database.

Cloudflare will produce canonical search export blobs for the latest source version and notify the Quickwit machine when a new blob is ready. The Quickwit machine will download that blob, rebuild a new latest-only index generation, validate it, and then swap search traffic to the new generation.

This is preferable to direct D1 access from the Quickwit machine because it gives us:

- a stable contract between systems
- deterministic rebuild inputs
- easy replay and rollback
- lower operational coupling
- cleaner auth and network boundaries

---

## Data Model Context

The current corpus is split across:

- `nodes` metadata in D1
- `source_versions` and `sources` metadata in D1
- body content addressed by `blob_hash`
- blob location metadata in `blobs`
- actual blob payloads in R2 packfiles

Search export must flatten those into a canonical node document.

Each exported search document represents a single node in the latest active source version and includes:

- structural metadata
- source metadata
- normalized title text
- normalized body text if the node has blob-backed content
- a single combined `search_text` field used for full-text indexing

---

## Latest-Only Indexing Model

The search product is latest-only.

That means:

- for each `source_id`, exactly one `source_version_id` is active in search
- older source versions are ignored by the search corpus
- when a new source version becomes active, the old one disappears from the next index generation

This avoids mixing superseded law into user-facing results and avoids ranking problems caused by duplicate versions.

### Implication for rebuild strategy

Quickwit is treated as an immutable build target. We do not try to surgically patch the live corpus in place. Instead:

1. Cloudflare exports a canonical blob for the new latest source version.
2. The Quickwit machine updates its local manifest of current latest exports by source.
3. The Quickwit machine builds a new full latest-only generation from the current set of latest exports.
4. The search API switches to the new generation after validation.

This is a blue/green model for the search corpus.

---

## Why No Chunking in V1

The first version indexes one full document per node.

Reasons:

- it matches the logical corpus model
- it keeps export and ingest simple
- it makes result handling trivial
- it avoids introducing collapse logic and chunk-level ranking artifacts

Quickwit and Tantivy do not force document chunking. Large text fields are valid. Chunking remains an optional later optimization if one of these shows up in production:

- poor ranking for very long bodies
- unacceptable index size
- unacceptable indexing latency
- poor snippet quality for long nodes

Until that happens, one document per node is the right default.

---

## High-Level Architecture

```text
Cloudflare ingest/job completion
  -> search export workflow
      -> writes export blob(s) to R2
      -> generates signed download URL
      -> POST /indexing-jobs to Quickwit machine

Quickwit machine
  -> indexing coordinator receives webhook
  -> downloads export blob
  -> updates local latest-export registry
  -> rebuilds new Quickwit generation from all current latest exports
  -> validates generation
  -> flips search API to new generation

Search clients
  -> app/server calls search API on Quickwit machine
  -> search API queries active Quickwit generation
```

### Components

Cloudflare side:

- export workflow
- signed export URL generator
- webhook sender

Quickwit machine:

- Quickwit
- indexing coordinator
- search API
- local registry of active latest exports
- local generation state

---

## Cloudflare-Side Export Flow

### Trigger

The export flow runs when a source version has completed ingest and is promoted to latest for its source.

The trigger point should be after:

- all node metadata writes are durable
- all referenced blobs are flushed to R2
- the source version is considered valid for serving

### Export scope

Each export blob covers one `source_version_id`.

That keeps the Cloudflare-side work bounded and lets the Quickwit machine maintain a simple latest-export registry keyed by `source_id`.

### Export output

For each source version, Cloudflare writes:

- one canonical NDJSON blob containing every searchable node for that source version
- one small manifest JSON file describing the blob

Suggested R2 keys:

```text
search-exports/<source_id>/<source_version_id>/nodes.ndjson
search-exports/<source_id>/<source_version_id>/manifest.json
```

### Export document semantics

One NDJSON line equals one searchable node.

Nodes with no body content are still exported. Their `body_text` is empty and their `search_text` is derived from structural fields.

Nodes with body content include flattened text derived from the stored blob payload.

### Export normalization

The exporter should:

- collapse repeated whitespace
- flatten block content into readable plain text
- strip nulls into empty strings where appropriate
- avoid embedding raw JSON payloads in the indexed document
- produce deterministic field ordering

The text flattening logic should be shared with or derived from the existing body extraction semantics already used elsewhere in the repo so search and other readers do not diverge.

---

## Canonical Search Document Shape

```json
{
  "doc_id": "cgs-2025:root/title-1/chapter-1/section-1-1",
  "source_id": "cgs",
  "source_name": "Connecticut General Statutes",
  "jurisdiction": "state",
  "region": "ct",
  "doc_type": "statute",
  "source_version_id": "cgs-2025",
  "version_date": "2025-01-01",
  "node_id": "root/title-1/chapter-1/section-1-1",
  "parent_id": "root/title-1/chapter-1",
  "level_name": "section",
  "level_index": 3,
  "sort_order": 17,
  "path": "/sections/1/1",
  "readable_id": "1-1",
  "heading_citation": "Sec. 1-1",
  "name": "Definitions",
  "source_url": "https://...",
  "accessed_at": "2026-03-30T10:15:00.000Z",
  "blob_hash": "abc123",
  "has_body": true,
  "breadcrumb": "Title 1 > Chapter 1 > Sec. 1-1 Definitions",
  "title_text": "Sec. 1-1 Definitions",
  "body_text": "As used in this title ...",
  "search_text": "Title 1 Chapter 1 Sec. 1-1 Definitions As used in this title ..."
}
```

### Required fields

- `doc_id`
- `source_id`
- `source_version_id`
- `version_date`
- `node_id`
- `level_name`
- `level_index`
- `sort_order`
- `has_body`
- `title_text`
- `body_text`
- `search_text`

### Useful filter fields

- `source_id`
- `jurisdiction`
- `region`
- `doc_type`
- `level_name`

### Construction notes

- `doc_id` should be `${source_version_id}:${node_id}`
- `search_text` should be the primary full-text field
- `breadcrumb` should be built from ancestor headings if available
- `title_text` should be derived from citation plus name, not raw path

---

## Manifest Format

Each source-version export should include a manifest like:

```json
{
  "schema_version": 1,
  "export_id": "2026-03-30T14:12:55Z-cgs-cgs-2025",
  "source_id": "cgs",
  "source_version_id": "cgs-2025",
  "version_date": "2025-01-01",
  "doc_count": 84231,
  "blob_path": "search-exports/cgs/cgs-2025/nodes.ndjson.gz",
  "sha256": "4f2f...",
  "generated_at": "2026-03-30T14:12:55.000Z"
}
```

The Quickwit machine uses this manifest to:

- validate downloads
- track the latest known export per source
- decide whether a rebuild is required

---

## Signed Download Contract

When an export blob is ready, Cloudflare sends a webhook to the Quickwit machine with:

- source metadata
- manifest metadata
- a short-lived signed URL for the manifest
- a short-lived signed URL for the blob itself

Suggested request:

```json
{
  "event": "source_export_ready",
  "schema_version": 1,
  "source_id": "cgs",
  "source_version_id": "cgs-2025",
  "version_date": "2025-01-01",
  "manifest_url": "https://...",
  "blob_url": "https://...",
  "generated_at": "2026-03-30T14:12:55.000Z",
  "signature": "..."
}
```

### Auth

Use an HMAC signature over the body with a shared secret.

The Quickwit machine should:

- reject unsigned requests
- reject requests with invalid signatures
- reject stale timestamps
- treat the webhook as an enqueue signal, not as proof that the blob is valid

### Signed URL shape

Two acceptable implementations:

1. direct signed R2 download URL
2. signed Worker URL that proxies the download

The design does not depend on which variant is used. The only requirement is that the Quickwit machine can download the manifest and blob without needing direct Cloudflare credentials.

---

## Quickwit-Side Components

### 1. Indexing coordinator

Responsibilities:

- receive webhooks
- download manifests and export blobs
- validate checksums
- maintain a local registry of latest exports by `source_id`
- serialize rebuild work
- trigger new Quickwit generations
- record generation status and failure reason

This should be a small dedicated service, not Quickwit itself. Quickwit can ingest documents over API calls, but it does not replace the need for a service that can accept the webhook, download the presigned blob, validate it, and then push documents into Quickwit.

### 2. Latest-export registry

The coordinator maintains local state:

```json
{
  "cgs": {
    "source_version_id": "cgs-2025",
    "manifest_path": "/var/lib/fastlaw-search/manifests/cgs-2025.json",
    "blob_path": "/var/lib/fastlaw-search/blobs/cgs-2025.ndjson.gz",
    "updated_at": "2026-03-30T14:13:21.000Z"
  }
}
```

This registry is the input to every full rebuild. It defines the active latest corpus.

### 3. Quickwit

Quickwit stores the active searchable generation plus one previous generation for rollback.

### 4. Search API

The app should query a small API on the Quickwit machine rather than Quickwit directly.

Responsibilities:

- query the currently active generation
- own any relevance tuning
- hide Quickwit query details from the app
- expose stable result payloads
- support future reranking without app changes

---

## Rebuild Flow

### Step 1: Receive export-ready webhook

The coordinator validates the request and enqueues a rebuild job.

### Step 2: Download artifacts

The coordinator downloads:

- manifest
- NDJSON blob

It verifies:

- schema version
- source metadata
- checksum
- document count after ingest preparation

### Step 3: Update latest-export registry

The new export replaces the previous latest export for that `source_id`.

This step happens before the rebuild starts so the current registry always represents the desired next corpus.

### Step 4: Build next generation

The coordinator creates a new generation id, for example:

```text
latest-2026-03-30T14-15-02Z
```

It then constructs the full latest-only corpus from all currently registered source blobs and ingests that into a new Quickwit generation.

### Step 5: Validate generation

Validation should include:

- total document count matches the sum of manifests
- basic query smoke tests pass
- the generation is queryable

### Step 6: Activate generation

The search API flips its active generation pointer only after validation succeeds.

### Step 7: Retain rollback target

Keep:

- current generation
- previous generation

Delete older generations on a delayed cleanup schedule.

---

## Why Full Rebuilds Instead of Incremental Updates

The corpus is latest-only and source-versioned.

That means a new source version logically replaces an old one. A full rebuild:

- gives exact replacement semantics
- avoids stale documents lingering in the corpus
- avoids delete-heavy live mutation logic
- keeps failure handling simple
- keeps relevance deterministic

This is the correct bias until the corpus becomes too large for acceptable rebuild latency.

If rebuild time later becomes a real problem, the next step should be per-source indexes plus federated query, not an immediate jump to row-level mutation.

---

## Quickwit Index Design

Use a single logical index schema for the latest-only corpus.

### Indexed full-text fields

- `search_text`
- `title_text`
- `body_text`
- `breadcrumb`

### Stored raw/filter fields

- `doc_id`
- `source_id`
- `source_name`
- `jurisdiction`
- `region`
- `doc_type`
- `source_version_id`
- `version_date`
- `node_id`
- `parent_id`
- `level_name`
- `level_index`
- `sort_order`
- `path`
- `readable_id`
- `heading_citation`
- `name`
- `source_url`
- `accessed_at`
- `blob_hash`
- `has_body`

### Search defaults

Default search should target:

- `search_text` first
- `title_text` with a higher boost
- `body_text` as recall support

Search result ranking should prefer:

- exact citation matches
- exact title matches
- heading matches over body-only matches

That ranking logic can live in the search API layer if the initial Quickwit query language is not enough by itself.

---

## Exporter Implementation Notes

### Building `breadcrumb`

The exporter should compute a human-readable ancestor path from the node hierarchy. This is useful both for ranking and display.

### Building `title_text`

Use:

- `heading_citation`
- `name`

Do not use raw `path` as the primary title signal.

### Building `body_text`

If `blob_hash` is null:

- `body_text = ""`

If `blob_hash` is present:

- resolve the blob from `blobs`
- read the R2 content
- flatten the node content to plain text

### Building `search_text`

Recommended composition:

```text
<breadcrumb>
<title_text>
<body_text>
```

Whitespace-normalized into a single string.

---

## Failure Handling

### Export succeeds, webhook fails

Cloudflare should retry webhook delivery with exponential backoff.

### Webhook succeeds, download fails

The Quickwit coordinator should retry artifact download until the signed URL expires. If the URL expires, the job should fail explicitly and require Cloudflare to re-issue a new webhook or a requeue operation.

### Rebuild fails

The active generation remains unchanged.

The failed generation is marked failed and kept long enough for inspection.

### Validation fails

The active generation remains unchanged.

### Duplicate export notifications

The coordinator should be idempotent on `(source_id, source_version_id, sha256)`.

If it has already processed the exact export, it should no-op.

---

## Security Model

The Quickwit machine should not need:

- D1 credentials
- R2 credentials
- general Cloudflare API credentials

It only needs:

- access to webhook endpoint
- ability to download signed export URLs
- local credentials for Quickwit and the local search services

### Network exposure

Preferred:

- Quickwit bound to localhost or private network only
- public exposure only for the small search API
- indexing coordinator endpoint protected by HMAC and optional IP allowlisting

---

## Operational Notes

### Minimum deployment shape

- one Quickwit machine
- local SSD or NVMe
- one indexing coordinator service
- one search API service
- one retained rollback generation

### Backups

Persist:

- downloaded manifests
- downloaded export blobs
- latest-export registry
- generation metadata

Those artifacts are enough to replay index builds without asking Cloudflare to regenerate everything immediately.

### Concurrency

Only one rebuild should run at a time.

If multiple exports arrive in quick succession:

- update the latest-export registry for each
- coalesce them into the next rebuild

This avoids wasted intermediate builds.

---

## Query API Shape

The app-facing search API can start simple:

### Request

```json
{
  "query": "unfair trade practices",
  "top_k": 20,
  "filters": {
    "source_id": ["cgs"],
    "level_name": ["section", "chapter"]
  }
}
```

### Response

```json
{
  "query": "unfair trade practices",
  "generation": "latest-2026-03-30T14-15-02Z",
  "results": [
    {
      "score": 12.34,
      "source_id": "cgs",
      "source_version_id": "cgs-2025",
      "node_id": "root/title-42/chapter-735a/section-42-110b",
      "level_name": "section",
      "heading_citation": "Sec. 42-110b",
      "name": "Unfair trade practices prohibited",
      "path": "/sections/42/110b",
      "breadcrumb": "Title 42 > Chapter 735a > Sec. 42-110b Unfair trade practices prohibited",
      "snippet": "No person shall engage in unfair methods of competition ..."
    }
  ]
}
```

The API is intentionally generic and independent of route assumptions in the current app.

---

## Rollout Plan

### Phase 1

- implement canonical search export for one source version
- implement webhook delivery to the Quickwit machine
- stand up Quickwit, indexing coordinator, and search API
- build and query one latest-only generation

### Phase 2

- support all active sources
- add generation validation and rollback tooling
- wire app traffic to the new search API

### Phase 3

- tune ranking
- add faceting and filters
- measure whether chunking is actually needed

---

## Deferred Options

These are intentionally out of scope for v1:

- chunked search documents
- semantic reranking
- historical search
- incremental delete/update indexing
- direct Quickwit querying from the app
- multi-machine Quickwit cluster
- Kafka-based ingest

---

## Final Recommendation

Build Quickwit as a separate latest-only search appliance fed by canonical source-version export blobs from Cloudflare.

The key implementation choices are:

- one full document per node
- source-version export blobs in R2
- signed URL webhook from Cloudflare to the Quickwit machine
- local latest-export registry on the Quickwit machine
- full blue/green rebuilds of the latest-only corpus
- small stable search API in front of Quickwit

This gives the cleanest v1 architecture and keeps the boundary between Cloudflare and the search box explicit, deterministic, and easy to operate.
