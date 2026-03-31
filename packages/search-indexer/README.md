# Search Indexer

Small orchestrator service for Quickwit latest-only search ingestion.

## Responsibilities

- accept signed export-ready webhooks from Cloudflare
- download the presigned manifest and NDJSON blob to local disk
- persist ingestion and generation state in local SQLite
- rebuild the active latest-only Quickwit generation
- expose a small `/search` API against the active generation

## Required Environment Variables

- `QUICKWIT_BASE_URL`
- `QUICKWIT_WEBHOOK_SECRET`

## Optional Environment Variables

- `PORT`
- `FASTLAW_SEARCH_STATE_DIR`
- `FASTLAW_SEARCH_DB_PATH`
- `QUICKWIT_INDEX_PREFIX`
- `QUICKWIT_DEFAULT_SEARCH_FIELDS`
- `REBUILD_POLL_MS`
- `RETAINED_GENERATIONS`

## Endpoints

- `GET /healthz`
- `POST /indexing-jobs`
- `GET /indexing-jobs/:id`
- `POST /rebuild`
- `POST /search`
