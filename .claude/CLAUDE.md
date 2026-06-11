# @zerodb/queue — Claude Code Rules

## Overview
BullMQ-compatible job queue using ZeroDB instead of Redis. Zero runtime dependencies (uses native fetch).

## Architecture
- `src/index.js` — All classes (Queue, Worker, Job, QueueScheduler, FlowProducer)
- `tests/basic.test.js` — 40 tests using Node.js built-in test runner
- `scripts/build.js` — Zero-dep build (ESM + CJS + .d.ts)
- `dist/` — Built output (index.js, index.cjs, index.d.ts)

## Development
```bash
node --test tests/basic.test.js    # Run tests
node scripts/build.js              # Build dist
```

## Key Design Decisions
- Zero runtime dependencies — only native fetch
- BullMQ API compatibility (Queue, Worker, Job classes)
- Auto-provisions ZeroDB table on first use
- Worker uses polling (configurable interval)
- Exponential backoff for retries
- Tests mock global fetch — no network calls

## ZeroDB API
All operations go through `POST /v1/public/zerodb/mcp/execute` with:
- `create_table`, `insert_rows`, `query_rows`, `update_rows`, `delete_rows`
- `create_event` for job added notifications
