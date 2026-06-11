# @zerodb/queue — Cody Rules

## Overview
BullMQ-compatible job queue using ZeroDB instead of Redis. Zero runtime dependencies.

## Quick Commands
```bash
node --test tests/basic.test.js    # Run 40 tests
node scripts/build.js              # Build ESM + CJS + types
npm publish --access public        # Publish to npm
```

## Architecture
Single-file source at `src/index.js` with 6 exported classes:
- **Queue** — add/get/remove jobs via ZeroDB tables
- **Worker** — poll-based job processor with concurrency
- **Job** — job state container
- **QueueScheduler** — delayed job promotion + stall detection
- **FlowProducer** — parent/child job dependencies
- **ZeroDBClient** — HTTP client for ZeroDB MCP execute endpoint

## Rules
- Zero runtime deps — native fetch only
- All tests must pass before publishing
- Keep BullMQ API compatibility
