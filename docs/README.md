# Documentation Router

This is the entry point for all project documentation. It links to everything; it does not duplicate content.

## Read first every session
- `skills/session-protocol.md` for start-of-session checklist, mid-session signals, end-of-session checklist, and mechanical guardrails. Read this before taking any action.
- `skills/live-session-cache.md` for the chat-logging skill: when it auto-activates, branch and file conventions, integration with the rest of this protocol.
- `SESSION_PROTOCOL.md` for the canonical protocol definition.
- `CHANGELOG.md` for the last ~10 entries, filtered by tag relevant to the task.

## Cross-cutting reference
- `PRODUCT_BLUEPRINT.md` for the sport-agnostic product concept.
- `DECISIONS.md` for the append-only ADR log capturing the why behind non-obvious choices.
- `CONNECTIONS.md` for endpoints, credentials, MCP tokens, and secret references.
- `GLOSSARY.md` for domain vocabulary, with per-sport sections.
- `ROADMAP.md` for what is planned, deliberately brief.

## Component docs (live next to code)
ETL: `/etl/<sport>/README.md` and `/etl/_shared/README.md`. Code files live flat in `/etl/`; the subfolders hold docs only.
Database: `/database/<sport>/README.md`.
Web: `/web/<sport>/README.md` and `/web/_shared/README.md`.
Infrastructure: `/infrastructure/README.md` plus runbooks in `/infrastructure/runbooks/`.

## How sport status varies
NBA is the only sport currently live. MLB is in development: 3 of the 6 ADR-0003 pages are coded (Game, VS, EV) — works in progress, not considered live yet — and 2 of the 5 missing ADR-0004 derived entities are materialized (`mlb.player_at_bats`, `mlb.career_batter_vs_pitcher`). NFL has an ETL pipeline that runs on schedule (first run 2026-04-21), but no active development since and no downstream product consumer. Each component README starts with a STATUS line that states current maturity in one sentence.

## Documentation structure
This hierarchy replaced a legacy `PROJECT_REFERENCE.md` at the repo root on 2026-04-20. Archived at `/docs/_archive/`. See `DECISIONS.md` ADR-0001 for the structure decision and ADR-0016 for the archive decision.

## Tag taxonomy for CHANGELOG and DECISIONS
Every entry uses bracketed scope tags. The first tag is sport scope, the second is component scope.

Sport scopes: `[nba]`, `[mlb]`, `[nfl]`, `[shared]`.
Component scopes: `[etl]`, `[web]`, `[database]`, `[infra]`, `[grading]`, `[odds]`, `[lineup]`, `[live]`, `[mcp]`, `[docs]`.

Combine as needed. Examples: `[nba][grading]`, `[shared][docs]`, `[mlb][web]`, `[nba][odds]`. Sessions filter by relevant tags rather than reading every entry.
