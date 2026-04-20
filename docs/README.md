# Documentation Router

This is the entry point for all project documentation. It links to everything; it does not duplicate content.

## Read first every session
- `SESSION_PROTOCOL.md` for how every session starts and ends.
- `CHANGELOG.md` for the last 20 entries, filtered by tag relevant to the task.

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
NBA is live. MLB is in design phase with data and visuals cataloged but no code yet. NFL is in planning. Each component README starts with a STATUS line that states current maturity in one sentence. Empty placeholder docs are not used; if something is not designed yet, the README says so.

## Migration in progress
This documentation structure is being built out from a legacy `PROJECT_REFERENCE.md` at the repo root. During the transition both old and new docs coexist. See `DECISIONS.md` ADR-0001 for the structure decision and the migration plan.

## Tag taxonomy for CHANGELOG and DECISIONS
Every entry uses bracketed scope tags. The first tag is sport scope, the second is component scope.

Sport scopes: `[nba]`, `[mlb]`, `[nfl]`, `[shared]`.
Component scopes: `[etl]`, `[web]`, `[database]`, `[infra]`, `[grading]`, `[odds]`, `[lineup]`, `[live]`, `[mcp]`, `[docs]`.

Combine as needed. Examples: `[nba][grading]`, `[shared][docs]`, `[mlb][web]`, `[nba][odds]`. Sessions filter by relevant tags rather than reading every entry.
