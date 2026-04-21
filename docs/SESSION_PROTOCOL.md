# Session Protocol

This file defines how every session with Claude starts and ends. Following it makes session-end updates take seconds and prevents reverts of recent changes.

## At the start of every session

1. Read `/docs/README.md` for the router.
2. Read this file.
3. Read the last 20 entries of `/docs/CHANGELOG.md`. Filter mentally to entries with tags relevant to the current task. Ignore unrelated tags.
4. Based on the stated task, read only the relevant component READMEs:
   - Sport-specific task: `/<area>/<sport>/README.md` and any sub-component README in scope.
   - Cross-sport task: `/docs/PRODUCT_BLUEPRINT.md` plus relevant `/<area>/_shared/README.md` files.
   - Design task on a sport without active code: read the sport's README. The STATUS line indicates maturity.

The component README's INVARIANTS section is authoritative. Anything listed there must not change without a deliberate ADR entry that supersedes it.

## At the end of every session

1. Append exactly one entry to `/docs/CHANGELOG.md` with `[sport][component]` tags. Format:

```
YYYY-MM-DD [tag][tag] One-line summary. See /path/README.md.
```

Newest entry goes at the top.

2. If an invariant was added or changed, use `str_replace` on the INVARIANTS section of the relevant component README. Do not rewrite the whole file.

3. If a non-obvious decision was made, append to `/docs/DECISIONS.md` as a new ADR with the next sequential number. ADRs are append-only and never rewritten.

## Things sessions never do

- Rewrite a component README wholesale. Always use `str_replace` on the changed section.
- Duplicate CHANGELOG content inside a README's "Recent Changes" section. The README points to CHANGELOG; it does not repeat it.
- Touch any `_legacy_*` folders. Those hold reference material only.
- Push files to `main` without a corresponding CHANGELOG entry in the same session.

## Transition note

The archived `/docs/_archive/PROJECT_REFERENCE.md` and `/docs/_archive/CHANGELOG.md` contain the historical record. New work follows this protocol. The old protocol described in PROJECT_REFERENCE.md is being phased out. Final cutover removes the legacy files; until then, new content goes in `/docs/` and component READMEs.

If a piece of needed context exists only in the legacy files and has not yet been migrated, that is a signal to migrate it as part of the current session and create a CHANGELOG entry recording the move.

## Why this protocol exists

Three problems it solves:
1. Slow updates between sessions caused by rewriting a monolithic reference doc. Fixed by scoped `str_replace` on small files.
2. Reverts of recent changes when a new session is unaware of them. Fixed by reading CHANGELOG first and respecting INVARIANTS sections.
3. Re-deriving solutions to problems already solved. Fixed by DECISIONS.md preserving the why.
