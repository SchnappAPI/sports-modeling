# CLAUDE.md

Context for Claude Code sessions in this repo. Read this first every session. Source of truth for process is `/docs/`.

## Project identity

Schnapp (schnapp.bet) is a personal sports analytics platform for NBA, MLB, and NFL prop betting research. Positioned as a consumer sports research platform, not a gambling tool. Moving toward paid subscription.

Stack: Next.js 15 on Azure Static Web Apps, Azure SQL Serverless, Python ETL via GitHub Actions, Flask service on an Azure VM, FastMCP server, Cloudflare tunnels for live subdomains.

Repo layout is documented in `/docs/README.md` (the router). Component READMEs sit next to code in `/etl/<sport>/`, `/web/<sport>/`, `/database/<sport>/`, and `/infrastructure/`.

## Session protocol

Every session:

1. Read `/docs/README.md` for the router and tag taxonomy.
2. Read `/docs/SESSION_PROTOCOL.md` and `/docs/skills/session-protocol.md`.
3. Read the last ten entries of `/docs/CHANGELOG.md`, filtered by tags relevant to the task.
4. Read the component README matching the task: `/etl/<sport>/README.md`, `/web/<sport>/README.md`, or `/database/<sport>/README.md`. The INVARIANTS section is authoritative.

If the stated task contradicts what `/docs/` says, `/docs/` wins. Flag the mismatch before proceeding.

## End-of-session requirements

Before ending a session that made changes:

1. Append one entry to `/docs/CHANGELOG.md` tagged `[sport][component]`. Newest entry goes at the top.
2. For any invariant added or changed, edit the INVARIANTS section of the relevant component README. Section edits only, never full rewrites.
3. For any non-obvious decision, append an ADR to `/docs/DECISIONS.md` using a date-based identifier: `ADR-YYYYMMDD-N` where `N` starts at 1. Grep the file for `^## ADR-YYYYMMDD-` with today's date to find the next unused counter. ADRs are append-only; ADRs 0001-0019 remain on the legacy sequential scheme.
4. For any roadmap shift, update `/docs/ROADMAP.md`.
5. For any schema, infrastructure, or connection change, update the matching README or runbook.

Never commit without an accompanying CHANGELOG entry.

## Style and tone

Match response length to complexity. Simple questions get short answers. Do not restate the question. Reserve caveats for genuine uncertainty.

Offer follow-up suggestions only when they yield a clear net benefit. Process improvements, better approaches, or relevant next steps the user might not have considered are welcome. Generic "let me know if you need anything else" padding is not.

Write code that is optimized for performance, readable, and efficient. Do not add abstraction, helper functions, or structural complexity unless the problem requires it. Balance simplicity against clarity: the simplest correct code that is also easy to follow is the target. Prefer set-based SQL over row-by-row, prefer native platform features over custom implementations.

When a simpler approach trades meaningful performance for readability, flag the tradeoff explicitly rather than deciding unilaterally.

## Code structure principle

Favor condensed, efficient implementations that group related operations. Combine steps where doing so keeps logic together and reduces intermediate state, as long as the result stays readable. Applies across SQL, TypeScript, Python, and Power Query M. For Power Query specifically, this means the Flat-Map-Type pattern: a three-stage pipeline of Fetch, Project, Type, with all field selection, renaming, drilling, and nullable handling inline inside a single `Table.FromRecords(List.Transform(...))` expression, types applied once at the end. camelCase columns, PascalCase steps.

## Web workflow

1. Edit files locally
2. Run `npm run build` (or `npm run dev` for interactive testing) to catch type errors and compile failures
3. If build fails, read the error, fix at the source. Do not commit around a build failure.
4. `git add`, `git commit`, `git push`
5. Azure SWA auto-deploys on push to `main` (roughly 90 seconds)
6. Verify the deploy via the GitHub Actions UI or by checking the live site

## ETL workflow

Python does not run on the user's corporate Windows devices because of ThreatLocker. All automated Python runs in GitHub Actions. Do not suggest running Python locally on Windows. If Python execution is needed for diagnostics, write a script, commit it, trigger the relevant workflow (`db_inventory.yml`, `mlb-pbp-etl.yml`, etc.), and read output through workflow logs.

The standard loop:

1. Edit the Python file locally
2. Syntax check: `python -c "import ast; ast.parse(open('path/to/file.py').read())"` (runs on Mac, not Windows)
3. Commit and push
4. Trigger the workflow via `gh workflow run` or the GitHub Actions MCP
5. Monitor with `gh run watch` or `github:actions_list`

## How to find context

Before asking the user where something is, search the repo. `grep -r "term" .` is free and fast. Use it. The user should only be asked for context that genuinely cannot be derived from the repo, the `/docs/` hierarchy, or the git history.

`git log -n 5 -- <file>` before editing any file you did not create in the current session. Recent commits may reveal intentional decisions that should not be reverted.

## Where to run which workload

Two Claude surfaces touch this repo: claude.ai chat and Claude Code. They have different speed profiles because they have different write primitives.

**Claude Code (local filesystem):** fast. Edits use `str_replace` or `edit_block` on the local file, which patches only the changed region. `git commit` and `git push` are single local operations. Multi-file commits are free. `npm run build` and `tsc --noEmit` run inline before pushing. This is the right surface for any session that edits multiple files or ends with a CHANGELOG append.

**claude.ai chat (GitHub MCP):** slower for file edits. The only write primitive is the GitHub Contents API, which requires a full-file upload per change. A one-line CHANGELOG append re-uploads the entire file (currently ~43 KB, growing). Acceptable for single-file one-off work, design discussion, research, and planning. Not the default editing path.

**Dispatch pattern:** when a claude.ai chat session produces work that should execute on Claude Code, end the turn with a clearly-labeled "Claude Code prompt" block the user can paste into a Claude Code session. The prompt should be self-contained: files to touch, exact `str_replace` patches or new content, the CHANGELOG entry text, and the commit message. Do not commit from claude.ai in this flow. The prompt is the handoff.

Rule of thumb: if the change is one file and under a few hundred bytes, committing from claude.ai is fine. If it involves multiple files, a CHANGELOG entry plus code, or any iteration-prone edit, produce a Claude Code prompt instead.

## Host-specific context

This repo works from multiple machines. Either the Windows laptop or the Mac is a valid Claude Code host. Choose whichever you are sitting at.

**Windows laptop (1stLake user):** full stack. Power BI MCP works here (connects to Power BI Desktop's local Analysis Services). ThreatLocker blocks local Python and blocks direct execution of the `claude.ps1` wrapper in the npm global folder. Start Claude Code with the Node wrapper invocation instead:

```
node "C:\Users\1stLake\AppData\Roaming\npm\node_modules\@anthropic-ai\claude-code\cli-wrapper.cjs"
```

Worth aliasing in PowerShell profile. Do not use the bare `claude` command on this host; it hits ThreatLocker.

**MacBook Pro (planned, not yet set up):** will replicate the Node/git/npm stack so `claude` runs directly without ThreatLocker in the way. No Power BI MCP (Power BI Desktop is Windows-only). When set up, update this section and `/docs/CONNECTIONS.md` with specific paths.

**Azure VM (schnapp-runner-2):** Ubuntu 24.04, Python venv at `/home/schnapp-admin/venv/`. Not a Claude Code host. Used for self-hosted GitHub Actions runs and the Flask live service.

If a task mentions Power BI, verify the session is on the Windows laptop before proceeding. If not, tell the user and stop.

## MCPs expected in this environment

- `schnapp-ops`: remote HTTP, bearer token auth. Tools include `flask_status`, `flask_restart`, `live_scoreboard`, `live_boxscore`, `workflow_status`, `workflow_trigger`, `shell_exec`, `read_file`. The `shell_exec` tool executes on the Azure VM, not the local machine.
- `github`: remote HTTP, GitHub PAT auth. Used for workflow triggering, log reading, and PR operations that local `git` does not cover. Not the preferred path for file edits when Claude Code is available.
- `powerbi-modeling`: Windows-only, local stdio. Only expected on the Windows laptop. For any Power BI task, call `connection_operations` with `ListLocalInstances`, then `Connect` using the connection string for the instance whose `parentWindowTitle` matches the active model.

If an MCP is missing when a task needs it, state which host is required and let the user decide whether to switch machines.

## Live service and live data

Flask service runs on the Azure VM as `schnapp-flask` systemd unit. Reachable at `https://live.schnapp.bet/` via Cloudflare tunnel. Provides NBA CDN scoreboard and boxscore data.

MCP server runs on the same VM as `schnapp-mcp`. Reachable at `https://mcp.schnapp.bet/mcp`. This is the Schnapp Ops MCP.

For live NBA games, use `live_scoreboard` and `live_boxscore`. Playoff game IDs use prefix `004`.

## Data sources and credentials

Azure SQL serverless. First connection after auto-pause takes 20 to 60 seconds. Connection details in `/docs/CONNECTIONS.md`.

Odds API key and other credentials in `/docs/CONNECTIONS.md`. Do not hardcode credentials. Do not commit credentials to the repo.

## What never to do

- Rewrite a component README in full. Section edits only.
- Commit without a CHANGELOG entry.
- Duplicate CHANGELOG content inside a README's "Recent Changes" section.
- Touch any `_legacy_*` or `_archive/` directory.
- Push files that fail `npm run build` or `tsc --noEmit`.
- Suggest running Python locally on the corporate Windows machines.
- Run destructive commands (`rm -rf`, `git reset --hard`, `DROP TABLE`) without explicit user confirmation in the same turn.
- Override an invariant listed in a component README without adding a superseding ADR to `/docs/DECISIONS.md`.
- Default to claude.ai GitHub MCP for multi-file or CHANGELOG-bearing edits. Produce a Claude Code prompt instead.

## What to do when uncertain

Ask one focused question. Do not guess when the answer affects data integrity, schema, or user-facing behavior. For minor ambiguity, state the interpretation inline and proceed.
