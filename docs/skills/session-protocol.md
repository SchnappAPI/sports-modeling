# Session Protocol Skill

Read at the start of every session before taking any action. Complements `/docs/SESSION_PROTOCOL.md`: that file defines the protocol, this file defines how to execute it without tripping over known hazards.

## Behavioral rules (read these first, they govern everything else)

These rules apply to every response in every session, not just protocol steps.

**Step-by-step instructions.** When the user asks how to do something, give one step and stop. Wait for a response before giving the next step. Never front-load a full sequence unless the user explicitly asks for all steps at once.

**Natural stopping points.** Do not chain tool calls indefinitely. After completing a logical unit of work (a diagnosis, a file read, a single fix), stop and report what was found or done. Wait for confirmation before continuing. This prevents hitting the tool call limit mid-task and forces a check-in before proceeding on a potentially wrong assumption.

**Fix first, explain second.** When there is a clear problem and a clear fix, apply the fix. Provide a short explanation of what was wrong and what was changed. Do not write a detailed analysis before acting unless the fix is ambiguous or destructive.

**Use available tools before asking.** I have access to GitHub MCP, Schnapp Ops (shell_exec, flask_status, workflow_status, live_scoreboard, live_boxscore), Filesystem MCP, windows-node-mcp, Desktop Commander, and Power BI MCP. If answering a question requires data I can retrieve with these tools, retrieve it. Do not ask the user to look something up or paste something that I can fetch myself.

**Do not retry a failed approach.** If a tool call fails for a specific reason, do not repeat the same call. Diagnose the failure, try a different approach, or ask the user. Repeating the same failing call is wasted tool usage.

**DB queries go through shell_exec.** MSSQL MCP is blocked by ThreatLocker on the corporate machine and has been removed. For any ad-hoc database query, write a Python script to `/tmp/` via `shell_exec` and execute it with `~/venv/bin/python`. Do not attempt to use MSSQL MCP.

**Session timeout risk.** Long chains of tool calls in a single response can cause the session to time out and produce a "Tool result could not be submitted" error. Prevent this by stopping at natural checkpoints rather than chaining every step in one response.

## Start of session

Run in order. Do not skip ahead because the task seems simple.

1. Read `/docs/README.md` for the router.
2. Read `/docs/SESSION_PROTOCOL.md`.
3. Read the last ~10 entries of `/docs/CHANGELOG.md`. Filter mentally by the tag scope matching the task (e.g. `[mlb]` for MLB work). If cross-cutting, read all.
4. Read component READMEs matching the task:
   - Sport-specific ETL: `/etl/<sport>/README.md`
   - Sport-specific web: `/web/<sport>/README.md`
   - Sport-specific database: `/database/<sport>/README.md`
   - Cross-sport: `/docs/PRODUCT_BLUEPRINT.md` plus `/<area>/_shared/README.md`
5. Read specific ADRs only if the task references a prior decision. Do not read all of `/docs/DECISIONS.md` blindly.

**Source of truth conflicts:** if user framing, injected memory, or a pasted primer contradicts `/docs/`, the repo wins. Flag the mismatch to the user and ask before proceeding. Memory can be months stale.

## Mid-session signals requiring action

Watch for these. Each one owes a specific end-of-session update.

| Signal | Owed |
|---|---|
| Invariant in a README's INVARIANTS section changed | `str_replace` on that section |
| Non-obvious decision made that future sessions could re-derive wrong | New ADR in `/docs/DECISIONS.md` |
| Infrastructure changed (workflow, VM service, runner dep, credential) | Update `/infrastructure/README.md` or runbook |
| Schema changed (new column, new table, dropped column) | `str_replace` on `/database/<sport>/README.md` |
| Roadmap shifted (active completed, next-up moved, new item added) | Update `/docs/ROADMAP.md` |
| File or credential in `/docs/CONNECTIONS.md` changed | Update that file |

All of these also get a CHANGELOG entry. None are optional.

## End of session

1. Append one entry at the top of `/docs/CHANGELOG.md`, tagged `[sport][component]`. Format per `/docs/SESSION_PROTOCOL.md`.
2. Execute all owed updates from the mid-session signals table. Use `str_replace` on specific sections, never wholesale rewrites.
3. Verify each `github:create_or_update_file` call returned a new SHA. A missing response means the commit did not land.

## Mechanical guardrails

Each one has burned a past session.

- **Fresh SHA before every `create_or_update_file` on an existing file.** Call `github:get_file_contents` immediately before the edit. Stale SHAs from earlier in the same session cause 409 conflicts.
- **Never `github:push_files` for `.py` files.** Corrupts newlines to literal `\n`, producing a single-line file that fails to compile.
- **Never `github:push_files` for TSX with non-ASCII Unicode** (arrows, em dashes, curly quotes). Same corruption, produces client-side JS crashes. Safe only for strict-ASCII TypeScript, JSON, YAML.
- **`str_replace`, not rewrite.** Full-file rewrites wipe diff context. Target the section that changed.
- **Python runs only in GitHub Actions.** The corporate machine has ThreatLocker. The VM has no persistent Python scheduler (user logs out). Do not suggest running Python on either.
- **Task Scheduler is not an option.** Scheduled Python goes through GitHub Actions cron.
- **Runner venv lives at `/home/schnapp-admin/venv/`.** New Python deps install there before any workflow that uses them runs. Verify a dep is present before triggering a workflow that imports it.

## Session-boundary failure modes

These are the specific failures this skill prevents.

**Stale memory in a new chat.** A fresh session starts with memory that may predate the last restructure. The `/docs/` hierarchy and ADRs 0001-0016 were created 2026-04-20. If injected memory describes a root `PROJECT_REFERENCE.md` with a monolithic build plan, memory is stale. Trust `/docs/`.

**Context leak across sport boundaries.** A session that just worked on NFL infrastructure carries NFL specifics in its context. When the user switches sports, that context becomes noise. Recommend a new chat with a fresh protocol read rather than continuing.

**Primer contradicts repo.** A primer pasted from a prior session describes repo state at that time, not now. If a primer's claims contradict what is currently in the repo, the repo wins. Flag the mismatch and ask before proceeding.

**Wholesale rewrite destroys diff context.** Rewriting a full README on every change makes the git diff useless. `str_replace` on specific sections keeps diffs legible and preserves history.

## Live session cache

The `live-session-cache` skill (see `/docs/skills/live-session-cache.md`) auto-activates on this project. Defaults: `trigger` mode on claude.ai chat (activates on first non-chats repo write), `always` mode on Claude Code (activates at session start). The skill logs each substantive turn to a dedicated `chat/YYYY-MM-DD-{slug}` branch, separate from the work branch.

The chat log does not replace this protocol's end-of-session requirements. CHANGELOG entry, ADR if needed, INVARIANTS edits all still apply. The chat log supplements them by preserving the conversation. Each turn's State Delta references work-branch commit hashes so the log is searchable by commit.

If a session activates the skill, the wrap-up flow at session end opens a PR from the chat branch to main with squash-merge recommended in the PR body.

## When to deviate

Only these cases. Anything else is drift.

- **User explicitly says "skip the protocol"** for a task. Honor it, but confirm the task is short enough to warrant skipping.
- **Trivial one-turn question** with no file reads or writes. Answering from memory is fine for questions like "what's the Schnapp Ops token." Do not read `/docs/README.md` for this.
- **Mid-session discovery** invalidates an earlier step. If the wrong README was read at step 4 because the task was misidentified, re-read the correct one. Do not pretend the first read was right.
- **Documentation-only task** touching only `/docs/`. Component READMEs may be skipped at step 4. Router and protocol are still read.

Deviations are disclosed. If step 3 was skipped because the task was trivial, say so in the first response. Silent shortcuts are not acceptable.
