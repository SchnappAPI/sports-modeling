# Live Session Cache: Project Integration

Project-specific rules for the `live-session-cache` skill. The user-installed skill at `~/.claude/skills/live-session-cache/SKILL.md` is the generic implementation. This file overrides defaults for sports-modeling.

## Mode

- claude.ai chat: `trigger` (default)
- Claude Code: `always` (default)

To change globally for this project, add `live-session-cache: <mode>` to project memory. Valid modes: `trigger`, `always`, `manual`.

Mode `trigger` activates the moment Claude is about to write to any path other than `chats/`. Mode `always` activates at session start. Mode `manual` activates only on user trigger phrase.

## Branch and file convention

- Branch name: `chat/YYYY-MM-DD-{slug}` off `main`
- File location while active: `chats/in-progress/{slug}.md`
- File location after wrap-and-merge: `chats/archive/{YYYY}/{MM}/{slug}.md`

The chat branch is always separate from the work branch. Repo edits proceed on whatever branch they would normally use (typically `main` for direct commits, or a feature branch for PR-bound work). The chat log is purely context.

## Integration with existing session protocol

The chat log does not replace the end-of-session protocol from `docs/SESSION_PROTOCOL.md`. All required updates still apply:

- CHANGELOG.md entry at the top of the file, tagged `[sport][component]`
- INVARIANTS section edit via `str_replace` if invariants changed
- ADR in DECISIONS.md if a non-obvious decision was made
- ROADMAP.md update if priorities shifted

The chat log supplements these by capturing the discussion. The CHANGELOG entry summarizes; the chat log preserves the conversation that produced the summary. Each turn's State Delta references the relevant work-branch commit hashes, making the chat log searchable by commit when reviewing why a change was made.

## PR convention

Wrap-and-merge opens a PR from the chat branch to `main`. PR title: `chat: {slug}`. PR body: the final summary block from the file. Squash-merge is recommended (and noted in the PR body) so `main`'s history shows one commit per chat instead of N turn-level commits.

## Resumption

Open `chat/*` branches are listed at the start of any chat with this skill active. The user picks resume-or-new. Resumed chats append a `## Session resumed — {timestamp}` marker before the next turn.

## What does not go in the chat log

- Credentials, API keys, or secrets even if they appeared in the conversation. Redact at write time.
- Long file contents pasted by the user. Tool output (web fetch, file reads) is fine; pasted file dumps are not.
- Code that was never accepted by the user. Only what landed on the work branch gets logged via State Delta and commit hash references.

## Surface-specific notes

### claude.ai chat (Mode `trigger`)

Per-turn writes via GitHub MCP `create_or_update_file` upload the full file each commit. The chat file grows each turn, so latency grows too. Activation only on first non-chats write keeps overhead off short throwaway chats.

### Claude Code (Mode `always`)

Per-turn writes are local git operations. A single push at session end pushes all turn commits at once. Latency is negligible. The chat branch lives alongside the work branch in the local repo; switching between them is a normal git checkout.

## Per-turn entry format

Each turn appends a block of this shape to the chat file:

```
## Turn N — YYYY-MM-DD HH:MM

### User
{verbatim user message}

### Reasoning
{2 to 5 sentence summary of approach taken}

### Response
{verbatim assistant response}

### Evolution note
{when this turn caused a reframing or shift in approach: how the shift happened, even if no code changed. Omit when not applicable.}

### State delta
{decisions made, files touched on the work branch with commit hashes, errors hit, open questions. Write "No state change" rather than omitting the section.}
```

Commit message format on the chat branch: `chat: turn N — {short description}`

## Backfill (Turn 0)

When activation happens mid-conversation (Mode `trigger`), Turn 0 is a full reconstruction of the conversation up to the activation point, not a summary. Every prior turn appears in full with all four fields populated. The only thing condensed is reasoning, which gets the per-turn 2 to 5 sentence treatment. The point is to capture the evolution of the idea, including discussions that did not directly produce code.
