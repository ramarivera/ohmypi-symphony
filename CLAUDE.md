# CLAUDE.md

@AGENTS.md

`AGENTS.md` is the canonical, harness-neutral guide. Edit it, not this file,
for anything that isn't Claude Code specific.

## Claude Code notes

- `.claude/settings.json` runs `bd prime` on SessionStart and PreCompact. If
  beads context looks missing after compaction, run `bd prime` yourself.
- Track work in `bd`, not TodoWrite/TaskCreate or markdown files.
- Run Bun via `mise exec -- ...`; the harness shell may not have mise's Bun
  on PATH.
