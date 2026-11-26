# CLAUDE.md - project_aepa

Animal abuse and later crime, from 7.7M Taiwanese criminal judgments. Start Claude Code
inside `project_aepa/` so this file loads.

## Read first

- [DEVLOG.md](DEVLOG.md) *Current State*: pipeline, commands, next steps, known issues.
  It is the single source of truth for status.
- [PIPELINE.md](PIPELINE.md): flow diagram and the Stage 2 / Stage 3 division of work.
- [README.md](README.md): setup and where each kind of data lives.
- [animal-abuse-crime-linkage-data-plan.md](animal-abuse-crime-linkage-data-plan.md): research design.

## Working rules

- Reply in Taiwan Traditional Chinese, concise. Code comments and .md files in English.
- **The user runs scripts.** Give the exact commands; do not run pipeline scripts, LLM runs
  or anything long or heavy yourself. Small read-only checks (a few batches, a sample of
  rows) are fine.
- **Git:** give commands for the current branch (usually `main`); never suggest
  `git checkout` or a new branch. Keep every command on one short line (long lines get
  wrapped on paste and break in zsh); use `git commit -F- <<'EOF'` for multi-line messages.
  The user commits and pushes.
- Lint before handing over code: pre-commit runs ruff and ruff-format. The ruff binary is
  at `~/.cache/pre-commit/repo*/py_env-*/bin/ruff`.
- After every change to a script, command or plan: update DEVLOG *Current State*, add a
  dated *Log* entry explaining why (never rewrite old entries), and update PIPELINE.md if a
  step's status or design changed. README holds setup only.
- Prefer real data to synthetic examples when testing or explaining.
- Keep the Ollama Stage 3 scripts and other superseded scripts (reference only).
- Resource monitoring must capture peaks (`resource_monitor.py`).

## Data and privacy

- All scripts run from `project_aepa/`. Data under `data/` is not in git except
  `data/labels/` and `data/processed.dvc`; see README for what is DVC-tracked.
- Defendant names are never written to outputs: only HMAC hashes (`NAME_HASH_KEY` in
  `.env`, identical on every machine, never committed, never changed).
- Use the structured offsets (`data/structured/`, `judgment_parser.section()`) instead of
  re-parsing full text.

## Method decisions (see DEVLOG Log for the reasons)

- Validate data before analysis. Analysis is paused until Check 1 and Check 2 pass.
- The open data platform is the only source; no extra scraping.
- Stage 1: criminal articles of the Animal Protection Act (25, 25-1, 26, 27-1, old 30)
  cited in statutes, 主文 or reasons, OR the title rule. Wildlife Act cases and article 7
  (owner's duty, dog bites) are out of scope.
- **Stage 2 finds candidates, it does not judge.** Remove only text that cannot be facts;
  tag likely noise instead of dropping it. Do not add hard-coded exclusion rules.
- **Stage 3 makes every judgment.** The LLM answers factual questions; scope is decided in
  code from those answers. Bump `PROMPT_VERSION` whenever a prompt or schema changes.
- Check 1 samples by source and tag to test both stages.
- Unit of analysis: one defendant in one case (merge appeal levels, split co-defendants).
