# Rules for AI coding agents (and humans)

Read `docs/contracts.md` first, then `src/orchflow/core/interfaces.py`.

## Boundaries
- Work only inside the folders of the track you were asked to work on:
  - Track 1 (logic): `src/orchflow/policies/`, `src/orchflow/services/`, `src/orchflow/core/registry.py`, `workflows/`, `docs/protocol.md`
  - Track 2 (simulation & results): `src/orchflow/runtime/sim/`, `src/orchflow/metrics/`, `experiments/`
  - Track 3 (live & tooling): `src/orchflow/runtime/live/`, `src/orchflow/cli.py`, `deploy/`, `.github/`
- **Do not edit `src/orchflow/core/`** (the shared contract) unless explicitly asked. Adding new
  fakes to `core/testing.py` is the one exception.
- Do not touch or port the legacy Java repo (`IoT-Centric-Workflows-Orchestration`). Read it for reference only.

## Rules the code must follow
- Node logic never sleeps, never reads the wall clock, never does I/O, never uses `random`.
  Use `ctx.now()`, `ctx.set_timer()`, `ctx.send()`, `ctx.start_work()` and `ctx.rng`.
- Every message belonging to a redeployment carries its `rebind_id`.
- Only runtimes emit `MessageSent`; logic never does.
- Units in field names (`_s`, `_m`, `_gb`). Seconds everywhere.
- New behaviour = a new plug-in registered with `@register(...)`, selected in YAML. Don't add `if mode == ...` branches in core.

## Commands
```
uv sync --all-extras        # install
uv run pytest -q            # tests (must pass before every PR)
uv run ruff check src tests # lint
uv run pyright              # types
uv run orchflow plugins     # list registered plug-ins
```

## Done means
Tests for your change exist and pass using the fakes in `core/testing.py`, and nothing outside your folders changed.
