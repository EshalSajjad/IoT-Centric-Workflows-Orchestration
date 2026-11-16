# Orchflow (rewrite)

Orchestration and management of IoT-centric distributed workflows: the GOM and LOM
protocols from *Demo: Orchflow* (ICDCS 2024), rebuilt in Python so the same logic runs in a
fast reproducible **simulation** and in a **live** MQTT deployment, with every event logged
so Table I can be regenerated with one command.

> Setup, Table I reproduction and status: see the [repository README](../README.md).
> Contracts explained: [`docs/contracts.md`](docs/contracts.md).

## Layout
```
src/orchflow/core/          shared building blocks (contract): models, specs, messages, events, interfaces, fakes
src/orchflow/policies/      Track 1: GOM / LOM logic, binding policies
src/orchflow/runtime/sim/   Track 2: SimPy runtime
src/orchflow/metrics/       Track 2: event log -> Table I
src/orchflow/runtime/live/  Track 3: MQTT runtime
src/orchflow/cli.py         Track 3: `orchflow` command
workflows/ configs/               YAML inputs: workflows and testbeds
experiments/                       one YAML per run; plume/traffic x gom/lom = Table I
deploy/local                       Docker Compose on one machine
deploy/vm                          multi-VM setup (bootstrap.sh, orchflow_vm.py); Oracle too
docs/                       contracts, protocol, deviations, decisions
tests/                      contract tests (+ each track's tests)
```

## Quickstart
```
uv sync --all-extras
uv run pytest -q
uv run orchflow plugins
```
(Python 3.11+; 3.12 recommended. Without uv: `python -m venv .venv`, then `pip install -e ".[dev]"`.)

Agents and contributors: read [`AGENTS.md`](AGENTS.md).
