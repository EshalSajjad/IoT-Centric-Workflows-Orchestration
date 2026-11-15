# Track 2 — Simulation & Results

Implementation locations: `src/orchflow/runtime/sim/`, `src/orchflow/metrics/`, and `experiments/`.
Nothing in the shared `core/`, the CLI, or other tracks was changed.

## Run it

Install dependencies in the `orchflow/` project root (Python 3.11+):

```sh
uv sync --all-extras
uv run pytest -q
uv run pytest -q src/orchflow/runtime/sim/test_sim.py src/orchflow/metrics/test_metrics.py
uv run python experiments/demo_echo_sim.py --seeds 3 --duration-s 60
uv run python experiments/demo_echo_sim.py --seeds 100 --duration-s 3600
```

For Table I after runs are saved:

```sh
uv run python -m orchflow.metrics.table1 results/echo-demo -o results/echo-demo/table1.csv
```

The demo uses `tiny_registry()` and the `echo` plugin to test without Track 1.
It is **not** a GOM/LOM result and cannot validate the paper's 8/12/8/6
redeployment-message counts. Track 1 must provide `core.registry.build_registry`
and GOM/LOM policies, and Track 3 must wire `orchflow run`/`orchflow table1`.

## Simulation contracts

- `run_simulation(experiment, workflow, registry, seed=..., event_log_path=...)`
  returns a `SimulationResult` containing deterministic events.
- The runtime alone logs `RunStarted`, `RunEnded`, `MessageSent`, `DeviceJoined`,
  and `DeviceLeft`. It delivers messages in memory, without wall-time sleeps.
- Named timers replace earlier timers; cancel/departure invalidate stale callbacks.
  Work completion is ignored if the device departed or its generation changed.
- Network/work/mobility plugins are registered when `orchflow.runtime.sim.env`
  is imported. For the shared `orchflow plugins` command to enumerate them
  without importing the simulator first, the group should agree on extending
  `core/plugins.py`'s `_BUILTIN_MODULES`; do that through the shared-contract
  review process rather than editing another track's code.
- Simulator assigns reproducible wire IDs (`sim-...`) to outbound messages
  because `Message.id` defaults to random UUIDs. Policies should correlate
  by received wire IDs, not retain pre-send draft IDs.
- Default mobility: Exponential residence time mean 300 seconds, graceful
  departure, no rejoin. `mean_rejoin_s` and `graceful_probability` are optional.
  Rejoins use new node lifetimes to invalidate pending callbacks.
- `FixedWork` execute duration uses `Service.execution_time_s` when available;
  deploy duration depends on artifact size and transfer rate. Additional network
  models include `lan` and `edge_wifi` with configurable jitter.

## Metrics contracts

- `compute_metrics(events)` or `compute_from_file(events_jsonl)` operates only
  on shared `core.events.Event` records.
- Deadline miss = failed task OR completion later than deadline. Multiple
  results for one task instance are combined (latest completion, all must pass).
  Miss delay is measured **beyond the deadline**, among missed instances.
- Rebind duration uses events from the same emitting node, never two clocks.
- Overhead messages = `MessageSent` records with a non-null `rebind_id`.
- `build_table1(paths)` calculates a per-seed summary and two-sided Student-t
  95% CI across independent seeds. With just one seed the CI is undefined.
- Track 1 must settle the precise paper Table I field definitions and scenarios
  before claiming scientific reproduction. No calibration constants have been
  fabricated or applied to the raw event metrics.
