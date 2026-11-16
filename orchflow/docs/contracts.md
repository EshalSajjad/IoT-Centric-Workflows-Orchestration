# Shared building blocks (the contract)

**Status: DRAFT for the Friday 6 Nov group call.** Once approved, everyone codes against
this. Code lives in `src/orchflow/core/`; this page explains it in plain words.

---

## The one idea to understand

Orchflow's decision-making ("which device do I pick? what do I send when a device
leaves?") is written **once**, by Track 1, as plain Python classes that only *react*:

- a **message** arrives → `on_message`
- a **timer** goes off → `on_timer`
- some **work** finishes (a deploy, a sensor read) → `on_work_done`
- the node starts / a device departs → `on_start` / `on_depart`

and they *respond* only by calling methods on a **context** object (`ctx`):
`ctx.send(...)`, `ctx.set_timer(...)`, `ctx.start_work(...)`, `ctx.emit(...)`,
`ctx.now()`, `ctx.rng`, `ctx.registry`.

The logic never sleeps, never reads the real clock, never opens a network connection
and never uses `random`. That's what makes it reusable:

| Who provides `ctx` | What it really does |
|---|---|
| Track 2: simulation | fake clock, messages passed in memory, durations sampled → 60 min in seconds |
| Track 3: live | real clock, messages over MQTT (Mosquitto), real file transfer / processes |
| Tests: `RecordingContext` | does nothing, just records what the logic asked for |

---

## The blocks

### 1. Things in the testbed (`core/models.py`)
`Location`, `Cell` (an area), `PeerNode` (one per cell), `Device`, `Service` (something
a device offers, e.g. `wind_speed`), `Artifact` (a file to deploy, e.g. the plume jar).

- IDs: `coord`, `peer-01`, `dev-0001`. **A node's ID is also its address.**
- Units are in the field name: `_s` seconds, `_m` metres, `_gb` gigabytes. No minutes.
- Objects are read-only. "Is this device available?" lives in the **Registry**.

### 2. Input files (`core/spec.py`), all YAML
| File | Model | Says |
|---|---|---|
| `workflows/*.yaml` | `WorkflowSpec` | tasks, what services each needs, period, deadline, dependencies |
| `configs/*.yaml` | `TestbedSpec` | how many peers × devices, area, seed → the registry is **generated** from this |
| `experiments/*.yaml` | `ExperimentSpec` | which workflow + testbed, `sim` or `live`, GOM or LOM, which plug-ins, how many seeds |

Misspelt keys are rejected (so typos fail loudly). Examples are in each folder.

### 3. Messages (`core/messages.py`)
The **only** way nodes talk. A `Message` has a `kind` (e.g. `task_assign`,
`device_left`, `rebind_request`), `src`, `dst`, `payload`, and optional `task_id`,
`instance`, `rebind_id`.

> **Counting rule:** every message that is part of a redeployment carries that
> redeployment's `rebind_id`. Table I's message column = messages with a `rebind_id`.
> This is how we *measure* messages per redeployment instead of counting stdout by hand.
>
> **We port, we don't fit.** GOM/LOM send exactly the messages the legacy code sends
> during a rebind, one `ctx.send` per legacy MQTT publish. We do **not** add or remove
> messages to hit the paper's 8 / 12 / 8 / 6. The paper's figures are a comparison we
> report; any difference goes in `docs/deviations.md` with the reason.

### 4. Event log (`core/events.py`)
Everything that happens is written as one line of JSON in `events.jsonl`: run start/end,
task instance start/end, device joined/left/bound, rebind start/end, deployment start/end,
message sent. **All Table I numbers are computed from this file only.**

- The **runtime** logs: run start/end, device joined/left, and *every* message sent (automatically).
- The **logic** logs: task instances, bindings, rebinds, deployments (via `ctx.emit`).
- Every duration is measured with two events from the **same node** (machines' clocks differ).

### 5. Plugs (`core/interfaces.py`)
| Interface | Written by | Purpose |
|---|---|---|
| `NodeLogic`, `OrchestrationProtocol` | Track 1 | GOM / LOM behaviour of coordinator, peer, device |
| `BindingPolicy` | Track 1 | pick one device from candidates (`nearest` = paper behaviour) |
| `Registry` | Track 1 | look up devices/services; `candidates(..., exclude=...)` |
| `NodeContext` | Tracks 2 and 3 | the `ctx` described above |
| `MobilityModel` | Track 2 | when devices leave (exponential, mean 5 min); used by both runtimes |
| `NetworkModel`, `WorkModel` | Track 2 | sim only: message delay, deploy/execute duration |

### 6. Plug-ins by name (`core/plugins.py`)
`@register("binding", "nearest")` on a class → selectable in YAML as
`binding: {name: nearest}`. Run `orchflow plugins` to see what's registered.
This is how future students add ideas without touching core code.

### 7. Fakes for working alone (`core/testing.py`)
| Fake | Use it if you're… |
|---|---|
| `RecordingContext` | Track 1: test GOM/LOM without any runtime |
| `EchoProtocol` (`orchestration: echo`) | Tracks 2 and 3: a dummy protocol to test your runtime before GOM/LOM exist |
| `tiny_registry()`, `tiny_workflow()`, `tiny_experiment()` | everyone: small ready-made inputs |
| `sample_events()` | Track 2: an event log whose correct metrics are written in its docstring |

`tests/test_contracts.py` shows each one in use.

---

## Who builds what (from PLAN_v2 §8)

| Track | Folder(s) | Builds against |
|---|---|---|
| 1. Logic | `policies/`, `services/`, `importers/`, `core/registry.py`, `workflows/`, `docs/protocol.md`, `docs/deviations.md` | `NodeLogic`, `BindingPolicy`, `Registry`; tests with `RecordingContext` |

**Track 1 is done when:** the rebind message sequences of GOM and LOM are traced from the
legacy code into `docs/protocol.md` (each message cites its legacy file:line); the Python
port sends exactly that sequence (tests assert it message by message); the departed device
is never re-picked; the IMW workflow loads from YAML. The resulting count per rebind is
compared with the paper's 8 / 12 / 8 / 6 and any gap is explained in `docs/deviations.md`.

**Which legacy code to port from:** use the `.java` source, not the shipped jars.
`workflow-coordinator.jar` and `peer-node-gom.jar` were rebuilt from source by us, so they
match it. `peer-node-lom.jar` and `end-device.jar` are the authors' originals and contain
different code (`com.hascode...Main_Rasp_Local`, `Mobility_IoT`); look inside them only
for behaviour that has no source at all (e.g. the device heartbeat / "I am alive").
| 2. Simulation & results | `runtime/sim/`, `metrics/`, `experiments/` | `NodeContext`, `MobilityModel`, events; tests with `echo` + `sample_events()` |
| 3. Live & tooling | `runtime/live/`, `deploy/local/`, `cli.py`, CI | `NodeContext`, messages; tests with `echo` |

Stay inside your own folders. On Sunday, swap the fakes for the real parts.

---

## Changing the contract

Anything under `src/orchflow/core/` (except adding fakes to `testing.py`) is shared.

1. Open a PR titled `contract: ...` explaining why.
2. **Any one other member** approves it. No single gatekeeper.
3. Adding a field with a default or adding a new message kind is fine. Renaming or removing
   something breaks the others: tell the group first.
4. If you change the event log format, bump `SCHEMA_VERSION`.

## Questions to settle on the call
- [ ] Are the message kinds in `messages.py` enough for GOM and LOM? (Track 1 to check against the legacy code.)
- [ ] Does the live runtime run one process per node, or many nodes per process? (Track 3; doesn't affect the contract.)
- [ ] Names for each track.
