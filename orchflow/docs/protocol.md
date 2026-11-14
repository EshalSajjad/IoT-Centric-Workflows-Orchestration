# GOM / LOM protocol

**Owner: Track 1 (logic).** Code: `src/orchflow/policies/` (`common.py`, `gom.py`, `lom.py`).
Tests: `tests/test_policies.py`.

**Rule we follow: emulate the legacy code, don't fit the paper.** Every message below is one
`ctx.send`, and each one corresponds to one MQTT publish in the legacy Java source, cited as
`File.java:line`. Line numbers refer to the legacy repo at commit `2e783d2`, **source files,
not the shipped jars** (see "Which legacy code" below). Every place we had to differ is listed
in [`deviations.md`](deviations.md).

---

## 1. Who does what

| | GOM (Global) | LOM (Local) |
|---|---|---|
| Picks devices at start | coordinator | the task's peer node |
| Deploys artifacts | coordinator | the task's peer node |
| Detects a departure | the device's peer (heartbeat timeout) | the device's peer (heartbeat timeout) |
| Picks the replacement | coordinator | the peer, **inside its own cell only** |
| Runs task instances | coordinator starts them; the peer collects device results | same |

The two modes differ **only** in who decides. Everything else is shared code in
`policies/common.py`.

**Which peer runs a task:** the peer whose cell centre is nearest the task location
(legacy `xml_task_parser.Peer_Node_Search`). The shipped LOM code skipped this and hardcoded
`Selected_Peer_Node = 1` (`xml_task_parser.java:129-130`).

**Which device is picked:** the binding plug-in (`nearest` = legacy `Helper.findMinimal`),
among available devices in the task's cell that offer the service. Both legacy modes
restrict the search to one peer + cell (`GOM_Approach.java:346`, `xml_task_parser.java:280`).

---

## 2. Normal operation (not overhead)

```
start   GOM: coord binds + deploys, sends TASK_ASSIGN{bindings} (communication message) to peer
        LOM: coord -> peer TASK_DELEGATE; peer binds + deploys; peer -> coord BIND_ACK
        then the coordinator starts the task (after its dependencies completed once)

every period_s, for each parallel slot:
  coord  -> peer    TASK_ASSIGN {slot}         TaskInstanceStarted (deadline_at)
  peer   -> device  DATA_REQUEST (sensor) / TASK_ASSIGN (fog)   one per bound device
  device -> peer    TASK_RESULT                after its "execute" work
  peer   -> coord   TASK_RESULT {ok}           when every bound device answered
  coord             TaskInstanceEnded          (cloud services run here first, as work)

liveness:
  device -> peer    DEVICE_JOIN once at start / rejoin, then HEARTBEAT every heartbeat_s
```

None of these carry a `rebind_id`, so none count as redeployment overhead.

---

## 3. Detecting a departure

Legacy devices **never announce** that they leave. The peer notices:

| | GOM | LOM |
|---|---|---|
| Check loop | every **120 s** (`Mobility_GOM.java:63`) | every **30 s** (`Mobility_LOM.java:93`) |
| Departed when | last "I am Alive" older than **120 s** (`Mobility_GOM.java:89-91`) | see deviations D3 |
| Which devices are watched | only devices that received a deployment (`Mobility_GOM.java:86`) | same (`Mobility_LOM.java:341`) |

Parameters (`orchestration.params` in the experiment YAML), defaults = legacy:

| param | GOM | LOM | meaning |
|---|---|---|---|
| `check_interval_s` | 120 | 30 | how often the peer checks |
| `heartbeat_timeout_s` | 120 | 120 | silence after which a device has left |
| `heartbeat_s` | 60 | 60 | device heartbeat period (not in source, see D2) |
| `monitor` | `deployable` | `deployable` | `all` = watch every binding (NOT legacy) |

**What moves with a redeployment.** Legacy replaces the departed device's IP **everywhere in
the task's communication message** (`Workflow_Coordinator_Main.java:298`,
`Mobility_LOM.java:567`). So if the departed device also served unwatched services of the same
task (e.g. `wind_speed` on the plume device), they move to the replacement in the **same**
redeployment. One departure = one redeployment.

---

## 4. Redeployment message sequences

Every message below carries the redeployment's `rebind_id`; Table I's "messages" column counts
exactly these. `RebindStarted` / `RebindEnded` are emitted by the **peer** (one clock).

### GOM: 8 messages

| # | From → to | Kind | Legacy message | Legacy code |
|---|---|---|---|---|
| | peer | *RebindStarted* | timeout detected | `Mobility_GOM.java:89-95` |
| 1 | peer → coord | `REDEPLOY_START` | `<hint>[k]</hint> Start` | `Mobility_GOM.java:108-115` (task 1), `:146-153` (task 4); coord starts its timer at `Workflow_Coordinator_Main.java:242-246` |
| 2 | peer → coord | `DEVICE_LEFT` | `IP Address: [ip] Device has left {k}` | built `Mobility_GOM.java:97`, sent `:239`; handled `Workflow_Coordinator_Main.java:259` |
| | coord | | picks replacement (excluding the departed device), SFTPs artifact | `GOM_Approach.java:346` (`selectAlternateDevice`) |
| 3 | coord → peer | `REBIND_REPLY` | `New IP_Address =…` | `Workflow_Coordinator_Main.java:341`; peer handles `Main_Rasp_Combine.java:172` |
| 4 | coord → new device | `DEPLOY` | deployment message | `Workflow_Coordinator_Main.java:344` |
| 5 | coord → new device | `TASK_ASSIGN` | communication message | `Workflow_Coordinator_Main.java:347` |
| 6 | coord → new device | `START_SERVICE` | `Resume Execution` | `Workflow_Coordinator_Main.java:350` |
| 7 | new device → peer | `TASK_RESULT` | execution `<response>` | handled `Main_Rasp_Combine.java:451` |
| 8 | peer → coord | `REDEPLOY_DONE` | `<hint>[k]</hint> End` | `Main_Rasp_Combine.java:471` (task 1), `:481` (task 4); coord stops timer `Workflow_Coordinator_Main.java:249-254` |
| | peer | *RebindEnded* | | |

Messages 3–6 were published to the peer's broker, which the devices subscribe to; we address
4–6 to the new device directly. Same count, same meaning.

### LOM: 6 messages

| # | From → to | Kind | Legacy message | Legacy code |
|---|---|---|---|---|
| | peer | *RebindStarted* | departure detected | `Mobility_LOM.java:257` (`get_device_status_after_2_mins`) |
| 1 | peer → departed device | `DEVICE_LEFT` | `IP Address [ip] Device has left` | built `Mobility_LOM.java:348`, sent `:359` (it is gone, so the runtime drops it, but it is still sent and counted) |
| 2 | peer → coord | `REDEPLOY_START` | `<Task>[k]</Task> Device left` | `Mobility_LOM.java:384/396`, sent `:390/402`; coord starts timer `Workflow_Coordinator_Main.java:506` |
| 3 | peer → coord | `DEVICE_LEFT` | `IP Address [ip] Device has left` | `Mobility_LOM.java:391/403` |
| | peer | | picks replacement in its own cell, SFTPs artifact | `Mobility_LOM.java:561` → `xml_task_parser.java:247`; upload `Mobility_LOM.java:696-707` |
| 4 | peer → coord | `REDEPLOY_DONE` | `Redeployment Done [k]` | `Mobility_LOM.java:766-796`; coord stops timer `Workflow_Coordinator_Main.java:515` |
| | peer | *RebindEnded* | | |
| 5 | peer → new device | `START_SERVICE` | `Resume Execution` | **commented out** in source (`Mobility_LOM.java:800-890`); see D1 |
| 6 | new device → peer | `TASK_RESULT` | execution `<response>` | (device echoes the `rebind_id`, as in GOM step 7) |

### Compared with the paper (Table I, messages per redeployment)

| | Our port | Paper |
|---|---|---|
| GOM plume | 8 | 8 |
| GOM traffic | 8 | 8 |
| LOM traffic | 6 | 6 |
| LOM plume | **6** | **12** |

The LOM-plume gap is explained in [`deviations.md`](deviations.md) (P1).

### If no replacement exists

GOM: the coordinator replies `REBIND_REPLY{device_id: null}`; the peer emits
`RebindEnded(ok=false)` and fails every instance waiting on that binding. LOM: the peer does
the same without asking anyone. (Legacy had no handling for this case.)

---

## 5. Notes for the other tracks

**Tracks 2 and 3 — mobility:** two plug-ins. `exponential` (Track 2) is the paper's
description; `legacy` (`policies/mobility_legacy.py`, `mobility: {name: legacy, params: {mode:
gom}}`) is what the original code did, see deviations D4. Use the `mode` matching the
orchestration. Departed devices must come back (or the cell empties within the hour); with
`exponential`, set `mean_rejoin_s`.

**Tracks 2 and 3 — work requests (`ctx.start_work`):**

| `work_id` prefix | kind | node | `data` keys | meaning |
|---|---|---|---|---|
| `deploy:` | `deploy` | GOM coord / LOM peer | `device_id`, `artifact`, `key`, `rebind_id`, `task_id` | transfer `artifact` **to `device_id`** (legacy SFTP); `rebind_id` is `None` at start-up |
| `exec:` | `execute` | device | `key`, `task_id`, `instance`, `reply_to`, `rebind_id` | run `service` once |
| `cloud:` | `execute` | coordinator | `task_id`, `instance` | run the task's cloud part (`service=None`) |

Live runtime: for `execute` work call `orchflow.services.run_service(service.name or None, ctx.rng)` and
return the work's `data` with `data["output"]` set to its result; the device forwards it in
`TASK_RESULT.payload["output"]`. The simulator can leave `output` unset.

**Registry availability:** the **logic** marks a device unavailable when its peer detects the
departure and available again on its next heartbeat / join. Runtimes should **not** call
`set_available` themselves; otherwise the coordinator learns of departures before any message
says so.

**Track 2 (metrics) — counting instances:** the workloads are `workflows/plume.yaml`
(period 60 s = the legacy sensor loop, deadline 58 s) and `workflows/traffic.yaml`
(period 60 s, deadline 40 s, 3 parallel intersections). Count the instances **whose deadline
falls within `duration_s` (3600 s) of the first instance's start**: 60 for plume and
60 × 3 = 180 for traffic (the paper reports 62 / 270, see deviations P2; compare rates, not
counts). Measure from the first instance, not t = 0, because start-up deployment takes a few
seconds.

**Track 3 (CLI):** the importer is `orchflow.importers.legacy_xml.main(argv)`; wire
`orchflow import-legacy` to it (same arguments as `python -m orchflow.importers.legacy_xml -h`).

**Binding keys:** bindings are named `"{task_id}/{slot}/{service}/{n}"` (`policies/common.py`,
`bkey`). They appear in message payloads as `payload["key"]`.

---

## 6. Which legacy code we ported from

The `.java` sources, not the shipped jars. `workflow-coordinator.jar` and `peer-node-gom.jar`
were rebuilt from source by us (Sept/Oct 2026), so they match it. `peer-node-lom.jar` and
`end-device.jar` are the authors' original builds and contain different code
(`com.hascode…Main_Rasp_Local`, `Mobility_IoT`, `*_Exp_1` / `*_Exp_2` variants); they were only
consulted for behaviour with no source at all.
