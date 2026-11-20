# Deviations

Every place the rewrite differs from **the legacy code** (D) or **the paper** (P), and why.
Our rule is to emulate the legacy code; anything here is either impossible to emulate on VMs,
a legacy bug, or something the legacy code simply doesn't contain. Anyone changing behaviour
adds a row here.

Line references are to the legacy repo source (see [`protocol.md`](protocol.md) §6).

---

## Against the paper

| # | What | Paper | Ours | Why |
|---|---|---|---|---|
| P1 | Messages per LOM redeployment, plume | 12 | 6 | Our port sends what the source sends. No code we have (source or the authors' original `peer-node-lom.jar`) contains peer-to-peer device queries; the only link to a second peer (`peer_node2` / `RP2`) is for sharing task output. The paper's plume sensors spanned 3 cells, and the authors ran experiment-specific builds (`*_Exp_1`, `*_Exp_2` classes in the jars), so the extra 6 likely came from code we don't have. GOM plume/traffic (8) and LOM traffic (6) match. |
| P2 | Periods / instances per hour | deadlines plume 58 s, traffic 40 s; 62 / 270 instances per hour | deadlines as in the paper; **period 60 s** for both (traffic × 3 intersections) → 60 / 180 instances per hour | The period is in neither the paper nor the XML. Legacy sensors report every 60 s (`end-device/.../Main_Rasp_IoT.java:2312`, `Thread.sleep(60000)`), so we use that. The paper's 62 / 270 can't be derived from anything in the code (58 s and 40 s periods would reproduce them, but that is fitting, not porting). Per-instance rates (deadline-miss %) are compared, not raw counts. |
| P4 | Traffic intersections | 3 intersections across 3 cells | 3 parallel slots, all in the task's cell | The XML has one location for the traffic task (IMW task 4); slots are bound in the nearest cell, as legacy binds within one peer + cell. |
| P3 | Reporting | one unseeded run | mean ± 95% CI over ≥100 seeds | A single unseeded run can't be reproduced by anyone; message counts per redeployment are deterministic and are compared exactly. |

## Against the legacy code

### Behaviour

| # | What | Legacy | Ours | Why |
|---|---|---|---|---|
| D1 | LOM: starting the replacement | "New IP / deployment / communication / Resume Execution" block is commented out (`Mobility_LOM.java:800-890`), so the new device is never told to start | peer sends `START_SERVICE` ("Resume Execution") only | Minimum needed for the replacement to run. Adds 1 message (+1 for the device's reply) to the LOM count. |
| D2 | Device heartbeat period | "I am Alive" only exists inside the original `end-device.jar` | `heartbeat_s = 60` | No source. 60 s tolerates one lost beat under the legacy 120 s timeout. Parameter. |
| D3 | LOM departure detection | the LOM peer *simulates* departures itself (probability model, `Mobility_LOM.java:134`) and announces them at its next 30 s loop | heartbeat timeout (120 s), checked every 30 s, same mechanism as GOM | In our design departures come from the runtime's mobility model, so the peer can only notice them through missing heartbeats. Increases detection lag in LOM (affects deadline misses, **not** redeploy time, which starts at detection). |
| D4 | Who decides departures | the code itself: GOM's end device (`Mobility_IoT` in end-device.jar, every 110 s) and LOM's peer (`Mobility_LOM.java:257`, every 30 s) roll `p = 1 - 0.2·e^(-0.2·r)` against a fixed residence table (`:76`); new devices get 10-15 min; unseeded | the runtime's mobility plug-in, seeded. Two choices: `exponential` (Track 2, the paper's mean 5 min) or `legacy` (`policies/mobility_legacy.py`, the code's rule, including its bug of subtracting the total elapsed time at every check) | The paper and the code disagree, so both are available. With `legacy`, devices live ~6.6 min (GOM) / ~3.2 min (LOM). Simplified: a departed slot is refilled after the worst detection time (GOM 250 s, LOM 160 s) instead of immediately, so the peer can still notice; legacy's clock resets (`Reset_Device`, `Main_Rasp_Combine.java:183`) aren't modelled. Rarely, every device of a LOM cell is away at once and a redeployment finds no replacement (D9). |
| D5 | Replacement choice | `selectAlternateDevice` can return the device that just left (`GOM_Approach.java:346`, no exclusion) | departed device always excluded; also never bind a device twice to the same service of a task | Legacy bug. |
| D6 | Addresses and peer selection | hardcoded Pi IPs (`Mobility_LOM.java:73`, `:579-679`; `Main_Rasp_Combine.java:648-727`), LOM peer hardcoded to 1 (`xml_task_parser.java:130`) | from the registry; task's peer = nearest cell | Hardcoded to the authors' Raspberry Pi testbed; we run on VMs and in simulation. |
| D7 | GOM messages 4-6 | published to the peer's MQTT broker, which the device subscribes to | addressed to the new device directly | Same number of messages and meaning; our transport has one inbox per node. |
| D8 | Start-up messages | deployment + communication XML per service | GOM: one `TASK_ASSIGN{bindings}` per task; LOM: `TASK_DELEGATE` + `BIND_ACK` | Start-up is not redeployment overhead, so Table I is unaffected; total message counts are lower than legacy. |
| D9 | No replacement available | not handled | `RebindEnded(ok=false)`, waiting instances fail | Legacy would hang. |
| D10 | `monitor: all` option | n/a | optional: every binding watched and rebound on its own | Not legacy; off by default. For later experiments. |
| D11 | `qos` binding | QoS columns stored, never read | optional `qos` binding plug-in | Not legacy; `nearest` is the default. Extension point for later work. |

### Data

| # | What | Legacy | Ours | Why |
|---|---|---|---|---|
| D12 | Testbed | MySQL dump (`db_script/gom.sql`), 10 peers × 20 devices, Pi IPs | generated from `configs/*.yaml` with a seed (20 × 20 by default) | Reproducible, no hardcoded addresses; the paper used 20 peers. Every device offers every service, as in `gom.sql`. |
| D13 | Service execution times | DB values, not used by the legacy code | defaults: sensor 0.2 s, fog 2.0 s, cloud 1.0 s (`core/registry.py`) | Only used by the simulator's work model; to be calibrated. |
| D14 | Requirement radius / hardware / availability | in the XML; the legacy device query ignores radius and hardware and filters on `Availability >=` | radius and hardware ignored (importer leaves them unset); service availability not modelled | Radius and hardware: same as legacy. Availability: no equivalent field in our registry yet. |
| D15 | Cloud services | `CloudServiceHandler` on the coordinator | run on the coordinator as `execute` work, never bound to a device | Same placement as legacy. |
| D16 | Service behaviour (live) | sensors query a SOAP sensor server; fog services are jars (`data_integration_service.jar`, `Traffic_Flow_Finder.jar`); the plume was made beforehand in NOAA ALOHA and shipped as KML (`end-device/output_0.kml`, `Aloha_0.jpg`; `Main_Rasp_IoT.java:884` just hands them over) | stubs in `services/`: one reading + legacy unit per sensor from the seeded `ctx.rng` (wind varies around the demo's SW wind), placeholder fog outputs. The dashboard shows legacy's own ALOHA KML and chart by default (`--plume aloha`); `--plume computed` instead draws a Gaussian plume (`services/plume.py`, the model ALOHA uses for neutral gases) from each instance's readings | Only units, the temperature range (20-30, `Main_Rasp_IoT.java:2156`) and the ALOHA output are in the repo. Readings and plume don't affect any Table I metric. |
| D17 | Demo visualization | separate programs, some only on the authors' machines: workflow monitor (`MqttWeb/web-page-monitor.htm`, not in repo), `plume_display.html` and `map_display.html` written by the GPS simulator jar (Google Maps, routes from the Google Directions API), the plume KML and the re-route page served by the authors' server (`203.135.63.70`, not in repo) | one page, `orchflow dashboard` (`viz/`), fed only by the run's messages, with one view per legacy window (`/#map`, `/#monitor`, `/#aloha`, each openable in its own window): workflow monitor with legacy's four colours following each service's current instance (a box stays grey for at least 1.5 s so sub-second work is visible; during a redeployment red until the replacement is picked, then green until it is deployed, our reading of legacy's legend since the monitor's source is not in the repo), Google Maps (keyless, like the demo; `--map osm` for OpenStreetMap) with the ALOHA zones, devices, and the ALOHA chart and summary on their own view; the GPS simulator's 3 trips (`directions.json` from the jar) driven at legacy's 40 km/h; routes fetched once from OpenStreetMap (Nominatim + OSRM) into `viz/data/vehicle_routes.json` | Same pictures from the same inputs. Re-routing logic was on the authors' server and is unknown: our stand-in only stops a car whose remaining route enters the threat zone (none do with the ALOHA plume, as in the video). The cars are for display; they don't feed the traffic task's peer choice (legacy hardcodes peer 7 there, `Workflow_Coordinator_Main.java:780`). |
