# deploy/vm: run Orchflow across VMs

Runs the live runtime on several Ubuntu VMs (Oracle Cloud or local), driven from your laptop.
Every VM runs the same Docker image. One VM also runs the MQTT broker (Mosquitto). The VMs
talk over [Tailscale](https://tailscale.com), so nothing needs a public port.

```
 laptop ── ssh ──►  orchflow-vm-1   coord        + Mosquitto (Tailscale IP only)
 (orchflow_vm.py)   orchflow-vm-2   peer-01..10  + their 200 devices
                    orchflow-vm-3   peer-11..20  + their 200 devices
                         │ all agents connect to the broker over the tailnet
```

Devices are not VMs. They are lightweight agents packed onto the peer VMs, and they "leave"
and "rejoin" by following the experiment's mobility model inside the agent. No VM is ever
stopped during a run.

| File | What it is |
|---|---|
| `bootstrap.sh` | Prepares one VM: Docker, Tailscale, chrony, swap, the repo, the image, and optionally Mosquitto. Safe to run again. |
| `orchflow_vm.py` | Laptop-side driver with commands `setup`, `update`, `check`, `run`, `stop`. |
| `inventory.example.yaml` | Which VM hosts which nodes. Copy it to `inventory.yaml`, which is gitignored. |
| `cloud-init.example.yaml` | Bootstraps an Oracle VM on first boot. |

## 1. What you need

- **VMs:** Ubuntu 22.04 or 24.04, x86_64 or ARM64 (Oracle Ampere works). Each needs SSH
  access with your key and a user with passwordless `sudo`; the `ubuntu` user on Oracle and
  Multipass images has both.
  - Rough sizing for the paper testbed: 1 VM for the coordinator and broker, plus 2 VMs with
    10 peers each. 1 GB of RAM per VM is enough, because `bootstrap.sh` adds 2 GB of swap.
- **Tailscale:** one account. For unattended setup, create an auth key under
  *Settings → Keys*.
- **On your laptop:** `ssh` and `scp` (built into Windows 10+, macOS and Linux), plus Orchflow
  installed:
  ```
  cd orchflow
  uv sync --all-extras
  ```
- **Pushed code:** the VMs `git fetch` from GitHub, so push your branch before `update` or
  `run`.

## 2. Create and bootstrap the VMs

Pick one of these.

**Option A, Oracle Cloud (cloud-init).**
1. Create each instance from an Ubuntu image.
2. Paste a filled-in copy of `cloud-init.example.yaml` into *Advanced options → Management →
   Cloud-init script*.
3. Use a different `TS_HOSTNAME` per VM, and set `RUN_BROKER=1` on exactly one VM.

The VM sets itself up on first boot. To watch progress on the VM:
`sudo tail -f /var/log/orchflow-bootstrap.log`.

**Option B, any VM you can SSH into (local or cloud).**
1. Install Tailscale on your laptop and log in.
2. Write `deploy/vm/inventory.yaml`, starting from the example. Until the VMs are on the
   tailnet, use addresses your laptop can already reach.
3. From `orchflow/`, run:
   ```
   TS_AUTHKEY=tskey-auth-... uv run python deploy/vm/orchflow_vm.py setup -i deploy/vm/inventory.yaml
   ```
   This pipes `bootstrap.sh` to every VM over SSH, in parallel. The auth key travels over
   stdin and is never written to the inventory. Logs go to `results/vm-setup/`.
4. Once the VMs show up in `tailscale status`, switch the inventory to their Tailscale names.

On Windows PowerShell, set the key first instead: `$env:TS_AUTHKEY="tskey-auth-..."`.

To bootstrap a single VM by hand: `sudo RUN_BROKER=1 bash bootstrap.sh`. The top of
`bootstrap.sh` lists every setting.

## 3. Check

```
uv run python deploy/vm/orchflow_vm.py check -i deploy/vm/inventory.yaml experiments/plume_gom.yaml
```

For each VM this checks SSH, the image, the commit it is on, and whether it can reach the
broker. With an experiment given, it also checks that the hosts together cover every node of
the testbed exactly once.

Put every VM on your current commit:
```
uv run python deploy/vm/orchflow_vm.py update -i deploy/vm/inventory.yaml
```
This checks out your local `HEAD` and rebuilds the image. Use `--ref` to pick a different
commit or branch.

## 4. Run

Smoke test first: 2 minutes, devices never leave.
```
uv run python deploy/vm/orchflow_vm.py run -i deploy/vm/inventory.yaml experiments/plume_gom.yaml --seeds 1 --duration 120 --no-mobility
```

Full runs, 60 minutes per seed:
```
uv run python deploy/vm/orchflow_vm.py run -i deploy/vm/inventory.yaml experiments/plume_gom.yaml --seeds 3
uv run python deploy/vm/orchflow_vm.py run -i deploy/vm/inventory.yaml experiments/plume_lom.yaml --seeds 3
```

For each seed, `run` does this:
1. Starts the agent container on every follower VM, then on the coordinator's VM.
2. The coordinator waits until every node says "ready", then starts the run.
3. After `duration_s`, the coordinator stops everyone.
4. Each VM's `events-<host>.jsonl` is copied into
   `results/<experiment>-live-<time>/seed-NNN/`.
5. At the end, it prints Table I. You can rebuild Table I later with
   `uv run orchflow table1 results/<folder>`.

Seeds run one after another, so 3 seeds take about 3 hours. If something goes wrong:
```
uv run python deploy/vm/orchflow_vm.py stop -i deploy/vm/inventory.yaml
```

Add `--dry-run` to any command to print the SSH commands without running them.

## Inventory

```yaml
ssh: {user: ubuntu, key: ~/.ssh/id_ed25519}
hosts:
  orchflow-vm-1: {nodes: coord, run_broker: true}
  orchflow-vm-2: {peers: 1-10}          # peer-01..peer-10 and all their devices
  orchflow-vm-3: {peers: 11-20, cpus: 2}
```

- **`peers: a-b`** expands to `peer-NN,devices@peer-NN` for each peer, so a cell's traffic
  stays on one VM.
- **`nodes:`** takes anything `orchflow agent --nodes` accepts, e.g. `coord`, `peer-03`,
  `devices@peer-03`, or `all`.
- **`cpus:`** caps the container with `docker --cpus`.
- **`broker:`** defaults to the host marked `run_broker`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `check` says `broker=UNREACHABLE` | Is the broker VM on the tailnet (`tailscale status`)? Is `orchflow-broker` running there (`docker ps`)? If you bootstrapped before logging in to Tailscale, re-run `setup` on that host. |
| `permission denied ... docker.sock` | `bootstrap.sh` added your user to the `docker` group. Open a new SSH session, or re-run the command. |
| `check` shows a different commit | Push your branch, then run `update`. |
| The coordinator warns "starting without N node(s)" | A host started late or crashed. See its logs with `ssh <host> docker logs orchflow-agent`. Raise `--ready-timeout` for slow VMs. |
| Oracle: VMs can't reach each other over Tailscale | `bootstrap.sh` adds an `iptables` ACCEPT rule for `tailscale0`. Check it with `sudo iptables -S INPUT`. |

## Not done yet

- **Deploying and executing are timed waits, not real work.** `runtime/live/work.py` uses
  `SleepWork`: no artifact is copied or started. Redeploy times on VMs therefore come from the
  configured sizes and rates, not from the network.
- **No network shaping.** The experiment's `network:` setting only applies in simulation.
  Shaping VM links with `tc netem` to look like edge Wi-Fi is the next step.
- **The broker has no authentication.** It only listens on the Tailscale IP, so keep it that
  way.
