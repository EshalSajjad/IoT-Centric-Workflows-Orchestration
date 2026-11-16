# deploy/local (Track 3)

Run the whole live system on one laptop with Docker.

```
cd orchflow/deploy/local
docker compose up --build
```

This starts four containers:

| container   | what it runs                                        |
|-------------|-----------------------------------------------------|
| broker      | Mosquitto, the MQTT broker (also on localhost:1883) |
| coordinator | `orchflow agent ... --nodes coord` (the leader)     |
| peers       | `orchflow agent ... --nodes peers` (peer-01, peer-02) |
| devices     | `orchflow agent ... --nodes devices` (dev-0001..0006) |

The experiment is `experiment_echo.yaml`: the dummy `echo` protocol on a 2 x 3 testbed
(`testbed_local.yaml`), one round every 5 s, for 30 s. The coordinator waits until peers
and devices say "ready", then says "start"; after 30 s it says "stop" and they all exit.

Results: `orchflow/results/live-echo/events-{coord,peers,devices}.jsonl`. Merge them:

```
uv run orchflow merge-logs results/live-echo
```

When it's done: Ctrl+C, then `docker compose down`.

## Without Docker

Start Mosquitto any way you like (e.g. `docker run -d -p 1883:1883 eclipse-mosquitto:2
mosquitto -c /mosquitto-no-auth.conf`), then in three terminals from `orchflow/`:

```
uv run orchflow agent deploy/local/experiment_echo.yaml --nodes devices
uv run orchflow agent deploy/local/experiment_echo.yaml --nodes peers
uv run orchflow agent deploy/local/experiment_echo.yaml --nodes coord
```

Or everything in one process, no broker at all:
`uv run orchflow run deploy/local/experiment_echo.yaml --broker memory`

## Using GOM / LOM later

Change one line in the experiment YAML: `orchestration: {name: gom}`. No code changes.
