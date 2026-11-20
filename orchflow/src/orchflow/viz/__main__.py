"""python -m orchflow.viz EXPERIMENT.yaml [--broker host:port] [--port 8050] [--map osm]
[--plume computed]"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="orchflow dashboard",
                                 description="Live map + task status for a run.")
    ap.add_argument("experiment", type=Path)
    ap.add_argument("--broker", default="memory",
                    help="'memory' (default): run the experiment here too; "
                         "or host:port of the run's Mosquitto broker to watch it")
    ap.add_argument("--port", type=int, default=8050)
    ap.add_argument("--duration", type=float, help="override duration_s (memory mode)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--map", choices=["google", "osm"], default="google",
                    help="google (default, like legacy; set ORCHFLOW_GOOGLE_MAPS_KEY to drop the "
                         "'development purposes only' overlay) or osm (OpenStreetMap)")
    ap.add_argument("--plume", choices=["aloha", "computed"], default="aloha",
                    help="aloha (default, legacy's precomputed ALOHA zones) or computed "
                         "(Gaussian plume from each instance's readings)")
    args = ap.parse_args(argv)

    from orchflow.viz.server import run_dashboard

    if sys.platform == "win32":  # aiomqtt needs the selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_dashboard(args.experiment, broker=args.broker, port=args.port,
                                  duration_s=args.duration, seed=args.seed,
                                  map_provider=args.map, plume_mode=args.plume))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
