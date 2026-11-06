"""TRACK 3 owns this file. Stub so `orchflow` exists from day one.

Target commands:
    orchflow run experiments/table1_plume_gom.yaml [--seeds 100]
    orchflow table1 results/<run-folder>
    orchflow import-legacy path/to/workflow.xml -o workflows/imw.yaml  (Track 1 writes the importer)
    orchflow plugins                                                 (list registered plug-ins)
"""

from __future__ import annotations

import argparse
import sys

from orchflow.core import plugins


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="orchflow")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plugins", help="list registered plug-ins")
    for name in ("run", "table1", "import-legacy"):
        sub.add_parser(name, help="not implemented yet")
    args = parser.parse_args(argv)

    if args.cmd == "plugins":
        for kind in plugins.PLUGIN_KINDS:
            print(f"{kind:14} {', '.join(plugins.available(kind)) or '-'}")
        return 0
    print(f"`orchflow {args.cmd}` is not implemented yet", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
