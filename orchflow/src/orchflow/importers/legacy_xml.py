"""Legacy workflow XML -> WorkflowSpec YAML (Track 1).

Reads the workflow model the legacy coordinator consumed
(``workflow-coordinator/incident-management-workflow-model.xml``) and produces a
``WorkflowSpec``. Run it as::

    python -m orchflow.importers.legacy_xml path/to/workflow.xml -o workflows/imw.yaml
    python -m orchflow.importers.legacy_xml path/to/workflow.xml -o workflows/plume.yaml \\
        --task 1 --period 60 --deadline 58

How the XML maps
----------------
* ``<Task id>``                -> ``TaskSpec.id``; location = its first Gamma's location.
* ``<Gamma>``                  -> ``ServiceRequirement``:
  ``Service_Name``, ``Service_Type`` (sensor / Fog Service / Cloud Service -> kind),
  ``Deployable`` (deployable / non_deployable).
* ``<interaction>``            -> dependencies. Gammas are numbered globally across tasks
  (1..N in document order); an interaction ``Gamma_1 -> Gamma_2`` between two different
  tasks makes the second task depend on the first.
* Artifacts are not in the XML; they come from the legacy ``deployment`` table
  (``db_script/gom.sql``), see ``LEGACY_ARTIFACTS``.

Deliberately NOT imported (the legacy device query ignores them, see docs/deviations.md D14):
``Radius``, ``Hardware_Requirements``, ``Software_Requirements``, ``Availability``.
Periods and deadlines are not in the XML at all (deviations.md P2); pass them explicitly.
"""

from __future__ import annotations

import argparse
import shlex
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml

from orchflow.core.models import Location, ServiceKind
from orchflow.core.spec import ServiceRequirement, TaskSpec, WorkflowSpec

# Deployable service -> jar it ships, from the legacy `deployment` table (db_script/gom.sql).
LEGACY_ARTIFACTS = {
    "plume_service": "data_integration_service.jar",
    "traffic_flow_service": "Traffic_Flow_Finder.jar",
}

_KINDS = {"sensor": ServiceKind.SENSOR, "fog service": ServiceKind.FOG,
          "cloud service": ServiceKind.CLOUD}


def _text(el: ET.Element, path: str) -> str:
    found = el.find(path)
    if found is None or found.text is None:
        raise ValueError(f"<{el.tag} id={el.get('id')}> has no <{path}>")
    return found.text.strip()


def _requirement(gamma: ET.Element) -> ServiceRequirement:
    name = _text(gamma, "Service_Name")
    kind_raw = _text(gamma, "Service_Type").lower()
    if kind_raw not in _KINDS:
        raise ValueError(f"unknown Service_Type {kind_raw!r} for {name}")
    deployable = _text(gamma, "Deployable").lower() == "deployable"
    return ServiceRequirement(
        service_name=name, kind=_KINDS[kind_raw], deployable=deployable,
        artifact=LEGACY_ARTIFACTS.get(name, f"{name}.jar") if deployable else None,
    )


def parse_legacy_workflow(xml: str | Path, *, workflow_id: str = "imw",
                          name: str = "Incident management workflow (legacy IMW)") -> WorkflowSpec:
    """Parse the legacy XML (a path, or the XML text itself)."""
    raw = Path(xml).read_text(encoding="utf-8") if isinstance(xml, Path) else xml
    root = ET.fromstring(raw)

    task_of_gamma: dict[int, str] = {}
    tasks: list[dict[str, Any]] = []
    n = 0
    for task_el in root.iter("Task"):
        gammas = task_el.findall("Gamma")
        if not gammas:
            continue
        task_id = task_el.get("id") or str(len(tasks) + 1)
        first = gammas[0]
        loc = Location(lat=float(_text(first, "Location/Latitude")),
                       lon=float(_text(first, "Location/Longitude")))
        reqs = []
        for g in gammas:
            n += 1
            task_of_gamma[n] = task_id
            reqs.append(_requirement(g))
        tasks.append({"id": task_id, "location": loc, "requirements": reqs,
                      "name": " + ".join(r.service_name for r in reqs), "depends_on": []})

    by_id = {t["id"]: t for t in tasks}
    for inter in root.iter("interaction"):
        src = task_of_gamma.get(int(_text(inter, "Gamma_1")))
        dst = task_of_gamma.get(int(_text(inter, "Gamma_2")))
        if src is None or dst is None:
            raise ValueError(f"interaction {inter.get('id')} names an unknown Gamma")
        if src != dst and src not in by_id[dst]["depends_on"]:
            by_id[dst]["depends_on"].append(src)

    return WorkflowSpec(id=workflow_id, name=name, tasks=[TaskSpec(**t) for t in tasks])


def extract(wf: WorkflowSpec, task_ids: list[str], *, workflow_id: str, name: str,
            period_s: float | None = None, deadline_s: float | None = None,
            parallel_instances: int = 1) -> WorkflowSpec:
    """A workflow made of some of ``wf``'s tasks (dependencies on dropped tasks are removed),
    with timing applied to every kept task. Used for the Table I workloads."""
    keep = set(task_ids)
    tasks = []
    for t in wf.tasks:
        if t.id in keep:
            tasks.append(t.model_copy(update={
                "depends_on": [d for d in t.depends_on if d in keep],
                "period_s": period_s, "deadline_s": deadline_s,
                "parallel_instances": parallel_instances,
            }))
    missing = keep - {t.id for t in tasks}
    if missing:
        raise KeyError(f"no task(s) {sorted(missing)} in workflow {wf.id}")
    return WorkflowSpec(id=workflow_id, name=name, tasks=tasks)


def to_yaml(wf: WorkflowSpec, header: str = "") -> str:
    data = wf.model_dump(mode="json", exclude_defaults=True)
    # requirement defaults (count 1, no radius...) are left out to keep the files readable
    body = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
    return (header + body) if header else body


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m orchflow.importers.legacy_xml",
                                 description="Convert a legacy Orchflow workflow XML to YAML.")
    ap.add_argument("xml", type=Path)
    ap.add_argument("-o", "--output", type=Path, help="write here instead of stdout")
    ap.add_argument("--task", action="append", default=[],
                    help="keep only this task id (repeatable)")
    ap.add_argument("--id", default=None, help="workflow id (default: imw, or the output name)")
    ap.add_argument("--name", default=None)
    ap.add_argument("--period", type=float, default=None, help="period_s for kept tasks")
    ap.add_argument("--deadline", type=float, default=None, help="deadline_s for kept tasks")
    ap.add_argument("--parallel", type=int, default=1, help="parallel_instances for kept tasks")
    args = ap.parse_args(argv)

    wf = parse_legacy_workflow(args.xml)
    if args.task or args.period or args.deadline or args.parallel != 1:
        wid = args.id or (args.output.stem if args.output else "imw")
        wf = extract(wf, args.task or [t.id for t in wf.tasks], workflow_id=wid,
                     name=args.name or wid, period_s=args.period, deadline_s=args.deadline,
                     parallel_instances=args.parallel)
    cmd = shlex.join(["python", "-m", "orchflow.importers.legacy_xml",
                      *(sys.argv[1:] if argv is None else argv)])
    out = to_yaml(wf, header=f"# GENERATED from the legacy workflow XML -- do not edit by hand.\n"
                             f"# {cmd}\n")
    if args.output:
        args.output.write_text(out, encoding="utf-8")
    else:
        sys.stdout.write(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
