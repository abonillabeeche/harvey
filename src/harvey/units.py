"""Protection units: what Harvey replicates and fails over as one.

A unit is either a **recovery group** (VMs, and optionally standalone PVCs, labelled
`harvey.io/recovery-group=<name>` in one namespace) or a single ungrouped VM (a group of one).
Storage is protected, synced, promoted and reprotected per unit, so:

  * a disk shared by several VMs is replicated once and promoted once, after ALL of them stopped;
  * PVCs that no VM uses can be protected by putting them in a group;
  * Portworx gets one MigrationSchedule (one recovery point) for the whole unit.

The guard: if a VM shares a disk with a VM outside its unit, every operation refuses, because failing over
one of them alone would split-brain the shared disk (or break the other VM's protection).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .common import A_UNIT_CLAIMS, L_GROUP, ReplicationError
from .kube import KubeClient


def vm_claims(vm: dict) -> List[str]:
    claims = []
    for v in vm["spec"]["template"]["spec"].get("volumes", []):
        if "persistentVolumeClaim" in v:
            claims.append(v["persistentVolumeClaim"]["claimName"])
        elif "dataVolume" in v:
            claims.append(v["dataVolume"]["name"])
    return claims


@dataclass
class Unit:
    ns: str
    name: str                       # group name, or the VM name for an ungrouped VM
    group: Optional[str]            # None for an ungrouped VM
    vms: List[str] = field(default_factory=list)
    claims: List[str] = field(default_factory=list)       # every PVC replicated with the unit, ordered, unique
    standalone: List[str] = field(default_factory=list)   # claims no member VM uses

    @property
    def key(self) -> Tuple[str, str]:
        return self.ns, self.name

    def describe(self) -> str:
        if not self.group:
            return f"{self.ns}/{self.name}"
        extra = f" + {len(self.standalone)} standalone PVC(s)" if self.standalone else ""
        return f"{self.ns}/{self.group} (recovery group: {', '.join(self.vms)}{extra})"


def _ordered_union(*lists: List[str]) -> List[str]:
    out: List[str] = []
    for lst in lists:
        for x in lst:
            if x not in out:
                out.append(x)
    return out


def group_of(vm: dict) -> Optional[str]:
    return (vm["metadata"].get("labels") or {}).get(L_GROUP)


def resolve_unit(k: KubeClient, ns: str, vm_name: Optional[str] = None, group: Optional[str] = None,
                 cluster: str = "") -> Unit:
    """The unit of a VM (or of a named group) as seen on cluster `k`. Raises if a disk is shared with a VM
    outside the unit. Works on either site: replica VMs carry the group label and the unit's claim list."""
    vms = {v["metadata"]["name"]: v for v in k.list("vm", ns)}
    if group is None:
        if vm_name not in vms:
            raise ReplicationError(f"VM {ns}/{vm_name} not found on {cluster or k.name}")
        group = group_of(vms[vm_name])
    if group:
        members = sorted(n for n, v in vms.items() if group_of(v) == group)
        if not members and vm_name is None:
            pvcs_only = [p for p in k.list("pvc", ns) if (p["metadata"].get("labels") or {}).get(L_GROUP) == group]
            if not pvcs_only:
                raise ReplicationError(f"recovery group {ns}/{group} has no members on {cluster or k.name}")
        name = group
    else:
        members, name = [vm_name], vm_name
    from_vms: List[str] = []
    recorded: List[str] = []
    for m in members:
        from_vms = _ordered_union(from_vms, vm_claims(vms[m]))
        ann = (vms[m]["metadata"].get("annotations") or {}).get(A_UNIT_CLAIMS)
        if ann:
            try:
                recorded = _ordered_union(recorded, json.loads(ann))
            except ValueError:
                pass
    labelled: List[str] = []
    if group:
        labelled = [p["metadata"]["name"] for p in k.list("pvc", ns)
                    if (p["metadata"].get("labels") or {}).get(L_GROUP) == group]
    claims = _ordered_union(from_vms, sorted(labelled), recorded)
    unit = Unit(ns=ns, name=name, group=group or None, vms=members, claims=claims,
                standalone=[c for c in claims if c not in from_vms])
    check_shared_disks(vms, unit, cluster or k.name)
    return unit


def check_shared_disks(vms: Dict[str, dict], unit: Unit, cluster: str):
    """Refuse when a unit's disk is also used by a VM outside the unit."""
    clashes: Dict[str, List[str]] = {}
    for n, v in vms.items():
        if n in unit.vms:
            continue
        for c in vm_claims(v):
            if c in unit.claims:
                clashes.setdefault(n, []).append(c)
    if not clashes:
        return
    others = ", ".join(f"{n} (shares {', '.join(cs)})" for n, cs in sorted(clashes.items()))
    everyone = " ".join(sorted(set(unit.vms) | set(clashes)))
    raise ReplicationError(
        f"{cluster}: {unit.describe()} shares disks with VMs outside it: {others}. VMs that share a disk must be "
        f"protected and failed over together, in one recovery group:\n"
        f"    harvey group add <name> -n {unit.ns} {everyone}")
