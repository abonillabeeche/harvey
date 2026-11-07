"""Shared constants, errors and result types used by the orchestrator and the storage backends."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

L_MANAGED = "harvey.io/managed"          # label on everything Harvey creates/owns
L_VM = "harvey.io/vm"                    # label: VM name the object belongs to
L_ROLE = "harvey.io/role"                # label on VMs: primary | replica
A_PEER = "harvey.io/peer-cluster"        # annotation on VMs: the other site
A_RUN = "harvey.io/run-strategy"         # annotation: runStrategy to restore on start
A_SYNCED = "harvey.io/synced-at"
A_SOURCE = "harvey.io/source-cluster"
A_REPROTECT = "harvey.io/needs-reprotect"
A_BACKEND = "harvey.io/storage-backend"  # annotation on VMs: backend that replicates the disks
A_PRIMARY_SINCE = "harvey.io/primary-since"  # annotation on VMs: when this site became primary (UTC ISO)
L_GROUP = "harvey.io/recovery-group"     # label on VMs and standalone PVCs: recovery group membership (user-set)
A_UNIT = "harvey.io/unit"                # annotation on VMs: the protection unit (group name, or the VM name)
A_UNIT_CLAIMS = "harvey.io/unit-claims"  # annotation on VMs: JSON list of every PVC replicated with the unit

# metadata that must not travel to the other cluster (backends add their own prefixes)
DROP_ANN_PREFIXES = ("kubectl.kubernetes.io/", "kubevirt.io/latest-observed-api-version",
                     "kubevirt.io/storage-observed-api-version", "harvesterhci.io/volumeClaimTemplates",
                     "harvey.io/", "pv.kubernetes.io/", "volume.kubernetes.io/", "volume.beta.kubernetes.io/",
                     "harvesterhci.io/imageId", "cdi.kubevirt.io/")
DROP_LABEL_PREFIXES = ("harvey.io/",)
KEEP_LABELS = ("harvey.io/recovery-group",)   # travels to the other site with the object


class ReplicationError(Exception):
    pass


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def iso(t: Optional[datetime.datetime]) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if t else "-"


def parse_k8s_time(s: Optional[str]) -> Optional[datetime.datetime]:
    """RFC3339 timestamp as written by the API server ('2026-11-06T23:10:00Z')."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        return None


def clean_meta(meta: dict, keep_annotations: bool = True, extra_drop: Iterable[str] = ()) -> dict:
    out = {"name": meta["name"]}
    if meta.get("namespace"):
        out["namespace"] = meta["namespace"]
    labels = {k: v for k, v in (meta.get("labels") or {}).items()
              if k in KEEP_LABELS or not k.startswith(DROP_LABEL_PREFIXES)}
    if labels:
        out["labels"] = labels
    if keep_annotations:
        drop = DROP_ANN_PREFIXES + tuple(extra_drop)
        ann = {k: v for k, v in (meta.get("annotations") or {}).items() if not k.startswith(drop)}
        if ann:
            out["annotations"] = ann
    return out


@dataclass
class VolumeStatus:
    """One replicated volume, as reported by a storage backend.

    src_state / dst_state : the backend's relationship state on each side (e.g. TMR promoted/established,
                            Portworx MigrationSchedule active/suspended)
    repl_state            : state of the replication engine (SnapMirror 'snapmirrored', Stork 'Successful', ...)
    healthy               : True when replication is working, False when broken, None when unknown
    lag_s                 : age of the newest recovery point = worst-case data loss if the primary fails now
    """
    claim: str
    src_handle: str = "-"
    dst_handle: str = "-"
    src_state: str = "-"
    dst_state: str = "-"
    repl_state: str = "-"
    healthy: Optional[bool] = None
    lag_s: Optional[int] = None
    recovery_point: Optional[datetime.datetime] = None
    transfer: str = "-"
    note: str = ""


@dataclass
class FailoverResult:
    stopped: list = field(default_factory=list)
    synced: list = field(default_factory=list)       # (vm, claim, bytes, seconds)
    promoted: list = field(default_factory=list)
    started: list = field(default_factory=list)
    failed: list = field(default_factory=list)       # (vm, phase, error)
    rpo: Dict[str, Optional[int]] = field(default_factory=dict)   # vm -> worst-case data loss seconds (0 = lossless)
    rto_seconds: Optional[int] = None
    phases: Dict[str, int] = field(default_factory=dict)
