"""Storage backend interface.

Harvey splits DR into two halves:
  * the orchestrator (replication.Replicator) moves the VM metadata and drives the failover sequence
    (stop -> final sync -> promote -> start), roles, hooks, RTO/RPO bookkeeping;
  * a StorageBackend moves the disk data and knows how to make the replica volumes usable.

A backend works on all PVCs of one VM at a time (some engines replicate per volume, others per group/namespace).
Every method that changes state must be idempotent: Harvey re-runs steps after partial failures.
"""

from __future__ import annotations

import datetime
from typing import Callable, Dict, List, Optional, Tuple

from ..common import ReplicationError, VolumeStatus
from ..kube import KubeClient


class StorageBackend:
    #: short name used in config (`replication.<name>:`) and in the VM annotation harvey.io/storage-backend
    name: str = ""
    #: CSI driver names whose PVs this backend can replicate
    drivers: Tuple[str, ...] = ()
    #: annotation prefixes on PVCs/VMs that are backend-internal and must not be copied to the other site
    drop_annotation_prefixes: Tuple[str, ...] = ()
    #: wording for the CLI
    engine: str = ""                       # e.g. "NetApp SnapMirror", "Portworx async DR"

    def __init__(self, clusters: Dict[str, KubeClient], settings: dict, timeouts: Dict[str, int],
                 log: Callable[[str], None], sc_map: Optional[Dict[str, str]] = None,
                 cluster_config: Optional[Dict[str, dict]] = None, debug: bool = False):
        """settings = the `replication.<name>:` config section; cluster_config = the per-cluster config dicts
        (for backend-specific per-cluster overrides)."""
        self.k = clusters
        self.settings = settings or {}
        self.timeouts = timeouts
        self.log = log
        self.sc_map = sc_map or {}
        self.cluster_config = cluster_config or {}
        self.debug = debug

    # ── helpers shared by all backends ───────────────────────────────────────
    def check_claims(self, cluster: str, ns: str, claims: List[str]) -> Dict[str, dict]:
        """PVCs of the VM on `cluster`, verified to be bound to a PV of one of our CSI drivers."""
        k = self.k[cluster]
        out = {}
        for c in claims:
            pvc = k.get("pvc", ns, c)
            if pvc is None:
                raise ReplicationError(f"{cluster}: PVC {ns}/{c} not found")
            pv = k.get("pv", None, pvc["spec"]["volumeName"]) if pvc["spec"].get("volumeName") else None
            drv = ((pv or {}).get("spec", {}).get("csi") or {}).get("driver")
            if drv not in self.drivers:
                raise ReplicationError(f"{cluster}: PVC {ns}/{c} is on '{drv}', not {'/'.join(self.drivers)} — "
                                       f"{self.engine} cannot replicate it")
            out[c] = pvc
        return out

    # ── protection ───────────────────────────────────────────────────────────
    def protect(self, src: str, dst: str, ns: str, vm: str, claims: List[str], wait_init: bool) -> List[str]:
        """Start replicating the VM's PVCs src -> dst. When it returns, the destination PVCs exist (bound or about
        to be) so the replica VM can reference them. Returns human-readable action lines."""
        raise NotImplementedError

    def wait_initialized(self, src: str, dst: str, ns: str, vm: str, claims: List[str]):
        """Block until the first full copy (baseline) of every volume is on dst."""
        raise NotImplementedError

    def reprotect(self, new_src: str, new_dst: str, ns: str, vm: str, claims: List[str],
                  wait_init: bool) -> List[str]:
        """After a failover to new_src: replicate new_src -> new_dst, overwriting new_dst's copy."""
        raise NotImplementedError

    def unprotect(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[str]:
        """Stop replicating; the replica volumes stay as independent copies."""
        raise NotImplementedError

    # ── failover ─────────────────────────────────────────────────────────────
    def preflight(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool):
        """Raise ReplicationError unless dst holds replica volumes of this VM that can be promoted."""
        raise NotImplementedError

    def final_sync(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                   not_before: datetime.datetime) -> List[Tuple[str, int, float]]:
        """Planned failover, VM already stopped on src: replicate the last changes and stop scheduled replication.
        Must guarantee the replica's recovery point is not older than `not_before`. Returns
        [(claim, bytes, seconds)]."""
        raise NotImplementedError

    def recovery_point_age(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                           src_up: bool) -> Optional[int]:
        """Unplanned failover: worst-case age (seconds) of the data the replica would start with; None = unknown."""
        raise NotImplementedError

    def promote(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool, unplanned: bool):
        """Make dst's volumes read-write/attachable and make sure the old direction can no longer overwrite
        them (fence)."""
        raise NotImplementedError

    # ── status ───────────────────────────────────────────────────────────────
    def volume_status(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[VolumeStatus]:
        raise NotImplementedError

    def replica_pvc_annotations(self, src_pvc: dict) -> dict:
        """Extra annotations for a destination PVC the orchestrator creates (only used by backends that let the
        orchestrator create PVCs)."""
        return {}
