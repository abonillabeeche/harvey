"""Portworx backend: Portworx async DR driven through Stork (ClusterPair + SchedulePolicy + MigrationSchedule).

Split of work, same as the NetApp backend: Portworx/Stork moves the disks (and their PV/PVC objects), Harvey moves
the VM spec, Secrets, ConfigMaps and NADs and orchestrates.

Data path (per protected VM; all Stork objects live in the ClusterPair's namespace on the SOURCE cluster):
  ClusterPair     <pair>                 created once per direction (storkctl create clusterpair ...,
                                         with an object store; see docs/portworx.md)
  SchedulePolicy  harvey-<N>min          interval policy (cluster-scoped)
  MigrationSchedule harvey-<vm>          volume-only migration: includeResources=false, includeVolumes=true
                                         -> Stork migrates the volume data plus ONLY the PV/PVC objects,
                                         selected by the label harvey.io/vm=<vm> that Harvey puts on the VM's
                                         PVCs and PVs; startApplications=false; autoSuspend=true
  Stork copies every MigrationSchedule to the destination (suspended, annotation static-copy=true).

  final sync  : suspend the schedule, wait for a running migration, run one more Migration (RPO 0)
  failover    : (source reachable) suspend the schedule; on the destination set the copy's
                status.applicationActivated=true. That is Stork's own "app is active here" flag: with autoSuspend
                the old source suspends itself instead of overwriting the promoted volumes when it comes back.
  reprotect   : delete the old direction's schedule on both sides, create harvey-<vm> on the new primary with
                the reverse ClusterPair.
  recovery point of the replica: creationTimestamp of the last successful Migration (source side), or the
                PVC annotation stork.libopenstorage.org/migrationTime on the destination when the source is down.

Config (`replication.portworx:`):
  interval_minutes: 15                   # SchedulePolicy harvey-<N>min (Portworx recommends >= 15)
  pairs:                                 # ClusterPair per direction, "name" (in the VM's namespace) or "ns/name"
    site-a->site-b: harvey-to-site-b     #   (a pair outside the VM namespace requires that namespace to be
    site-b->site-a: harvey-to-site-a     #    Stork's admin namespace; both directions must use the same namespace)
  default pair name when a direction is not listed: harvey-<destination cluster name>
"""

from __future__ import annotations

import datetime
import re
import time
from typing import Dict, List, Optional, Tuple

from ..common import (L_MANAGED, L_VM, ReplicationError, VolumeStatus, iso, now_utc, parse_k8s_time)
from ..kube import KubeError
from . import register
from .base import StorageBackend

PXD = "pxd.portworx.com"
STORK_API = "stork.libopenstorage.org/v1alpha1"
A_MIGRATED = "stork.libopenstorage.org/migrated"
A_MIGRATION_TIME = "stork.libopenstorage.org/migrationTime"     # Go layout 2006-01-02-150405 (UTC)
A_MIGRATION_NAME = "stork.libopenstorage.org/migrationName"
A_STATIC_COPY = "stork.libopenstorage.org/static-copy"
A_RECLAIM = "stork.libopenstorage.org/reclaimPolicy"
A_PROMOTED = "harvey.io/promoted-at"
L_VM_NS = "harvey.io/vm-namespace"
BUSY = ("", "Pending", "InProgress")
DONE_OK = ("Successful",)
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def stork_time(s: Optional[str]) -> Optional[datetime.datetime]:
    """Parses Stork's name/annotation timestamp '2026-11-06-231500'."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d-%H%M%S").replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        return None


def migration_name_time(name: Optional[str]) -> Optional[datetime.datetime]:
    """Trigger time encoded in a migration name: Stork schedules use '<sched>-interval-2026-11-07-060250',
    Harvey's one-off migrations '<sched>-final-20261107060250'."""
    if not name:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2}-\d{6})$", name)
    if m:
        return stork_time(m.group(1))
    m = re.search(r"(\d{14})$", name)
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            return None
    return None


def schedule_items(sched: Optional[dict]) -> List[dict]:
    """All migrations a MigrationSchedule has started (any policy type), oldest first."""
    items = []
    for lst in (((sched or {}).get("status") or {}).get("items") or {}).values():
        items += lst or []
    return sorted(items, key=lambda i: i.get("creationTimestamp") or "")


def last_successful(sched: Optional[dict]) -> Optional[dict]:
    ok = [i for i in schedule_items(sched) if i.get("status") in DONE_OK]
    return ok[-1] if ok else None


def migration_spec(ns: str, vm: str, pair: str) -> dict:
    """Stork MigrationSpec that moves only the disks (PV + PVC + data) of one VM."""
    return {
        "clusterPair": pair,
        "namespaces": [ns],
        "selectors": {L_VM: vm},
        "includeResources": False,
        "includeVolumes": True,
        "startApplications": False,
        "purgeDeletedResources": False,
        "ignoreOwnerReferencesCheck": True,   # CDI DataVolume-owned PVCs (image-backed root disks)
    }


@register
class PortworxBackend(StorageBackend):
    name = "portworx"
    drivers = (PXD,)
    drop_annotation_prefixes = ("stork.libopenstorage.org/",)
    engine = "Portworx async DR"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.interval = int(self.settings.get("interval_minutes", 15))
        self.pairs: Dict[str, str] = self.settings.get("pairs") or {}

    # ── naming ───────────────────────────────────────────────────────────────
    def pair_ref(self, src: str, dst: str, ns: str) -> Tuple[str, str]:
        """(namespace, name) of the ClusterPair src -> dst; also the namespace of its Migration objects."""
        ref = self.pairs.get(f"{src}->{dst}") or f"harvey-{dst}"
        if "/" in ref:
            pns, name = ref.split("/", 1)
            return pns, name
        return ns, ref

    @staticmethod
    def schedule_name(ns: str, vm: str, pair_ns: str) -> str:
        n = f"harvey-{vm}" if pair_ns == ns else f"harvey-{ns}-{vm}"
        return n[:63].rstrip("-")

    @property
    def policy_name(self) -> str:
        return f"harvey-{self.interval}min"

    def _sched(self, src: str, dst: str, ns: str, vm: str) -> Tuple[str, str, str]:
        """(pair namespace, pair name, schedule name) for the src -> dst direction."""
        pns, pair = self.pair_ref(src, dst, ns)
        return pns, pair, self.schedule_name(ns, vm, pns)

    def _labels(self, ns: str, vm: str) -> dict:
        return {L_MANAGED: "true", L_VM: vm, L_VM_NS: ns}

    # ── building blocks ──────────────────────────────────────────────────────
    def check_pair(self, cluster: str, pns: str, pair: str):
        cp = self.k[cluster].get("clusterpair", pns, pair)
        if cp is None:
            raise ReplicationError(
                f"{cluster}: ClusterPair {pns}/{pair} not found. Create it once per direction, e.g.\n"
                f"    storkctl create clusterpair {pair} -n {pns} --src-kube-file <{cluster}.yaml> "
                f"--dest-kube-file <peer.yaml> --provider s3 --s3-endpoint ... --unidirectional\n"
                f"  or set replication.portworx.pairs in the Harvey config.")
        st = cp.get("status") or {}
        if st.get("schedulerStatus") != "Ready" or st.get("storageStatus") != "Ready":
            raise ReplicationError(f"{cluster}: ClusterPair {pns}/{pair} is not Ready "
                                   f"(scheduler {st.get('schedulerStatus')}, storage {st.get('storageStatus')})")

    def ensure_policy(self, cluster: str):
        k = self.k[cluster]
        if k.get("schedulepolicy", None, self.policy_name) is None:
            k.create("schedulepolicy", None, {
                "apiVersion": STORK_API, "kind": "SchedulePolicy",
                "metadata": {"name": self.policy_name, "labels": {L_MANAGED: "true"}},
                "policy": {"interval": {"intervalMinutes": self.interval}}})

    def label_volumes(self, cluster: str, ns: str, vm: str, claims: List[str]):
        """Stork selects the VM's PVCs and PVs by label (PVs are filtered with the same selector)."""
        k = self.k[cluster]
        for c in claims:
            pvc = k.merge_patch("pvc", ns, c, {"metadata": {"labels": {L_VM: vm}}})
            if pvc["spec"].get("volumeName"):
                k.merge_patch("pv", None, pvc["spec"]["volumeName"], {"metadata": {"labels": {L_VM: vm}}})

    def set_activated(self, cluster: str, pns: str, sched: str, active: bool, suspend: Optional[bool] = None):
        """Stork's per-site 'application is active here' flag (status.applicationActivated, no status
        subresource on Stork CRDs) — the fence used by autoSuspend."""
        k = self.k[cluster]
        cur = k.get("migrationschedule", pns, sched)
        if cur is None:
            return False
        patch: dict = {"status": {"applicationActivated": active}}
        if suspend is not None:
            patch["spec"] = {"suspend": suspend}
        k.merge_patch("migrationschedule", pns, sched, patch)
        return True

    def suspend(self, cluster: str, pns: str, sched: str) -> bool:
        k = self.k[cluster]
        if k.get("migrationschedule", pns, sched) is None:
            return False
        k.merge_patch("migrationschedule", pns, sched, {"spec": {"suspend": True}})
        return True

    def _running_migrations(self, cluster: str, pns: str, sched: str) -> List[dict]:
        k = self.k[cluster]
        out = []
        for i in schedule_items(k.get("migrationschedule", pns, sched)):
            if i.get("status") in BUSY:
                m = k.get("migration", pns, i["name"])
                if m is not None and ((m.get("status") or {}).get("status") or "") in BUSY:
                    out.append(m)
        return out

    def wait_idle(self, cluster: str, pns: str, sched: str, timeout_s: int, cancel: bool = False):
        """Wait until no migration of this schedule is running; with cancel=True delete the ones still running
        at the deadline (Stork cancels the volume migration in the Migration finalizer)."""
        k = self.k[cluster]
        deadline = time.time() + timeout_s
        while True:
            running = self._running_migrations(cluster, pns, sched)
            if not running:
                return
            if time.time() > deadline:
                if not cancel:
                    raise ReplicationError(f"{cluster}: migration {running[0]['metadata']['name']} still running "
                                           f"after {timeout_s}s")
                for m in running:
                    self.log(f"  [yellow]cancelling running migration {m['metadata']['name']}[/yellow]")
                    k.delete("migration", pns, m["metadata"]["name"])
                k.wait_for(lambda: not self._running_migrations(cluster, pns, sched), 120,
                           what="cancelled migrations to go away")
                return
            time.sleep(5)

    def run_migration(self, cluster: str, pns: str, pair: str, ns: str, vm: str, tag: str) -> dict:
        """One-off Migration with the schedule's spec; waits for it and returns the finished object."""
        k = self.k[cluster]
        name = f"{self.schedule_name(ns, vm, pns)}-{tag}-{now_utc().strftime('%Y%m%d%H%M%S')}"[:63].rstrip("-")
        k.create("migration", pns, {"apiVersion": STORK_API, "kind": "Migration",
                                    "metadata": {"name": name, "namespace": pns, "labels": self._labels(ns, vm)},
                                    "spec": migration_spec(ns, vm, pair)})
        return self.wait_migration(cluster, pns, name)

    def wait_migration(self, cluster: str, pns: str, name: str) -> dict:
        k = self.k[cluster]
        deadline = time.time() + self.timeouts["transfer"]
        while time.time() < deadline:
            m = k.get("migration", pns, name)
            st = (m or {}).get("status") or {}
            if st.get("stage") == "Final" or st.get("status") in ("Failed", "PartialSuccess", "Successful"):
                if st.get("status") in DONE_OK:
                    return m
                reasons = "; ".join(f"{v.get('persistentVolumeClaim')}: {v.get('reason')}"
                                    for v in st.get("volumes") or [] if v.get("status") not in DONE_OK)
                raise ReplicationError(f"{cluster}: migration {pns}/{name} {st.get('status')}"
                                       + (f" ({reasons})" if reasons else ""))
            time.sleep(5)
        raise ReplicationError(f"{cluster}: migration {pns}/{name} not finished after {self.timeouts['transfer']}s")

    def wait_first_success(self, cluster: str, pns: str, sched: str, after: datetime.datetime) -> dict:
        """Waits for a successful scheduled migration started at/after `after` (the baseline)."""
        k = self.k[cluster]
        deadline = time.time() + self.timeouts["transfer"]
        started = time.time()
        while time.time() < deadline:
            items = [i for i in schedule_items(k.get("migrationschedule", pns, sched))
                     if (parse_k8s_time(i.get("creationTimestamp")) or after) >= after.replace(microsecond=0)]
            for i in reversed(items):
                if i.get("status") in DONE_OK:
                    return k.get("migration", pns, i["name"]) or i
                if i.get("status") in ("Failed", "PartialSuccess"):
                    m = k.get("migration", pns, i["name"]) or {}
                    reasons = "; ".join(f"{v.get('persistentVolumeClaim')}: {v.get('reason')}"
                                        for v in (m.get("status") or {}).get("volumes") or []
                                        if v.get("status") not in DONE_OK)
                    raise ReplicationError(f"{cluster}: migration {i['name']} {i['status']}"
                                           + (f" ({reasons})" if reasons else ""))
                break   # newest one is still running
            if not items and time.time() - started > 90:
                # the schedule did not start one by itself: kick one off with the same spec
                sch = k.get("migrationschedule", pns, sched)
                pair = sch["spec"]["template"]["spec"]["clusterPair"]
                ns = sch["spec"]["template"]["spec"]["namespaces"][0]
                vm = sch["spec"]["template"]["spec"]["selectors"][L_VM]
                return self.run_migration(cluster, pns, pair, ns, vm, "initial")
            time.sleep(5)
        raise ReplicationError(f"{cluster}: no successful migration of {pns}/{sched} after "
                               f"{self.timeouts['transfer']}s")

    def _create_schedule(self, cluster: str, pns: str, sched: str, pair: str, ns: str, vm: str) -> str:
        k = self.k[cluster]
        body = {"apiVersion": STORK_API, "kind": "MigrationSchedule",
                "metadata": {"name": sched, "namespace": pns, "labels": self._labels(ns, vm)},
                "spec": {"template": {"spec": migration_spec(ns, vm, pair)},
                         "schedulePolicyName": self.policy_name, "suspend": False, "autoSuspend": True}}
        cur = k.get("migrationschedule", pns, sched)
        if cur is not None and (cur["metadata"].get("annotations") or {}).get(A_STATIC_COPY):
            # a DR-side copy left from the other direction: replace it with a real schedule
            k.delete("migrationschedule", pns, sched)
            k.wait_for(lambda: k.get("migrationschedule", pns, sched) is None, 120, what=f"delete copy {sched}")
            cur = None
        if cur is None:
            k.create("migrationschedule", pns, body)
            return "created"
        k.merge_patch("migrationschedule", pns, sched, {"spec": body["spec"], "metadata": {"labels": body["metadata"]["labels"]}})
        return "updated"

    def _delete_schedule(self, cluster: str, pns: str, sched: str) -> bool:
        k = self.k[cluster]
        if k.get("migrationschedule", pns, sched) is None:
            return False
        k.delete("migrationschedule", pns, sched)
        k.wait_for(lambda: k.get("migrationschedule", pns, sched) is None, 120, what=f"delete {sched}")
        return True

    @staticmethod
    def _migrated_bytes(m: dict) -> Dict[str, int]:
        return {v.get("persistentVolumeClaim"): int(v.get("bytesTotal") or 0)
                for v in ((m or {}).get("status") or {}).get("volumes") or []}

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, src: str, dst: str, ns: str, vm: str, claims: List[str], wait_init: bool) -> List[str]:
        ks, kd = self.k[src], self.k[dst]
        self.check_claims(src, ns, claims)
        pns, pair, sched = self._sched(src, dst, ns, vm)
        self.check_pair(src, pns, pair)
        for c in claims:
            d = kd.get("pvc", ns, c)
            if d is not None and (d["metadata"].get("annotations") or {}).get(A_MIGRATED) != "true":
                raise ReplicationError(f"{dst}: PVC {ns}/{c} already exists and is not a Stork-migrated copy; "
                                       f"refusing to overwrite it")
        out = []
        self.label_volumes(src, ns, vm, claims)
        out.append(f"{src}: labelled {len(claims)} PVC(s)+PV(s) {L_VM}={vm}")
        self.ensure_policy(src)
        self.ensure_policy(dst)        # the DR-side copy of the schedule references it too
        out.append(f"{src}: migrationschedule {pns}/{sched} {self._create_schedule(src, pns, sched, pair, ns, vm)} "
                   f"(pair {pair}, every {self.interval} min)")
        # this site runs the VM: its schedule carries the 'active' flag (fences a schedule pointing at it)
        self.set_activated(src, pns, sched, True)
        self.log(f"  waiting for the baseline migration {src} → {dst} ...")
        # any successful migration of this schedule is a valid baseline (re-runs adopt one already in flight)
        m = self.wait_first_success(src, pns, sched, EPOCH)
        sizes = self._migrated_bytes(m)
        for c in claims:
            out.append(f"{dst}: pvc/{c} migrated ({sizes.get(c, 0)} B)")
        missing = [c for c in claims if kd.get("pvc", ns, c) is None]
        if missing:
            raise ReplicationError(f"{dst}: migration finished but PVC(s) {missing} are missing — are their PVs "
                                   f"labelled {L_VM}={vm} and Bound on {src}?")
        return out

    def wait_initialized(self, src: str, dst: str, ns: str, vm: str, claims: List[str]):
        pns, _, sched = self._sched(src, dst, ns, vm)
        if last_successful(self.k[src].get("migrationschedule", pns, sched)) is None:
            self.wait_first_success(src, pns, sched, EPOCH)

    # ── status ───────────────────────────────────────────────────────────────
    def volume_status(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[VolumeStatus]:
        kp, kr = self.k[primary], self.k[replica]
        pns, pair, sched = self._sched(primary, replica, ns, vm)
        s = kp.get("migrationschedule", pns, sched)
        copy = kr.get("migrationschedule", pns, sched) if kr else None
        items = schedule_items(s)
        last = items[-1] if items else None
        ok = last_successful(s)
        rp = parse_k8s_time((ok or {}).get("creationTimestamp"))
        last_m = kp.get("migration", pns, ok["name"]) if ok else None
        sizes = self._migrated_bytes(last_m)
        if s is None:
            src_state = "missing"
        elif (s["metadata"].get("annotations") or {}).get(A_STATIC_COPY):
            # only Stork's copy of the old direction is here: this site was failed over to and is fenced
            src_state = "fence" if (s.get("status") or {}).get("applicationActivated") else "copy"
        else:
            src_state = "suspended" if (s.get("spec") or {}).get("suspend") else "active"
        if copy is None:
            dst_state = "-"
        elif not (copy["metadata"].get("annotations") or {}).get(A_STATIC_COPY):
            # the replica site still has its own schedule of the OLD direction (unplanned failover, not reprotected)
            dst_state = "old-dir " + ("suspended" if (copy.get("spec") or {}).get("suspend") else "ACTIVE")
        else:
            dst_state = "fenced" if (copy.get("status") or {}).get("applicationActivated") else "standby"
        rows = []
        for c in claims:
            vs = VolumeStatus(claim=c, src_state=src_state, dst_state=dst_state)
            pvc = kp.get("pvc", ns, c)
            rpvc = kr.get("pvc", ns, c) if kr else None
            vs.src_handle = ((pvc or {}).get("spec") or {}).get("volumeName", "-")
            vs.dst_handle = ((rpvc or {}).get("spec") or {}).get("volumeName", "-")
            vs.repl_state = (last or {}).get("status") or "-"
            vs.recovery_point = rp
            vs.lag_s = int((now_utc() - rp).total_seconds()) if rp else None
            vs.transfer = "running" if last and last.get("status") in BUSY else "idle"
            vs.healthy = bool(s is not None and src_state == "active" and ok is not None
                              and (last or {}).get("status") not in ("Failed", "PartialSuccess")
                              and rpvc is not None)
            if c in sizes:
                vs.note = f"{sizes[c] // (1 << 20)} MiB"
            if s is None:
                vs.note = f"no MigrationSchedule {pns}/{sched} on {primary}"
            elif rpvc is None:
                vs.note = f"no replica PVC on {replica} yet"
            elif last and last.get("status") in ("Failed", "PartialSuccess"):
                vs.note = f"last migration {last.get('name')} {last.get('status')}"
            rows.append(vs)
        return rows

    # ── failover ─────────────────────────────────────────────────────────────
    def preflight(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool):
        kd = self.k[dst]
        for c in claims:
            p = kd.get("pvc", ns, c)
            if p is None:
                raise ReplicationError(f"{dst}: replica PVC {ns}/{c} missing — has the first migration finished?")
            if (p["metadata"].get("annotations") or {}).get(A_MIGRATED) != "true":
                raise ReplicationError(f"{dst}: PVC {ns}/{c} is not a Stork-migrated replica")
            if (p.get("status") or {}).get("phase") != "Bound":
                raise ReplicationError(f"{dst}: replica PVC {ns}/{c} is {(p.get('status') or {}).get('phase')}")
        if src_up:
            pns, _, sched = self._sched(src, dst, ns, vm)
            if self.k[src].get("migrationschedule", pns, sched) is None:
                raise ReplicationError(f"{src}: no MigrationSchedule {pns}/{sched} — is {ns}/{vm} protected in "
                                       f"the {src} -> {dst} direction?")

    def final_sync(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                   not_before: datetime.datetime) -> List[Tuple[str, int, float]]:
        pns, pair, sched = self._sched(src, dst, ns, vm)
        t0 = time.time()
        self.suspend(src, pns, sched)
        self.wait_idle(src, pns, sched, self.timeouts["transfer"])
        m = self.run_migration(src, pns, pair, ns, vm, "final")
        started = parse_k8s_time(m["metadata"].get("creationTimestamp"))
        if started and started < not_before.replace(microsecond=0):
            raise ReplicationError(f"final migration started {iso(started)}, before the stop time {iso(not_before)}")
        secs = time.time() - t0
        sizes = self._migrated_bytes(m)
        return [(c, sizes.get(c, 0), secs) for c in claims]

    def recovery_point_age(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                           src_up: bool) -> Optional[int]:
        if src_up:
            pns, _, sched = self._sched(src, dst, ns, vm)
            ok = last_successful(self.k[src].get("migrationschedule", pns, sched))
            rp = parse_k8s_time((ok or {}).get("creationTimestamp"))
            if rp:
                return int((now_utc() - rp).total_seconds())
        # source down: use what Stork wrote on the replica PVC. The migration NAME carries the trigger time
        # (= when the source snapshot was taken); migrationTime is when Stork applied the PVC after copying the
        # data, which would under-estimate the loss by the transfer time.
        worst = 0
        for c in claims:
            p = self.k[dst].get("pvc", ns, c)
            ann = (p or {}).get("metadata", {}).get("annotations") or {}
            t = migration_name_time(ann.get(A_MIGRATION_NAME)) or stork_time(ann.get(A_MIGRATION_TIME))
            if not t:
                return None
            worst = max(worst, int((now_utc() - t).total_seconds()))
        return worst

    def promote(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool, unplanned: bool):
        kd = self.k[dst]
        pns, _, sched = self._sched(src, dst, ns, vm)
        if src_up:
            try:
                self.suspend(src, pns, sched)
                # planned: final_sync already waited; unplanned: give a running migration a moment, then cancel it
                self.wait_idle(src, pns, sched, self.timeouts["tmr"] if unplanned else 60, cancel=True)
                self.set_activated(src, pns, sched, False)
            except KubeError as e:
                self.log(f"  [yellow]{src}: could not suspend {pns}/{sched} ({e})[/yellow]")
        # fence: mark the app active on dst. Stork's autoSuspend on the old source suspends its schedule instead of
        # migrating over the promoted volumes when that site comes back.
        if not self.set_activated(dst, pns, sched, True, suspend=True):
            self.log(f"  [yellow]{dst}: no Stork copy of {pns}/{sched}; the old source is not fenced — suspend or "
                     f"delete that schedule before {src} comes back (harvey reprotect does it)[/yellow]")
        stamp = iso(now_utc())
        for c in claims:
            p = kd.get("pvc", ns, c)
            if p is None or (p.get("status") or {}).get("phase") != "Bound":
                raise ReplicationError(f"{dst}: PVC {ns}/{c} not Bound")
            kd.merge_patch("pvc", ns, c, {"metadata": {"annotations": {A_PROMOTED: stamp}}})
        if not src_up:
            self.log(f"  [yellow]{src} unreachable: its schedule {pns}/{sched} is still enabled there; with "
                     f"autoSuspend it suspends itself when it sees {dst} active. Run harvey reprotect once "
                     f"{src} is back.[/yellow]")

    # ── reprotect / unprotect ────────────────────────────────────────────────
    def reprotect(self, new_src: str, new_dst: str, ns: str, vm: str, claims: List[str],
                  wait_init: bool) -> List[str]:
        ks, kd = self.k[new_src], self.k[new_dst]
        out = []
        self.check_claims(new_src, ns, claims)
        pns, pair, sched = self._sched(new_src, new_dst, ns, vm)
        old_pns, _, old_sched = self._sched(new_dst, new_src, ns, vm)
        self.check_pair(new_src, pns, pair)
        # 1. retire the old direction everywhere (its schedule on the old primary + Stork's copy here)
        if self._delete_schedule(new_dst, old_pns, old_sched):
            out.append(f"{new_dst}: migrationschedule {old_pns}/{old_sched} deleted (old direction)")
        if (old_pns, old_sched) != (pns, sched) and self._delete_schedule(new_src, old_pns, old_sched):
            out.append(f"{new_src}: migrationschedule {old_pns}/{old_sched} (copy) deleted")
        # 2. new direction
        self.label_volumes(new_src, ns, vm, claims)
        self.ensure_policy(new_src)
        self.ensure_policy(new_dst)
        t0 = now_utc()
        out.append(f"{new_src}: migrationschedule {pns}/{sched} {self._create_schedule(new_src, pns, sched, pair, ns, vm)} "
                   f"(pair {pair})")
        self.set_activated(new_src, pns, sched, True)
        self.log(f"  waiting for the first reverse migration {new_src} → {new_dst} ...")
        m = self.wait_first_success(new_src, pns, sched, t0)
        sizes = self._migrated_bytes(m)
        out += [f"{new_dst}: pvc/{c} overwritten from {new_src} ({sizes.get(c, 0)} B)" for c in claims]
        for c in claims:   # the old 'promoted' marker is history now
            if kd.get("pvc", ns, c) is not None:
                kd.merge_patch("pvc", ns, c, {"metadata": {"annotations": {A_PROMOTED: None}}})
        return out

    def unprotect(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[str]:
        out = []
        pns, _, sched = self._sched(primary, replica, ns, vm)
        if self._delete_schedule(primary, pns, sched):
            out.append(f"{primary}: migrationschedule {pns}/{sched} deleted")
        if self.k[replica] and self._delete_schedule(replica, pns, sched):
            out.append(f"{replica}: migrationschedule {pns}/{sched} (copy) deleted")
        return out

    def delete_replica_volumes(self, replica: str, ns: str, claims: List[str]) -> List[str]:
        """Stork creates destination PVs with reclaimPolicy Retain; restore the original policy first so the
        Portworx volume goes away with the PVC."""
        k = self.k[replica]
        out = []
        for c in claims:
            pvc = k.get("pvc", ns, c)
            if pvc is None:
                continue
            pvn = pvc["spec"].get("volumeName")
            pv = k.get("pv", None, pvn) if pvn else None
            if pv is not None:
                orig = (pv["metadata"].get("annotations") or {}).get(A_RECLAIM, "Delete")
                k.merge_patch("pv", None, pvn, {"spec": {"persistentVolumeReclaimPolicy": orig}})
            k.delete("pvc", ns, c)
            out.append(f"{replica}: pvc/{c} deleted")
        return out
