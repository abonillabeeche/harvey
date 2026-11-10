"""NetApp backend: ONTAP SnapMirror driven through Trident TridentMirrorRelationships (TMRs).

Data path (per PVC of a protected VM):
  source cluster : TMR <pvc>  state=promoted     -> exposes the ONTAP volume handle (svm:volume)
  dest cluster   : TMR <pvc>  state=established  remoteVolumeHandle=<source handle>, policy + schedule
                   PVC <pvc>  annotation trident.netapp.io/mirrorRelationship=<pvc>  (a DP volume on the peered SVM)
  failover       : dest TMR -> promoted (SnapMirror break, volume becomes RW)
  reprotect      : old-source TMR -> reestablished with remoteVolumeHandle=<new source handle> (reverse resync)

Config (`replication.netapp:`; the old top-level `replication.policy/schedule` keys still work):
  policy: MirrorAllSnapshots      # async SnapMirror policy
  schedule: 5min                  # ONTAP job schedule on the destination
Per cluster (optional): `ontap: {host, username, password}`; default = discovered from Trident's backend + Secret.
"""

from __future__ import annotations

import base64
import datetime
import time
from typing import Dict, List, Optional, Tuple

from ..common import L_MANAGED, L_VM, ReplicationError, VolumeStatus, clean_meta, iso, now_utc
from ..kube import KubeError
from ..ontap import OntapClient, OntapError, StalledTransfer, parse_iso_duration, snapshot_time
from . import register
from .base import StorageBackend

TRIDENT = "csi.trident.netapp.io"
ANN_MIRROR = "trident.netapp.io/mirrorRelationship"
_BUSY = ("transferring", "queued", "preparing", "finalizing")


class TmrWaitError(ReplicationError):
    """Some TMRs failed or didn't reach the wanted state; `failed` = names whose status is 'failed'."""

    def __init__(self, message: str, failed: List[str]):
        super().__init__(message)
        self.failed = failed


def tmr_condition(tmr: Optional[dict]) -> dict:
    if not tmr:
        return {}
    conds = (tmr.get("status") or {}).get("conditions") or []
    return conds[0] if conds else {}


@register
class NetAppBackend(StorageBackend):
    name = "netapp"
    drivers = (TRIDENT,)
    drop_annotation_prefixes = ("trident.netapp.io/",)
    engine = "NetApp SnapMirror"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.policy = self.settings.get("policy", "MirrorAllSnapshots")
        self.schedule = self.settings.get("schedule", "5min")
        self.ontap_overrides = {n: c["ontap"] for n, c in self.cluster_config.items()
                                if isinstance(c, dict) and c.get("ontap")}
        self._ontap_cache: Dict[Tuple[str, str], OntapClient] = {}

    def _labels(self, vm: str) -> dict:
        return {L_MANAGED: "true", L_VM: vm}

    # ── ONTAP discovery ──────────────────────────────────────────────────────
    def ontap_for_claim(self, cluster: str, ns: str, claim: str) -> OntapClient:
        """ONTAP client for the backend that hosts this PVC, from Trident's own backend config + Secret."""
        if cluster in self.ontap_overrides:
            o = self.ontap_overrides[cluster]
            key = (cluster, o["host"])
            if key not in self._ontap_cache:
                self._ontap_cache[key] = OntapClient(o["host"], o["username"], o["password"], debug=self.debug)
            return self._ontap_cache[key]
        k = self.k[cluster]
        pvc = k.get("pvc", ns, claim)
        if not pvc or not pvc["spec"].get("volumeName"):
            raise ReplicationError(f"{cluster}: PVC {ns}/{claim} isn't bound to a volume yet.")
        tvol = k.get("tvol", "trident", pvc["spec"]["volumeName"])
        if not tvol:
            raise ReplicationError(f"{cluster}: no TridentVolume for {pvc['spec']['volumeName']}")
        buuid = tvol["backendUUID"]
        key = (cluster, buuid)
        if key in self._ontap_cache:
            return self._ontap_cache[key]
        tbc = next((t for t in k.list("tbc", "trident")
                    if (t.get("status") or {}).get("backendInfo", {}).get("backendUUID") == buuid), None)
        if not tbc:
            raise ReplicationError(f"{cluster}: no TridentBackendConfig for backend {buuid}")
        sec = k.get("secret", "trident", tbc["spec"]["credentials"]["name"])
        user = base64.b64decode(sec["data"]["username"]).decode()
        pw = base64.b64decode(sec["data"]["password"]).decode()
        host = tbc["spec"]["managementLIF"]
        self._ontap_cache[key] = OntapClient(host, user, pw, debug=self.debug)
        return self._ontap_cache[key]

    def _wait_tmr(self, cluster: str, ns: str, name: str, states: Tuple[str, ...], need_handle: bool = True) -> dict:
        return self._wait_tmrs(cluster, ns, [name], states, need_handle)[name]

    def _wait_tmrs(self, cluster: str, ns: str, names: List[str], states: Tuple[str, ...],
                   need_handle: bool = True, interval: float = 3.0) -> Dict[str, dict]:
        """Waits until every TMR in `names` reports one of `states`; returns {name: condition}.

        Scales with the number of disks: one LIST of the namespace's TMRs per poll (not one GET per TMR), so a
        recovery group or a namespace with hundreds of disks costs the same number of API calls as one disk.
        Fails fast: a TMR whose status is 'failed' for the current spec (observedGeneration caught up) won't
        recover on its own, so there's no point in waiting for the timeout. Every failed or late TMR is reported,
        one line per disk, with Trident's message."""
        k = self.k[cluster]
        pending, done, failed = list(dict.fromkeys(names)), {}, {}
        conds: Dict[str, dict] = {}
        deadline = time.time() + self.timeouts["tmr"]
        while True:
            try:
                if len(pending) > 1:
                    objs = {o["metadata"]["name"]: o for o in k.list("tmr", ns)}
                else:
                    objs = {pending[0]: k.get("tmr", ns, pending[0])}
            except KubeError:
                objs = None   # transient API error: keep the last known conditions and poll again
            for n in list(pending):
                if objs is None:
                    break
                o = objs.get(n)
                c = conds[n] = tmr_condition(o)
                if c.get("state") in states and (c.get("localVolumeHandle") or not need_handle):
                    done[n] = c
                    pending.remove(n)
                elif c.get("state") == "failed" and \
                        int(c.get("observedGeneration") or 0) >= int((o or {}).get("metadata", {}).get("generation") or 0):
                    failed[n] = c
                    pending.remove(n)
            if not pending or time.time() >= deadline:
                break
            time.sleep(interval)
        if failed or pending:
            want = "/".join(states)
            lines = [f"{cluster}: tmr/{n} failed: {failed[n].get('message') or 'no message'}"
                     + (f" (remote {failed[n]['remoteVolumeHandle']})" if failed[n].get("remoteVolumeHandle") else "")
                     for n in failed]
            lines += [f"{cluster}: tmr/{n} still '{conds.get(n, {}).get('state', 'missing')}' after "
                      f"{self.timeouts['tmr']}s (waiting for {want})"
                      + (f": {conds[n]['message']}" if conds.get(n, {}).get("message") else "") for n in pending]
            raise TmrWaitError("; ".join(lines), list(failed))
        return done

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, src: str, dst: str, ns: str, vm: str, claims: List[str], wait_init: bool) -> List[str]:
        ks, kd = self.k[src], self.k[dst]
        src_pvcs = self.check_claims(src, ns, claims)
        out = []

        # 1. source TMRs -> volume handles
        for c in claims:
            t = ks.get("tmr", ns, c)
            if t is None:
                ks.create("tmr", ns, {"apiVersion": "trident.netapp.io/v1", "kind": "TridentMirrorRelationship",
                                      "metadata": {"name": c, "namespace": ns, "labels": self._labels(vm)},
                                      "spec": {"state": "promoted", "volumeMappings": [{"localPVCName": c}]}})
                out.append(f"{src}: tmr/{c} created (promoted)")
            elif t["spec"].get("state") != "promoted":
                raise ReplicationError(f"{src}: tmr/{c} is '{t['spec'].get('state')}', so {src} isn't the primary "
                                       f"for this volume.")
        handles = {c: cond["localVolumeHandle"] for c, cond in self._wait_tmrs(src, ns, claims, ("promoted",)).items()}

        # 2. destination TMRs + PVCs
        for c in claims:
            spvc = src_pvcs[c]
            if kd.get("tmr", ns, c) is None:
                kd.create("tmr", ns, {
                    "apiVersion": "trident.netapp.io/v1", "kind": "TridentMirrorRelationship",
                    "metadata": {"name": c, "namespace": ns, "labels": self._labels(vm)},
                    "spec": {"state": "established", "replicationPolicy": self.policy,
                             "replicationSchedule": self.schedule,
                             "volumeMappings": [{"localPVCName": c, "remoteVolumeHandle": handles[c]}]}})
                out.append(f"{dst}: tmr/{c} created (established <- {handles[c]})")
            if kd.get("pvc", ns, c) is None:
                sc = spvc["spec"].get("storageClassName", "")
                body = {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                        "metadata": clean_meta(spvc["metadata"], extra_drop=self.drop_annotation_prefixes),
                        "spec": {"accessModes": spvc["spec"]["accessModes"],
                                 "volumeMode": spvc["spec"].get("volumeMode", "Filesystem"),
                                 "resources": {"requests": {"storage": spvc["spec"]["resources"]["requests"]["storage"]}},
                                 "storageClassName": self.sc_map.get(sc, sc)}}
                body["metadata"].setdefault("labels", {}).update(self._labels(vm))
                body["metadata"].setdefault("annotations", {})[ANN_MIRROR] = c
                kd.create("pvc", ns, body)
                out.append(f"{dst}: pvc/{c} created ({body['spec']['volumeMode']}, {body['spec']['storageClassName']})")
        for c in claims:
            self._wait_established(dst, ns, c, handles[c])
        if wait_init:
            self.wait_initialized(src, dst, ns, vm, claims)
            out += [f"{dst}: {c} baseline transferred" for c in claims]
        return out

    def _wait_established(self, dst: str, ns: str, claim: str, src_handle: str):
        """Wait for the destination TMR to report established. Trident only does so once the SnapMirror baseline is
        done, which can take minutes; a baseline that stops making progress (seen on memory-starved simulators) is
        aborted and restarted from its checkpoint."""
        k = self.k[dst]
        stall = int(self.timeouts.get("stall", 120))
        deadline = time.time() + self.timeouts["transfer"]
        last, since, restarts = None, time.time(), 0
        while time.time() < deadline:
            c = tmr_condition(k.get("tmr", ns, claim))
            if c.get("state") in ("established", "reestablished") and c.get("localVolumeHandle"):
                return c
            try:
                ontap = self.ontap_for_claim(dst, ns, claim)
                rel = ontap.relationship_by_source(src_handle)
            except (ReplicationError, OntapError):
                rel = None
            cur = (rel or {}).get("transfer") or {}
            if cur.get("state") in _BUSY:
                b = cur.get("bytes_transferred")
                if b != last:
                    last, since = b, time.time()
                elif time.time() - since > stall and restarts < 3:
                    restarts += 1
                    self.log(f"  [yellow]{claim}: baseline stalled at {b} B for {stall}s, restarting "
                             f"({restarts}/3)[/yellow]")
                    try:
                        ontap.abort(rel["uuid"], cur["uuid"])
                        time.sleep(10)
                        ontap.set_state(rel["uuid"], "snapmirrored", wait=False)   # = initialize / resume from checkpoint
                    except OntapError as e:
                        self.log(f"  [yellow]{claim}: restart failed: {e}[/yellow]")
                    last, since = None, time.time()
            time.sleep(5)
        raise ReplicationError(f"{dst}: TMR {ns}/{claim} not established after {self.timeouts['transfer']}s: "
                               f"{tmr_condition(k.get('tmr', ns, claim))}")

    def wait_initialized(self, src: str, dst: str, ns: str, vm: str, claims: List[str]):
        k = self.k[dst]
        for claim in claims:
            handle = tmr_condition(k.get("tmr", ns, claim)).get("localVolumeHandle")
            ontap = self.ontap_for_claim(dst, ns, claim)

            def ok():
                r = ontap.relationship(handle)
                return r if r and r.get("state") == "snapmirrored" else None
            k.wait_for(ok, self.timeouts["transfer"], interval=10, what=f"SnapMirror baseline of {claim}")

    # ── status ───────────────────────────────────────────────────────────────
    def volume_status(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[VolumeStatus]:
        rows = []
        for c in claims:
            vs = VolumeStatus(claim=c)
            ps = tmr_condition(self.k[primary].get("tmr", ns, c))
            rs = tmr_condition(self.k[replica].get("tmr", ns, c)) if self.k[replica] else {}
            vs.src_state, vs.src_handle = ps.get("state", "-"), ps.get("localVolumeHandle", "-")
            vs.dst_state, vs.dst_handle = rs.get("state", "-"), rs.get("localVolumeHandle", "-")
            if vs.dst_handle != "-" and vs.dst_state in ("established", "reestablished"):
                try:
                    r = self.ontap_for_claim(replica, ns, c).relationship(vs.dst_handle)
                    if r:
                        vs.repl_state = r.get("state", "-")
                        vs.healthy = bool(r.get("healthy")) and vs.repl_state == "snapmirrored"
                        vs.lag_s = parse_iso_duration(r.get("lag_time"))
                        vs.recovery_point = snapshot_time(r.get("exported_snapshot"))
                        vs.transfer = (r.get("transfer") or {}).get("state", "idle")
                        if not r.get("healthy") and r.get("unhealthy_reason"):
                            vs.note = r["unhealthy_reason"][0].get("message", "")[:120]
                    else:
                        vs.healthy = False
                        vs.note = "no SnapMirror relationship on destination ONTAP"
                except (ReplicationError, OntapError, KubeError) as e:
                    vs.note = str(e)[:120]
            else:
                vs.healthy = False
                if rs.get("message"):
                    vs.note = rs["message"][:120]
            rows.append(vs)
        return rows

    # ── failover ─────────────────────────────────────────────────────────────
    def preflight(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool):
        for c in claims:
            st = tmr_condition(self.k[dst].get("tmr", ns, c)).get("state")
            # promoted = an earlier failover stopped half-way (the orchestrator refuses a dst that is primary or needs reprotect)
            if st not in ("established", "reestablished", "promoted"):
                raise ReplicationError(f"{dst}: TMR {ns}/{c} is '{st}' instead of established. Is {ns}/{vm} "
                                       f"protected from {src} to {dst}?")

    def final_sync(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                   not_before: datetime.datetime) -> List[Tuple[str, int, float]]:
        out = []
        for c in claims:
            nbytes, secs = self._final_update(dst, ns, c, not_before)
            out.append((c, nbytes, secs))
        return out

    def _final_update(self, dst: str, ns: str, claim: str, not_before: datetime.datetime) -> Tuple[int, float]:
        """SnapMirror update on the destination for one volume, after the VM was stopped. Returns (bytes, seconds).
        Retries while a scheduled transfer is already running (ONTAP rejects a second one)."""
        k = self.k[dst]
        handle = tmr_condition(k.get("tmr", ns, claim)).get("localVolumeHandle")
        ontap = self.ontap_for_claim(dst, ns, claim)
        stall = int(self.timeouts.get("stall", 120))
        t0 = time.time()
        deadline = t0 + self.timeouts["transfer"]
        attempts = 0
        busy_bytes, busy_since = None, time.time()
        nbytes = 0
        while True:
            rel = ontap.relationship(handle)
            if rel is None:
                raise ReplicationError(f"{dst}: no SnapMirror relationship for {handle}")
            cur = rel.get("transfer") or {}
            if cur.get("state") in _BUSY:
                # a scheduled transfer is running: let it finish, abort it if it is stuck
                if cur.get("bytes_transferred") != busy_bytes:
                    busy_bytes, busy_since = cur.get("bytes_transferred"), time.time()
                elif time.time() - busy_since > stall and cur.get("uuid"):
                    self.log(f"  [yellow]{claim}: the running transfer stalled for {stall}s; stopping it so Harvey can retry.[/yellow]")
                    try:
                        ontap.abort(rel["uuid"], cur["uuid"])
                    except OntapError:
                        pass
                    busy_since = time.time()
                if time.time() > deadline:
                    raise ReplicationError(f"{claim}: a SnapMirror transfer is still running after the timeout.")
                time.sleep(3)
                continue
            try:
                xfer = ontap.update(rel["uuid"])
            except OntapError as e:
                if "in progress" in str(e).lower() and time.time() < deadline:
                    time.sleep(3)
                    continue
                raise
            if not xfer:
                break
            try:
                t = ontap.wait_update(rel["uuid"], xfer, timeout_s=int(deadline - time.time()), stall_s=stall)
                nbytes = int(t.get("bytes_transferred") or 0)
                break
            except StalledTransfer as e:
                attempts += 1
                self.log(f"  [yellow]{claim}: {e}; retry {attempts}/3[/yellow]")
                if attempts >= 3:
                    raise ReplicationError(f"{claim}: final transfer stalled {attempts} times") from e
                time.sleep(5)
        if not xfer:  # older ONTAP returns a job instead of a transfer record: watch the snapshot time
            k.wait_for(lambda: (snapshot_time(ontap.relationship(handle).get("exported_snapshot")) or not_before)
                       > not_before, self.timeouts["transfer"], interval=3, what=f"final transfer {claim}")
            nbytes = 0
        rp = snapshot_time(ontap.relationship(handle).get("exported_snapshot"))
        if rp and rp < not_before.replace(microsecond=0):
            raise ReplicationError(f"{claim}: recovery point {iso(rp)} is older than the stop time {iso(not_before)}")
        return nbytes, time.time() - t0

    def recovery_point_age(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                           src_up: bool) -> Optional[int]:
        worst = 0
        for c in claims:
            try:
                h = tmr_condition(self.k[dst].get("tmr", ns, c)).get("localVolumeHandle")
                if not h:
                    return None
                ontap = self.ontap_for_claim(dst, ns, c)
                rel = ontap.relationship(h)
                # no relationship = promoted by an earlier, interrupted failover: the volume still carries the
                # last transferred snapshot, which is its recovery point
                rp = snapshot_time(rel.get("exported_snapshot") if rel else ontap.newest_snapmirror_snapshot(h))
            except (ReplicationError, OntapError, KubeError):
                return None
            if not rp:
                return None
            worst = max(worst, int((now_utc() - rp).total_seconds()))
        return worst

    def promote(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool, unplanned: bool):
        for c in claims:
            self._promote_one(dst, ns, c)

    def _promote_one(self, dst: str, ns: str, claim: str):
        """Promote the replica volume to read-write, and make sure it really is.

        Observed with Trident 26.06 + ONTAP 9.16.1: the TMR can reach status 'promoted' while the volume is still
        DP, so the VM cannot attach it ("volume is not read-write"):
          * a scheduled transfer was running -> Trident's break fails ("snapmirror transfer already in progress"),
            is requeued, and the TMR still ends up 'promoted';
          * the relationship was already quiesced (paused) -> Trident marks it promoted without breaking.
        So: abort a running transfer, let Trident promote, verify the volume type in ONTAP, and if it is still DP
        do quiesce + break ourselves."""
        k = self.k[dst]
        handle = tmr_condition(k.get("tmr", ns, claim)).get("localVolumeHandle")
        ontap = None
        if handle:
            try:
                ontap = self.ontap_for_claim(dst, ns, claim)
                self._abort_running(ontap, handle)
            except (ReplicationError, OntapError) as e:
                self.log(f"  [yellow]{claim}: ONTAP not reachable before promote ({e})[/yellow]")
                ontap = None
        k.merge_patch("tmr", ns, claim, {"spec": {"state": "promoted"}})
        self._wait_tmr(dst, ns, claim, ("promoted",), need_handle=False)
        if not (ontap and handle) or ontap.volume_type(handle) != "dp":
            return
        rel = ontap.relationship(handle)
        self.log(f"  [yellow]{claim}: TMR says promoted but {handle} is still DP "
                 f"(SnapMirror {rel.get('state') if rel else '?'}); breaking it via ONTAP[/yellow]")
        if rel:
            self._abort_running(ontap, handle)
            if rel.get("state") not in ("paused", "broken_off"):
                ontap.set_state(rel["uuid"], "paused")
                k.wait_for(lambda: (ontap.relationship(handle) or {}).get("state") in ("paused", "broken_off"),
                           120, interval=3, what=f"quiesce {claim}")
            ontap.set_state(rel["uuid"], "broken_off")
        k.wait_for(lambda: ontap.volume_type(handle) == "rw", 120, interval=3, what=f"{handle} read-write")

    def _abort_running(self, ontap: OntapClient, handle: str):
        rel = ontap.relationship(handle)
        cur = (rel or {}).get("transfer") or {}
        if cur.get("state") in _BUSY and cur.get("uuid"):
            try:
                ontap.abort(rel["uuid"], cur["uuid"])
            except OntapError:
                pass
            deadline = time.time() + 60
            while time.time() < deadline and ((ontap.relationship(handle) or {}).get("transfer") or {}).get(
                    "state") in _BUSY:
                time.sleep(2)

    # ── reprotect / unprotect ────────────────────────────────────────────────
    def reprotect(self, new_src: str, new_dst: str, ns: str, vm: str, claims: List[str],
                  wait_init: bool) -> List[str]:
        ks, kd = self.k[new_src], self.k[new_dst]
        out = []
        for c in claims:
            src_c = tmr_condition(ks.get("tmr", ns, c))
            if src_c.get("state") != "promoted" or not src_c.get("localVolumeHandle"):
                raise ReplicationError(f"{new_src}: tmr/{c} is not promoted with a volume handle: {src_c}")
            t = kd.get("tmr", ns, c)
            if t is None:
                raise ReplicationError(f"{new_dst}: tmr/{c} not found. Was this VM protected by Harvey?")
            dst_c = tmr_condition(t)
            if dst_c.get("state") in ("established", "reestablished") and \
                    dst_c.get("remoteVolumeHandle") == src_c["localVolumeHandle"]:
                out.append(f"{new_dst}: tmr/{c} already {dst_c['state']}")
                continue
            out += self._request_reestablish(new_dst, ns, c, src_c["localVolumeHandle"], t)
        try:
            try:
                self._wait_tmrs(new_dst, ns, claims, ("reestablished", "established"))
            except TmrWaitError as e:
                if not e.failed:
                    raise
                # one automatic retry: a resync can fail on a transient ONTAP state (seen in the lab: the relationship
                # left by an earlier failed resync hit its 5-minute schedule, "must be a data-protection volume")
                self.log(f"  [yellow]{e}; retrying the reestablish once[/yellow]")
                for c in e.failed:
                    src_h = tmr_condition(ks.get("tmr", ns, c))["localVolumeHandle"]
                    self._request_reestablish(new_dst, ns, c, src_h, kd.get("tmr", ns, c))
                self._wait_tmrs(new_dst, ns, e.failed, ("reestablished", "established"))
        except ReplicationError as e:
            raise ReplicationError(f"{e}. Please check that {new_dst}'s ONTAP can reach {new_src}'s (cluster and "
                                   f"SVM peering, intercluster LIFs); ONTAP's own reason is in Trident's log "
                                   f"(kubectl -n trident logs deploy/trident-controller -c trident-main | grep "
                                   f"'Could not reestablish'). Then run harvey reprotect again: it retries the failed "
                                   f"volumes.") from e
        if wait_init:
            self.wait_initialized(new_src, new_dst, ns, vm, claims)
        return out

    def _request_reestablish(self, new_dst: str, ns: str, c: str, src_handle: str, t: dict) -> List[str]:
        """Sets the TMR to reestablished (reverse resync from src_handle). A failed earlier attempt is stepped
        through promoted first: Trident only reconciles on a spec change, so re-sending the same failed spec would
        do nothing (the volume isn't mirrored at this point)."""
        kd, out = self.k[new_dst], []
        dst_c = tmr_condition(t)
        if t["spec"].get("state") == "reestablished" and dst_c.get("state") != "reestablishing":
            kd.merge_patch("tmr", ns, c, {"spec": {"state": "promoted"}})
            self._wait_tmr(new_dst, ns, c, ("promoted",), need_handle=False)
            out.append(f"{new_dst}: tmr/{c} was {dst_c.get('state')}: retrying the reestablish")
            t = kd.get("tmr", ns, c)
        vmap = t["spec"]["volumeMappings"][0]
        vmap["remoteVolumeHandle"] = src_handle
        patch = {"spec": {"state": "reestablished", "volumeMappings": [vmap]}}
        if not t["spec"].get("replicationPolicy"):
            patch["spec"]["replicationPolicy"] = self.policy
        if not t["spec"].get("replicationSchedule"):
            patch["spec"]["replicationSchedule"] = self.schedule
        kd.merge_patch("tmr", ns, c, patch)
        out.append(f"{new_dst}: tmr/{c} -> reestablished <- {src_handle}")
        return out

    def unprotect(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[str]:
        kp, kr = self.k[primary], self.k[replica]
        out = []
        for c in claims:
            if kr.get("tmr", ns, c) is not None:
                kr.delete("tmr", ns, c)
                out.append(f"{replica}: tmr/{c} deleted (relationship released)")
        for c in claims:
            kr.wait_for(lambda: kr.get("tmr", ns, c) is None, self.timeouts["tmr"], what=f"tmr {c} deletion")
            if kp.get("tmr", ns, c) is not None:
                kp.delete("tmr", ns, c)
                out.append(f"{primary}: tmr/{c} deleted")
        return out
