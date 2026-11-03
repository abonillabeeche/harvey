"""Storage-aware DR engine: NetApp SnapMirror (through Trident TridentMirrorRelationships) moves the disks,
Harvey moves the metadata (VM spec, cloud-init/credential Secrets, ConfigMaps, NetworkAttachmentDefinitions)
and orchestrates stop -> final sync -> promote -> start.

Data path (per PVC of a protected VM):
  source cluster : TMR <pvc>  state=promoted     -> exposes the ONTAP volume handle (svm:volume)
  dest cluster   : TMR <pvc>  state=established  remoteVolumeHandle=<source handle>, policy + schedule
                   PVC <pvc>  annotation trident.netapp.io/mirrorRelationship=<pvc>  (a DP volume on the peered SVM)
  failover       : dest TMR -> promoted (SnapMirror break, volume becomes RW)
  reprotect      : old-source TMR -> reestablished with remoteVolumeHandle=<new source handle> (reverse resync)

Who is primary is never stored locally: it is derived from the TMR states on each side.
"""

from __future__ import annotations

import base64
import copy
import datetime
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from .kube import KubeClient, KubeError
from .ontap import OntapClient, OntapError, StalledTransfer, parse_iso_duration, snapshot_time

TRIDENT = "csi.trident.netapp.io"
L_MANAGED = "harvey.io/managed"          # label on everything Harvey creates/owns
L_VM = "harvey.io/vm"                    # label: VM name the object belongs to
L_ROLE = "harvey.io/role"                # label on VMs: primary | replica
A_PEER = "harvey.io/peer-cluster"        # annotation on VMs: the other site
A_RUN = "harvey.io/run-strategy"         # annotation: runStrategy to restore on start
A_SYNCED = "harvey.io/synced-at"
A_SOURCE = "harvey.io/source-cluster"
A_REPROTECT = "harvey.io/needs-reprotect"
ANN_MIRROR = "trident.netapp.io/mirrorRelationship"

# metadata that must not travel to the other cluster
_DROP_ANN_PREFIXES = ("kubectl.kubernetes.io/", "kubevirt.io/latest-observed-api-version",
                      "kubevirt.io/storage-observed-api-version", "harvesterhci.io/volumeClaimTemplates",
                      "harvey.io/", "pv.kubernetes.io/", "volume.kubernetes.io/", "volume.beta.kubernetes.io/",
                      "harvesterhci.io/imageId", "cdi.kubevirt.io/", "trident.netapp.io/")
_DROP_LABEL_PREFIXES = ("harvey.io/",)


class ReplicationError(Exception):
    pass


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(t: Optional[datetime.datetime]) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if t else "-"


def _clean_meta(meta: dict, keep_annotations: bool = True) -> dict:
    out = {"name": meta["name"]}
    if meta.get("namespace"):
        out["namespace"] = meta["namespace"]
    labels = {k: v for k, v in (meta.get("labels") or {}).items() if not k.startswith(_DROP_LABEL_PREFIXES)}
    if labels:
        out["labels"] = labels
    if keep_annotations:
        ann = {k: v for k, v in (meta.get("annotations") or {}).items() if not k.startswith(_DROP_ANN_PREFIXES)}
        if ann:
            out["annotations"] = ann
    return out


def tmr_condition(tmr: Optional[dict]) -> dict:
    if not tmr:
        return {}
    conds = (tmr.get("status") or {}).get("conditions") or []
    return conds[0] if conds else {}


def vm_status(vm: Optional[dict]) -> str:
    if vm is None:
        return "Not Found"
    return (vm.get("status") or {}).get("printableStatus", "Unknown")


# ── VM introspection ─────────────────────────────────────────────────────────
def vm_claims(vm: dict) -> List[str]:
    """PVC names used by a VM (persistentVolumeClaim and dataVolume volumes)."""
    claims = []
    for v in vm["spec"]["template"]["spec"].get("volumes", []):
        if "persistentVolumeClaim" in v:
            claims.append(v["persistentVolumeClaim"]["claimName"])
        elif "dataVolume" in v:
            claims.append(v["dataVolume"]["name"])
    return claims


def vm_secret_names(vm: dict) -> List[str]:
    names = []
    tspec = vm["spec"]["template"]["spec"]
    for v in tspec.get("volumes", []):
        for key in ("cloudInitNoCloud", "cloudInitConfigDrive"):
            ci = v.get(key) or {}
            for ref in ("secretRef", "userDataSecretRef", "networkDataSecretRef"):
                if ci.get(ref, {}).get("name"):
                    names.append(ci[ref]["name"])
        if v.get("secret", {}).get("secretName"):
            names.append(v["secret"]["secretName"])
        if v.get("sysprep", {}).get("secret", {}).get("name"):
            names.append(v["sysprep"]["secret"]["name"])
    for ac in tspec.get("accessCredentials", []) or []:
        for kind in ("sshPublicKey", "userPassword"):
            n = (ac.get(kind) or {}).get("source", {}).get("secret", {}).get("secretName")
            if n:
                names.append(n)
    return sorted(set(names))


def vm_configmap_names(vm: dict) -> List[str]:
    names = []
    for v in vm["spec"]["template"]["spec"].get("volumes", []):
        if v.get("configMap", {}).get("name"):
            names.append(v["configMap"]["name"])
        if v.get("sysprep", {}).get("configMap", {}).get("name"):
            names.append(v["sysprep"]["configMap"]["name"])
    return sorted(set(names))


def vm_nads(vm: dict) -> List[Tuple[str, str]]:
    """(namespace, name) of every multus network the VM uses."""
    ns = vm["metadata"]["namespace"]
    out = []
    for n in vm["spec"]["template"]["spec"].get("networks", []) or []:
        ref = (n.get("multus") or {}).get("networkName")
        if ref:
            out.append(tuple(ref.split("/", 1)) if "/" in ref else (ns, ref))
    return sorted(set(out))


def original_run_strategy(vm: dict) -> str:
    """runStrategy to use when (re)starting this VM elsewhere."""
    ann = (vm["metadata"].get("annotations") or {}).get(A_RUN)
    if ann:
        return ann
    rs = vm["spec"].get("runStrategy")
    if rs is None and "running" in vm["spec"]:
        rs = "Always" if vm["spec"]["running"] else "Halted"
    if not rs or rs == "Halted":
        return "RerunOnFailure"  # Harvester's default
    return rs


def build_replica_vm(vm: dict, source_cluster: str, peer_cluster: str) -> dict:
    """Clean copy of a VirtualMachine for the other site: stopped, no runtime fields, no PVC templates."""
    spec = copy.deepcopy(vm["spec"])
    spec.pop("running", None)
    spec["runStrategy"] = "Halted"
    spec.pop("dataVolumeTemplates", None)
    for v in spec["template"]["spec"].get("volumes", []):
        if "dataVolume" in v:  # replica disks are plain PVCs
            v["persistentVolumeClaim"] = {"claimName": v.pop("dataVolume")["name"]}
    meta = _clean_meta(vm["metadata"])
    meta.setdefault("labels", {}).update({L_MANAGED: "true", L_VM: vm["metadata"]["name"], L_ROLE: "replica"})
    meta.setdefault("annotations", {}).update({
        A_RUN: original_run_strategy(vm),
        A_SOURCE: source_cluster,
        A_PEER: peer_cluster,
        A_SYNCED: _iso(now_utc()),
    })
    return {"apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine", "metadata": meta, "spec": spec}


# ── results ──────────────────────────────────────────────────────────────────
@dataclass
class VolumeStatus:
    claim: str
    src_handle: str = "-"
    dst_handle: str = "-"
    src_tmr: str = "-"
    dst_tmr: str = "-"
    sm_state: str = "-"
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


class Replicator:
    def __init__(self, clusters: Dict[str, KubeClient], settings: dict, log: Callable[[str], None],
                 ontap_overrides: Optional[Dict[str, dict]] = None, debug: bool = False):
        self.k = clusters
        self.policy = settings.get("policy", "MirrorAllSnapshots")
        self.schedule = settings.get("schedule", "5min")
        self.sc_map: Dict[str, str] = settings.get("storage_class_map") or {}
        self.timeouts = {"stop": 300, "start": 600, "tmr": 300, "transfer": 1800, "stall": 120, **(settings.get("timeouts") or {})}
        self.log = log
        self.debug = debug
        self.ontap_overrides = ontap_overrides or {}
        self._ontap_cache: Dict[Tuple[str, str], OntapClient] = {}

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
            raise ReplicationError(f"{cluster}: PVC {ns}/{claim} not bound")
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

    # ── helpers ──────────────────────────────────────────────────────────────
    def _get_vm(self, cluster: str, ns: str, vm: str, required: bool = True) -> Optional[dict]:
        obj = self.k[cluster].get("vm", ns, vm)
        if obj is None and required:
            raise ReplicationError(f"VM {ns}/{vm} not found on {cluster}")
        return obj

    def _labels(self, vm: str) -> dict:
        return {L_MANAGED: "true", L_VM: vm}

    def _wait_tmr(self, cluster: str, ns: str, name: str, states: Tuple[str, ...], need_handle: bool = True) -> dict:
        k = self.k[cluster]

        def ok():
            c = tmr_condition(k.get("tmr", ns, name))
            if c.get("state") in states and (c.get("localVolumeHandle") or not need_handle):
                return c
            return None
        try:
            return k.wait_for(ok, self.timeouts["tmr"], what=f"TMR {ns}/{name} -> {'/'.join(states)}")
        except KubeError as e:
            c = tmr_condition(k.get("tmr", ns, name))
            raise ReplicationError(f"{e}; TMR condition: {c}") from e

    def _check_trident_pvc(self, cluster: str, ns: str, claim: str) -> dict:
        k = self.k[cluster]
        pvc = k.get("pvc", ns, claim)
        if pvc is None:
            raise ReplicationError(f"{cluster}: PVC {ns}/{claim} not found")
        pv = k.get("pv", None, pvc["spec"].get("volumeName", "")) if pvc["spec"].get("volumeName") else None
        drv = ((pv or {}).get("spec", {}).get("csi") or {}).get("driver")
        if drv != TRIDENT:
            raise ReplicationError(f"{cluster}: PVC {ns}/{claim} is on '{drv}', not Trident — cannot replicate it "
                                   f"with SnapMirror")
        return pvc

    # ── metadata sync ────────────────────────────────────────────────────────
    def sync_metadata(self, ns: str, vm_name: str, src: str, dst: str, vm: Optional[dict] = None) -> List[str]:
        """Copies VM spec + Secrets + ConfigMaps + NADs from src to dst. The dst VM always stays Halted."""
        ks, kd = self.k[src], self.k[dst]
        vm = vm or self._get_vm(src, ns, vm_name)
        actions = []
        kd.ensure_namespace(ns)

        for s in vm_secret_names(vm):
            obj = ks.get("secret", ns, s)
            if obj is None:
                actions.append(f"WARN secret {s} missing on {src}")
                continue
            body = {"apiVersion": "v1", "kind": "Secret", "type": obj.get("type", "Opaque"),
                    "metadata": _clean_meta(obj["metadata"]), "data": obj.get("data", {})}
            body["metadata"].setdefault("labels", {}).update(self._labels(vm_name))
            actions.append(f"secret/{s} {kd.apply('secret', ns, body)}")

        for c in vm_configmap_names(vm):
            obj = ks.get("configmap", ns, c)
            if obj is None:
                actions.append(f"WARN configmap {c} missing on {src}")
                continue
            body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": _clean_meta(obj["metadata"]),
                    "data": obj.get("data", {}), "binaryData": obj.get("binaryData", {})}
            body["metadata"].setdefault("labels", {}).update(self._labels(vm_name))
            actions.append(f"configmap/{c} {kd.apply('configmap', ns, body)}")

        for nns, nname in vm_nads(vm):
            src_nad = ks.get("nad", nns, nname)
            dst_nad = kd.get("nad", nns, nname)
            if src_nad is None:
                actions.append(f"WARN NAD {nns}/{nname} missing on {src}")
                continue
            cn = (src_nad["metadata"].get("labels") or {}).get("network.harvesterhci.io/clusternetwork")
            if cn and kd.get("clusternetwork", None, cn) is None:
                actions.append(f"WARN NAD {nns}/{nname}: cluster network '{cn}' does not exist on {dst}")
            if dst_nad is not None:
                if dst_nad.get("spec") != src_nad.get("spec"):
                    actions.append(f"WARN NAD {nns}/{nname} differs on {dst} (left as is: site-specific?)")
                else:
                    actions.append(f"nad/{nns}/{nname} present")
                continue
            kd.ensure_namespace(nns)
            body = {"apiVersion": "k8s.cni.cncf.io/v1", "kind": "NetworkAttachmentDefinition",
                    "metadata": _clean_meta(src_nad["metadata"]), "spec": src_nad["spec"]}
            body["metadata"].setdefault("labels", {})[L_MANAGED] = "true"
            kd.create("nad", nns, body)
            actions.append(f"nad/{nns}/{nname} created")

        cur = kd.get("vm", ns, vm_name)
        if cur is not None and (cur["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
            raise ReplicationError(f"{dst} holds the PRIMARY copy of {ns}/{vm_name}; refusing to overwrite it "
                                   f"(sync runs from the primary to the replica)")
        if cur is not None and vm_status(cur) not in ("Stopped", "Halted", "Unknown"):
            raise ReplicationError(f"replica {ns}/{vm_name} on {dst} is {vm_status(cur)}; it must be stopped")
        body = build_replica_vm(vm, src, src)
        actions.append(f"vm/{vm_name} {kd.apply('vm', ns, body)} (Halted)")
        return actions

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, ns: str, vm_name: str, src: str, dst: str, wait_init: bool = False) -> List[str]:
        ks, kd = self.k[src], self.k[dst]
        vm = self._get_vm(src, ns, vm_name)
        cur_dst = kd.get("vm", ns, vm_name)
        if cur_dst is not None and (cur_dst["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
            raise ReplicationError(f"{ns}/{vm_name} is primary on {dst}; protect from {dst} instead")
        claims = vm_claims(vm)
        if not claims:
            raise ReplicationError(f"{ns}/{vm_name} has no PVC volumes")
        src_pvcs = {c: self._check_trident_pvc(src, ns, c) for c in claims}
        out = []

        # 1. source TMRs -> volume handles
        handles = {}
        for c in claims:
            t = ks.get("tmr", ns, c)
            if t is None:
                ks.create("tmr", ns, {"apiVersion": "trident.netapp.io/v1", "kind": "TridentMirrorRelationship",
                                      "metadata": {"name": c, "namespace": ns, "labels": self._labels(vm_name)},
                                      "spec": {"state": "promoted", "volumeMappings": [{"localPVCName": c}]}})
                out.append(f"{src}: tmr/{c} created (promoted)")
            elif t["spec"].get("state") != "promoted":
                raise ReplicationError(f"{src}: tmr/{c} is '{t['spec'].get('state')}' — {src} is not the primary "
                                       f"for this volume")
            handles[c] = self._wait_tmr(src, ns, c, ("promoted",))["localVolumeHandle"]

        # 2. metadata: namespace, secrets, configmaps, NADs and the (Halted) replica VM
        out += [f"{dst}: {a}" for a in self.sync_metadata(ns, vm_name, src, dst, vm=vm)]

        # 3. destination TMRs + PVCs
        for c in claims:
            spvc = src_pvcs[c]
            t = kd.get("tmr", ns, c)
            if t is None:
                kd.create("tmr", ns, {
                    "apiVersion": "trident.netapp.io/v1", "kind": "TridentMirrorRelationship",
                    "metadata": {"name": c, "namespace": ns, "labels": self._labels(vm_name)},
                    "spec": {"state": "established", "replicationPolicy": self.policy,
                             "replicationSchedule": self.schedule,
                             "volumeMappings": [{"localPVCName": c, "remoteVolumeHandle": handles[c]}]}})
                out.append(f"{dst}: tmr/{c} created (established <- {handles[c]})")
            if kd.get("pvc", ns, c) is None:
                sc = spvc["spec"].get("storageClassName", "")
                body = {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                        "metadata": _clean_meta(spvc["metadata"]),
                        "spec": {"accessModes": spvc["spec"]["accessModes"],
                                 "volumeMode": spvc["spec"].get("volumeMode", "Filesystem"),
                                 "resources": {"requests": {"storage": spvc["spec"]["resources"]["requests"]["storage"]}},
                                 "storageClassName": self.sc_map.get(sc, sc)}}
                body["metadata"].setdefault("labels", {}).update(self._labels(vm_name))
                body["metadata"].setdefault("annotations", {})[ANN_MIRROR] = c
                kd.create("pvc", ns, body)
                out.append(f"{dst}: pvc/{c} created ({body['spec']['volumeMode']}, {body['spec']['storageClassName']})")
        for c in claims:
            self._wait_tmr(dst, ns, c, ("established", "reestablished"))

        # 4. role labels
        self._set_role(src, ns, vm_name, "primary", dst)

        if wait_init:
            for c in claims:
                self.wait_initialized(dst, ns, c)
                out.append(f"{dst}: {c} baseline transferred")
        return out

    def _set_role(self, cluster: str, ns: str, vm_name: str, role: str, peer: str, extra_ann: Optional[dict] = None):
        ann = {A_PEER: peer}
        ann.update(extra_ann or {})
        self.k[cluster].merge_patch("vm", ns, vm_name, {"metadata": {
            "labels": {L_MANAGED: "true", L_VM: vm_name, L_ROLE: role}, "annotations": ann}})

    def wait_initialized(self, cluster: str, ns: str, claim: str, timeout_s: Optional[int] = None):
        k = self.k[cluster]
        handle = tmr_condition(k.get("tmr", ns, claim)).get("localVolumeHandle")
        ontap = self.ontap_for_claim(cluster, ns, claim)

        def ok():
            r = ontap.relationship(handle)
            return r if r and r.get("state") == "snapmirrored" else None
        return k.wait_for(ok, timeout_s or self.timeouts["transfer"], interval=10,
                          what=f"SnapMirror baseline of {claim}")

    # ── status ───────────────────────────────────────────────────────────────
    def protected_vms(self, namespace: Optional[str] = None) -> List[Tuple[str, str]]:
        found = set()
        for name, k in self.k.items():
            try:
                for v in k.list("vm", namespace, label_selector=f"{L_MANAGED}=true"):
                    found.add((v["metadata"]["namespace"], v["metadata"]["name"]))
            except KubeError:
                pass
        return sorted(found)

    def direction(self, ns: str, vm_name: str, a: str, b: str) -> Tuple[Optional[str], Optional[str]]:
        """(primary, replica) by VM role labels, falling back to TMR states."""
        roles = {}
        for c in (a, b):
            try:
                v = self.k[c].get("vm", ns, vm_name)
            except KubeError:
                v = None
            roles[c] = ((v or {}).get("metadata", {}).get("labels") or {}).get(L_ROLE)
        if roles[a] == "primary" and roles[b] != "primary":
            return a, b
        if roles[b] == "primary" and roles[a] != "primary":
            return b, a
        return None, None

    def volume_status(self, ns: str, vm_name: str, primary: str, replica: str) -> List[VolumeStatus]:
        vm = self._get_vm(primary, ns, vm_name)
        rows = []
        for c in vm_claims(vm):
            vs = VolumeStatus(claim=c)
            ps = tmr_condition(self.k[primary].get("tmr", ns, c))
            rs = tmr_condition(self.k[replica].get("tmr", ns, c)) if self.k[replica] else {}
            vs.src_tmr, vs.src_handle = ps.get("state", "-"), ps.get("localVolumeHandle", "-")
            vs.dst_tmr, vs.dst_handle = rs.get("state", "-"), rs.get("localVolumeHandle", "-")
            if vs.dst_handle != "-" and vs.dst_tmr in ("established", "reestablished"):
                try:
                    r = self.ontap_for_claim(replica, ns, c).relationship(vs.dst_handle)
                    if r:
                        vs.sm_state = r.get("state", "-")
                        vs.healthy = r.get("healthy")
                        vs.lag_s = parse_iso_duration(r.get("lag_time"))
                        vs.recovery_point = snapshot_time(r.get("exported_snapshot"))
                        vs.transfer = (r.get("transfer") or {}).get("state", "idle")
                        if not vs.healthy and r.get("unhealthy_reason"):
                            vs.note = r["unhealthy_reason"][0].get("message", "")[:120]
                    else:
                        vs.note = "no SnapMirror relationship on destination ONTAP"
                except (ReplicationError, OntapError, KubeError) as e:
                    vs.note = str(e)[:120]
            elif rs.get("message"):
                vs.note = rs["message"][:120]
            rows.append(vs)
        return rows

    # ── start / stop ─────────────────────────────────────────────────────────
    def stop_vm(self, cluster: str, ns: str, vm_name: str):
        k = self.k[cluster]
        vm = self._get_vm(cluster, ns, vm_name)
        patch = {"metadata": {"annotations": {A_RUN: original_run_strategy(vm)}}}
        if "running" in vm["spec"] and "runStrategy" not in vm["spec"]:
            patch["spec"] = {"running": False}
        else:
            patch["spec"] = {"runStrategy": "Halted"}
        k.merge_patch("vm", ns, vm_name, patch)

    def wait_stopped(self, cluster: str, ns: str, vm_name: str):
        k = self.k[cluster]

        def ok():
            return vm_status(k.get("vm", ns, vm_name)) in ("Stopped", "Halted") and \
                k.get("vmi", ns, vm_name) is None
        k.wait_for(ok, self.timeouts["stop"], what=f"{ns}/{vm_name} stopped")

    def start_vm(self, cluster: str, ns: str, vm_name: str):
        k = self.k[cluster]
        vm = self._get_vm(cluster, ns, vm_name)
        rs = original_run_strategy(vm)
        if "running" in vm["spec"] and "runStrategy" not in vm["spec"]:
            k.merge_patch("vm", ns, vm_name, {"spec": {"running": True}})
        else:
            k.merge_patch("vm", ns, vm_name, {"spec": {"runStrategy": rs}})

    def wait_running(self, cluster: str, ns: str, vm_name: str, timeout_s: Optional[int] = None):
        k = self.k[cluster]
        k.wait_for(lambda: vm_status(k.get("vm", ns, vm_name)) == "Running",
                   timeout_s or self.timeouts["start"], what=f"{ns}/{vm_name} Running")

    # ── final sync ───────────────────────────────────────────────────────────
    def final_update(self, dst: str, ns: str, claim: str, not_before: datetime.datetime) -> Tuple[int, float]:
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
        while True:
            rel = ontap.relationship(handle)
            if rel is None:
                raise ReplicationError(f"{dst}: no SnapMirror relationship for {handle}")
            cur = rel.get("transfer") or {}
            if cur.get("state") in ("transferring", "queued", "preparing", "finalizing"):
                # a scheduled transfer is running: let it finish, abort it if it is stuck
                if cur.get("bytes_transferred") != busy_bytes:
                    busy_bytes, busy_since = cur.get("bytes_transferred"), time.time()
                elif time.time() - busy_since > stall and cur.get("uuid"):
                    self.log(f"  [yellow]{claim}: running transfer stalled {stall}s, aborting it[/yellow]")
                    try:
                        ontap.abort(rel["uuid"], cur["uuid"])
                    except OntapError:
                        pass
                    busy_since = time.time()
                if time.time() > deadline:
                    raise ReplicationError(f"{claim}: a transfer has been running for too long")
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
            raise ReplicationError(f"{claim}: recovery point {_iso(rp)} is older than the stop time {_iso(not_before)}")
        return nbytes, time.time() - t0

    # ── promote ──────────────────────────────────────────────────────────────
    def promote(self, dst: str, ns: str, claim: str):
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
        if cur.get("state") in ("transferring", "queued", "preparing", "finalizing") and cur.get("uuid"):
            try:
                ontap.abort(rel["uuid"], cur["uuid"])
            except OntapError:
                pass
            deadline = time.time() + 60
            while time.time() < deadline and ((ontap.relationship(handle) or {}).get("transfer") or {}).get(
                    "state") in ("transferring", "queued", "preparing", "finalizing"):
                time.sleep(2)

    # ── failover ─────────────────────────────────────────────────────────────
    def failover(self, plan: List[Tuple[str, str, int, int]], src: str, dst: str, unplanned: bool = False,
                 force: bool = False, hooks_cb: Optional[Callable[[str], bool]] = None) -> FailoverResult:
        """plan = [(namespace, vm, priority, startup_delay)]. Higher priority = stopped last, started first."""
        res = FailoverResult()
        t_start = time.time()
        ks, kd = self.k[src], self.k[dst]
        hooks_cb = hooks_cb or (lambda phase: True)
        src_up = ks.reachable() if ks else False

        # ── preflight ──
        self.log("[bold]Preflight[/bold]")
        claims_of: Dict[Tuple[str, str], List[str]] = {}
        for ns, vm, _, _ in plan:
            dvm = self._get_vm(dst, ns, vm)
            if (dvm["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
                raise ReplicationError(f"{ns}/{vm} is already primary on {dst}")
            claims = vm_claims(dvm)
            for c in claims:
                st = tmr_condition(kd.get("tmr", ns, c)).get("state")
                if st not in ("established", "reestablished"):
                    raise ReplicationError(f"{dst}: tmr {ns}/{c} is '{st}', expected established — is {ns}/{vm} "
                                           f"protected in the {src} -> {dst} direction?")
            claims_of[(ns, vm)] = claims
            if src_up:
                s = vm_status(ks.get("vm", ns, vm))
                if unplanned and s == "Running" and not force:
                    raise ReplicationError(f"{ns}/{vm} is still Running on {src}; an unplanned failover would "
                                           f"split-brain. Stop it, use a planned failover, or --force.")
            elif not unplanned:
                raise ReplicationError(f"{src} is unreachable: use --unplanned (accepts data loss up to the "
                                       f"replication lag)")
            self.log(f"  {ns}/{vm}: {len(claims)} volume(s) ready")
        if not hooks_cb("pre_failover"):
            raise ReplicationError("pre_failover hook aborted")

        if not unplanned:
            # ── metadata sync (catch spec changes made since the last sync) ──
            self.log("[bold]Metadata sync[/bold]")
            for ns, vm, _, _ in plan:
                try:
                    self.sync_metadata(ns, vm, src, dst)
                except (ReplicationError, KubeError) as e:
                    self.log(f"  [yellow]{ns}/{vm}: metadata sync failed ({e}); using the last synced spec[/yellow]")

            # ── stop ──
            t = time.time()
            self.log("[bold]Stop on " + src + "[/bold]")
            stop_times = {}
            for ns, vm, prio, _ in sorted(plan, key=lambda x: x[2]):
                try:
                    self.stop_vm(src, ns, vm)
                    self.wait_stopped(src, ns, vm)
                    stop_times[(ns, vm)] = now_utc()
                    res.stopped.append((ns, vm))
                    self.log(f"  [green]✓[/green] {ns}/{vm} stopped")
                except (ReplicationError, KubeError) as e:
                    res.failed.append((f"{ns}/{vm}", "stop", str(e)))
                    self.log(f"  [red]✗ {ns}/{vm}: {e}[/red]")
            res.phases["stop"] = int(time.time() - t)

            # ── final sync ──
            t = time.time()
            self.log("[bold]Final SnapMirror update[/bold]")
            for ns, vm, _, _ in plan:
                if (ns, vm) not in stop_times:
                    continue
                try:
                    for c in claims_of[(ns, vm)]:
                        nbytes, secs = self.final_update(dst, ns, c, stop_times[(ns, vm)])
                        res.synced.append((f"{ns}/{vm}", c, nbytes, round(secs, 1)))
                        self.log(f"  [green]✓[/green] {ns}/{c}: {nbytes} B in {secs:.1f}s")
                    res.rpo[f"{ns}/{vm}"] = 0
                except (ReplicationError, OntapError, KubeError) as e:
                    res.failed.append((f"{ns}/{vm}", "final-sync", str(e)))
                    self.log(f"  [red]✗ {ns}/{vm}: {e} — NOT promoting this VM[/red]")
                    stop_times.pop((ns, vm))
            res.phases["final_sync"] = int(time.time() - t)
            eligible = [p for p in plan if (p[0], p[1]) in stop_times]
        else:
            # data loss window = age of the newest replicated snapshot, per VM (worst volume)
            eligible = list(plan)
            for ns, vm, _, _ in plan:
                worst = 0
                for c in claims_of[(ns, vm)]:
                    try:
                        h = tmr_condition(kd.get("tmr", ns, c)).get("localVolumeHandle")
                        rp = snapshot_time(self.ontap_for_claim(dst, ns, c).relationship(h).get("exported_snapshot"))
                        worst = max(worst, int((now_utc() - rp).total_seconds()) if rp else 10**9)
                    except (ReplicationError, OntapError, KubeError):
                        worst = 10**9
                res.rpo[f"{ns}/{vm}"] = None if worst >= 10**9 else worst

        # ── promote ──
        t = time.time()
        self.log("[bold]Promote on " + dst + "[/bold]")
        promoted = []
        for ns, vm, prio, delay in eligible:
            try:
                for c in claims_of[(ns, vm)]:
                    self.promote(dst, ns, c)
                res.promoted.append(f"{ns}/{vm}")
                promoted.append((ns, vm, prio, delay))
                self.log(f"  [green]✓[/green] {ns}/{vm}: {len(claims_of[(ns, vm)])} volume(s) read-write")
            except (ReplicationError, KubeError) as e:
                res.failed.append((f"{ns}/{vm}", "promote", str(e)))
                self.log(f"  [red]✗ {ns}/{vm}: {e}[/red]")
        res.phases["promote"] = int(time.time() - t)
        if not hooks_cb("pre_start"):
            raise ReplicationError("pre_start hook aborted (volumes are already promoted)")

        # ── start ──
        t = time.time()
        self.log("[bold]Start on " + dst + "[/bold]")
        for ns, vm, prio, delay in sorted(promoted, key=lambda x: x[2], reverse=True):
            try:
                # the promoted side is the primary from now on, even if the VM is slow to boot
                self._set_role(dst, ns, vm, "primary", src)
                if src_up:
                    try:
                        self._set_role(src, ns, vm, "replica", dst, {A_REPROTECT: "true"})
                    except KubeError:
                        pass
                self.start_vm(dst, ns, vm)
                self.wait_running(dst, ns, vm)
                res.started.append(f"{ns}/{vm}")
                self.log(f"  [green]✓[/green] {ns}/{vm} Running")
                if delay:
                    time.sleep(delay)
            except (ReplicationError, KubeError) as e:
                res.failed.append((f"{ns}/{vm}", "start", str(e)))
                self.log(f"  [red]✗ {ns}/{vm}: {e}[/red]")
        res.phases["start"] = int(time.time() - t)
        hooks_cb("post_failover")
        res.rto_seconds = int(time.time() - t_start)
        return res

    # ── reprotect ────────────────────────────────────────────────────────────
    def reprotect(self, ns: str, vm_name: str, new_src: str, new_dst: str, wait_init: bool = False) -> List[str]:
        """After a failover to new_src: make new_dst a replica of new_src again (reverse resync).
        Overwrites new_dst's copy of the disks with new_src's."""
        ks, kd = self.k[new_src], self.k[new_dst]
        vm = self._get_vm(new_src, ns, vm_name)
        if (vm["metadata"].get("labels") or {}).get(L_ROLE) != "primary":
            raise ReplicationError(f"{ns}/{vm_name} is not primary on {new_src}")
        dvm = self._get_vm(new_dst, ns, vm_name)
        if vm_status(dvm) not in ("Stopped", "Halted"):
            raise ReplicationError(f"{ns}/{vm_name} is {vm_status(dvm)} on {new_dst}; it must be stopped "
                                   f"(its disks will be overwritten)")
        out = []
        for c in vm_claims(vm):
            src_c = tmr_condition(ks.get("tmr", ns, c))
            if src_c.get("state") != "promoted" or not src_c.get("localVolumeHandle"):
                raise ReplicationError(f"{new_src}: tmr/{c} is not promoted with a volume handle: {src_c}")
            t = kd.get("tmr", ns, c)
            if t is None:
                raise ReplicationError(f"{new_dst}: tmr/{c} missing (was this VM protected by Harvey?)")
            dst_c = tmr_condition(t)
            if dst_c.get("state") in ("established", "reestablished") and \
                    dst_c.get("remoteVolumeHandle") == src_c["localVolumeHandle"]:
                out.append(f"{new_dst}: tmr/{c} already {dst_c['state']}")
                continue
            vmap = t["spec"]["volumeMappings"][0]
            vmap["remoteVolumeHandle"] = src_c["localVolumeHandle"]
            patch = {"spec": {"state": "reestablished", "volumeMappings": [vmap]}}
            if not t["spec"].get("replicationPolicy"):
                patch["spec"]["replicationPolicy"] = self.policy
            if not t["spec"].get("replicationSchedule"):
                patch["spec"]["replicationSchedule"] = self.schedule
            kd.merge_patch("tmr", ns, c, patch)
            out.append(f"{new_dst}: tmr/{c} -> reestablished <- {src_c['localVolumeHandle']}")
        for c in vm_claims(vm):
            self._wait_tmr(new_dst, ns, c, ("reestablished", "established"))
        out += [f"{new_dst}: {a}" for a in self.sync_metadata(ns, vm_name, new_src, new_dst, vm=vm)]
        kd.merge_patch("vm", ns, vm_name, {"metadata": {"annotations": {A_REPROTECT: None}}})
        if wait_init:
            for c in vm_claims(vm):
                self.wait_initialized(new_dst, ns, c)
        return out

    # ── unprotect ────────────────────────────────────────────────────────────
    def unprotect(self, ns: str, vm_name: str, primary: str, replica: str, delete_replica: bool) -> List[str]:
        kp, kr = self.k[primary], self.k[replica]
        vm = self._get_vm(primary, ns, vm_name)
        out = []
        for c in vm_claims(vm):
            if kr.get("tmr", ns, c) is not None:
                kr.delete("tmr", ns, c)
                out.append(f"{replica}: tmr/{c} deleted (relationship released)")
        for c in vm_claims(vm):
            kr.wait_for(lambda: kr.get("tmr", ns, c) is None, self.timeouts["tmr"], what=f"tmr {c} deletion")
            if kp.get("tmr", ns, c) is not None:
                kp.delete("tmr", ns, c)
                out.append(f"{primary}: tmr/{c} deleted")
        if delete_replica:
            rvm = kr.get("vm", ns, vm_name)
            if rvm is not None:
                if vm_status(rvm) not in ("Stopped", "Halted"):
                    raise ReplicationError(f"replica {ns}/{vm_name} on {replica} is {vm_status(rvm)}; not deleting")
                kr.delete("vm", ns, vm_name)
                out.append(f"{replica}: vm/{vm_name} deleted")
            for c in vm_claims(vm):
                if kr.get("pvc", ns, c) is not None:
                    kr.delete("pvc", ns, c)
                    out.append(f"{replica}: pvc/{c} deleted")
            for s in vm_secret_names(vm):
                obj = kr.get("secret", ns, s)
                if obj is not None and (obj["metadata"].get("labels") or {}).get(L_MANAGED) == "true":
                    kr.delete("secret", ns, s)
                    out.append(f"{replica}: secret/{s} deleted")
        kp.merge_patch("vm", ns, vm_name, {"metadata": {
            "labels": {L_MANAGED: None, L_VM: None, L_ROLE: None},
            "annotations": {A_PEER: None, A_RUN: None, A_REPROTECT: None}}})
        return out
