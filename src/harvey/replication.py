"""Storage-aware DR orchestrator.

A storage backend (harvey.backends: NetApp SnapMirror, Portworx async DR, ...) moves the disks; Harvey moves the
metadata (VM spec, cloud-init/credential Secrets, ConfigMaps, NetworkAttachmentDefinitions) and orchestrates
stop -> final sync -> promote -> start, plus reprotect/failback.

The backend is picked per VM from the CSI driver of its PVs (or `replication.backend` in the config) and
recorded on both copies of the VM (annotation harvey.io/storage-backend), so an unplanned failover can pick it
without the source site.

Who is primary is never stored locally: it is the harvey.io/role label on the VM at each site.
"""

from __future__ import annotations

import copy
import json
import time
from typing import Callable, Dict, List, Optional, Tuple

from .backends import REGISTRY, StorageBackend, backend_for_driver
from .common import (A_BACKEND, A_PEER, A_PRIMARY_SINCE, A_UNIT, A_UNIT_CLAIMS, A_REPROTECT, A_RUN, A_SOURCE, A_SYNCED, L_MANAGED, L_ROLE, L_VM,
                     FailoverResult, ReplicationError, VolumeStatus, clean_meta, iso, now_utc)
from .kube import KubeClient, KubeError
from .units import Unit, resolve_unit

__all__ = ["Unit", "A_BACKEND", "A_PEER", "A_REPROTECT", "A_RUN", "L_MANAGED", "L_ROLE", "L_VM", "FailoverResult",
           "ReplicationError", "Replicator", "VolumeStatus", "build_replica_vm", "original_run_strategy",
           "vm_claims", "vm_configmap_names", "vm_nads", "vm_secret_names", "vm_status"]


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


def build_replica_vm(vm: dict, source_cluster: str, peer_cluster: str, backend: Optional[str] = None,
                     extra_drop: Tuple[str, ...] = ()) -> dict:
    """Clean copy of a VirtualMachine for the other site: stopped, no runtime fields, no PVC templates."""
    spec = copy.deepcopy(vm["spec"])
    spec.pop("running", None)
    spec["runStrategy"] = "Halted"
    spec.pop("dataVolumeTemplates", None)
    for v in spec["template"]["spec"].get("volumes", []):
        if "dataVolume" in v:  # replica disks are plain PVCs
            v["persistentVolumeClaim"] = {"claimName": v.pop("dataVolume")["name"]}
    meta = clean_meta(vm["metadata"], extra_drop=extra_drop)
    meta.setdefault("labels", {}).update({L_MANAGED: "true", L_VM: vm["metadata"]["name"], L_ROLE: "replica"})
    ann = {A_RUN: original_run_strategy(vm), A_SOURCE: source_cluster, A_PEER: peer_cluster,
           A_SYNCED: iso(now_utc())}
    if backend:
        ann[A_BACKEND] = backend
    meta.setdefault("annotations", {}).update(ann)
    return {"apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine", "metadata": meta, "spec": spec}


class Replicator:
    def __init__(self, clusters: Dict[str, KubeClient], settings: dict, log: Callable[[str], None],
                 cluster_config: Optional[Dict[str, dict]] = None, debug: bool = False):
        self.k = clusters
        self.settings = settings or {}
        self.sc_map: Dict[str, str] = self.settings.get("storage_class_map") or {}
        self.timeouts = {"stop": 300, "start": 600, "tmr": 300, "transfer": 1800, "stall": 120,
                         **(self.settings.get("timeouts") or {})}
        self.log = log
        self.debug = debug
        self.cluster_config = cluster_config or {}
        self._backends: Dict[str, StorageBackend] = {}
        self.stale_primary: Dict[Tuple[str, str], str] = {}   # (ns, vm) -> cluster with an outdated primary copy

    # ── backends ─────────────────────────────────────────────────────────────
    def backend(self, name: str) -> StorageBackend:
        if name not in self._backends:
            if name not in REGISTRY:
                raise ReplicationError(f"unknown storage backend '{name}' (known: {', '.join(sorted(REGISTRY))})")
            section = dict(self.settings.get(name) or {})
            if name == "netapp":   # 0.2 configs kept policy/schedule at the top of `replication:`
                for key in ("policy", "schedule"):
                    if key in self.settings and key not in section:
                        section[key] = self.settings[key]
            self._backends[name] = REGISTRY[name](self.k, section, self.timeouts, self.log, sc_map=self.sc_map,
                                                  cluster_config=self.cluster_config, debug=self.debug)
        return self._backends[name]

    def detect_backend(self, cluster: str, ns: str, vm: dict) -> StorageBackend:
        """Backend for this VM: config override > annotation on the VM > CSI driver of its PVs on `cluster`."""
        if self.settings.get("backend"):
            return self.backend(self.settings["backend"])
        ann = (vm["metadata"].get("annotations") or {}).get(A_BACKEND)
        if ann:
            return self.backend(ann)
        return self.detect_backend_claims(cluster, ns, vm_claims(vm), vm["metadata"]["name"])

    def unit_backend(self, cluster: str, unit: Unit) -> StorageBackend:
        """Backend of a unit: config override > recorded on a member VM > CSI drivers of all its PVCs."""
        if self.settings.get("backend"):
            return self.backend(self.settings["backend"])
        for m in unit.vms:
            v = self.k[cluster].get("vm", unit.ns, m)
            ann = ((v or {}).get("metadata", {}).get("annotations") or {}).get(A_BACKEND)
            if ann:
                return self.backend(ann)
        return self.detect_backend_claims(cluster, unit.ns, unit.claims, unit.describe())

    def detect_backend_claims(self, cluster: str, ns: str, claims: List[str], what: str) -> StorageBackend:
        vm = {"metadata": {"name": what}}
        k = self.k[cluster]
        found = set()
        for c in claims:
            pvc = k.get("pvc", ns, c)
            pv = k.get("pv", None, pvc["spec"]["volumeName"]) if pvc and pvc["spec"].get("volumeName") else None
            drv = ((pv or {}).get("spec", {}).get("csi") or {}).get("driver")
            name = backend_for_driver(drv)
            if not name:
                raise ReplicationError(f"{cluster}: PVC {ns}/{c} uses the '{drv}' CSI driver, which no Harvey storage "
                                       f"backend replicates (supported: " + ", ".join(
                                           f"{n} ({'/'.join(b.drivers)})" for n, b in sorted(REGISTRY.items())) + ").")
            found.add(name)
        if not found:
            raise ReplicationError(f"{ns}/{vm['metadata']['name']} has no PVC-backed disks to replicate.")
        if len(found) > 1:
            raise ReplicationError(f"{ns}/{vm['metadata']['name']} uses more than one storage backend "
                                   f"({', '.join(sorted(found))}). Harvey replicates each VM or recovery group with "
                                   f"one backend, so please keep its disks on one backend.")
        return self.backend(found.pop())

    def backend_of(self, ns: str, vm_name: str, *clusters: str) -> StorageBackend:
        """Backend of a protected VM, from the first cluster that has the VM."""
        last_err: Optional[Exception] = None
        for c in clusters:
            try:
                v = self.k[c].get("vm", ns, vm_name)
            except KubeError as e:
                last_err = e
                continue
            if v is not None:
                return self.detect_backend(c, ns, v)
        raise ReplicationError(f"VM {ns}/{vm_name} not found on {', '.join(clusters)}"
                               + (f" ({last_err})" if last_err else ""))

    # ── helpers ──────────────────────────────────────────────────────────────
    def _get_vm(self, cluster: str, ns: str, vm: str, required: bool = True) -> Optional[dict]:
        obj = self.k[cluster].get("vm", ns, vm)
        if obj is None and required:
            raise ReplicationError(f"VM {ns}/{vm} not found on {cluster}")
        return obj

    def _labels(self, vm: str) -> dict:
        return {L_MANAGED: "true", L_VM: vm}

    def _set_role(self, cluster: str, ns: str, vm_name: str, role: str, peer: str, extra_ann: Optional[dict] = None):
        ann = {A_PEER: peer, A_PRIMARY_SINCE: iso(now_utc()) if role == "primary" else None}
        ann.update(extra_ann or {})
        self.k[cluster].merge_patch("vm", ns, vm_name, {"metadata": {
            "labels": {L_MANAGED: "true", L_VM: vm_name, L_ROLE: role}, "annotations": ann}})

    # ── metadata sync ────────────────────────────────────────────────────────
    def sync_metadata(self, ns: str, vm_name: str, src: str, dst: str, vm: Optional[dict] = None,
                      backend: Optional[StorageBackend] = None, unit: Optional[Unit] = None) -> List[str]:
        """Copies VM spec + Secrets + ConfigMaps + NADs from src to dst. The dst VM always stays Halted."""
        ks, kd = self.k[src], self.k[dst]
        vm = vm or self._get_vm(src, ns, vm_name)
        unit = unit or self.unit(src, ns, vm_name)
        backend = backend or self.unit_backend(src, unit)
        drop = backend.drop_annotation_prefixes
        actions = []
        kd.ensure_namespace(ns)

        for s in vm_secret_names(vm):
            obj = ks.get("secret", ns, s)
            if obj is None:
                actions.append(f"WARN secret {s} missing on {src}")
                continue
            body = {"apiVersion": "v1", "kind": "Secret", "type": obj.get("type", "Opaque"),
                    "metadata": clean_meta(obj["metadata"], extra_drop=drop), "data": obj.get("data", {})}
            body["metadata"].setdefault("labels", {}).update(self._labels(vm_name))
            actions.append(f"secret/{s} {kd.apply('secret', ns, body)}")

        for c in vm_configmap_names(vm):
            obj = ks.get("configmap", ns, c)
            if obj is None:
                actions.append(f"WARN configmap {c} missing on {src}")
                continue
            body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": clean_meta(obj["metadata"], extra_drop=drop),
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
                    "metadata": clean_meta(src_nad["metadata"]), "spec": src_nad["spec"]}
            body["metadata"].setdefault("labels", {})[L_MANAGED] = "true"
            kd.create("nad", nns, body)
            actions.append(f"nad/{nns}/{nname} created")

        cur = kd.get("vm", ns, vm_name)
        if cur is not None and (cur["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
            raise ReplicationError(f"{dst} holds the primary copy of {ns}/{vm_name}, so Harvey won't overwrite it "
                                   f"(sync copies from the primary to the replica).")
        if cur is not None and vm_status(cur) not in ("Stopped", "Halted", "Unknown"):
            raise ReplicationError(f"The replica of {ns}/{vm_name} on {dst} is {vm_status(cur)}. Please stop it; "
                                   f"replicas stay stopped until a failover.")
        body = build_replica_vm(vm, src, src, backend=backend.name, extra_drop=drop)
        body["metadata"]["annotations"].update(self._unit_ann(unit))
        actions.append(f"vm/{vm_name} {kd.apply('vm', ns, body)} (Halted)")
        return actions

    # ── units ────────────────────────────────────────────────────────────────
    def unit(self, cluster: str, ns: str, vm_name: Optional[str] = None, group: Optional[str] = None) -> Unit:
        """The protection unit (recovery group, or the VM alone) as seen on `cluster`; enforces the shared-disk
        guard."""
        return resolve_unit(self.k[cluster], ns, vm_name=vm_name, group=group, cluster=cluster)

    def _unit_ann(self, unit: Unit) -> dict:
        return {A_UNIT: unit.name, A_UNIT_CLAIMS: json.dumps(unit.claims)}

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, ns: str, vm_name: Optional[str], src: str, dst: str, wait_init: bool = False,
                group: Optional[str] = None) -> List[str]:
        """Protects the VM's unit: the VM alone, or its whole recovery group (all member VMs, their disks once,
        and the group's standalone PVCs)."""
        kd = self.k[dst]
        unit = self.unit(src, ns, vm_name, group)
        for m in unit.vms:
            cur_dst = kd.get("vm", ns, m)
            if cur_dst is not None and (cur_dst["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
                raise ReplicationError(f"{ns}/{m} is primary on {dst}. Please run protect from {dst} instead.")
        if not unit.claims:
            raise ReplicationError(f"{unit.describe()} has no PVC-backed disks to replicate.")
        be = self.unit_backend(src, unit)
        out = [f"storage backend: {be.name} ({be.engine})"]
        if unit.group:
            line = f"recovery group {unit.group}: VMs {', '.join(unit.vms) or '-'}; {len(unit.claims)} volume(s)"
            if unit.standalone:
                line += f", standalone PVC(s): {', '.join(unit.standalone)}"
            out.append(line)
        kd.ensure_namespace(ns)
        out += be.protect(src, dst, ns, unit.name, unit.claims, wait_init)
        for m in unit.vms:
            vm = self._get_vm(src, ns, m)
            out += [f"{dst}: {a}" for a in self.sync_metadata(ns, m, src, dst, vm=vm, backend=be, unit=unit)]
            self._set_role(src, ns, m, "primary", dst, {A_BACKEND: be.name, **self._unit_ann(unit)})
        return out

    def wait_initialized(self, src: str, dst: str, ns: str, vm_name: str):
        unit = self.unit(src, ns, vm_name)
        self.unit_backend(src, unit).wait_initialized(src, dst, ns, unit.name, unit.claims)

    # ── status ───────────────────────────────────────────────────────────────
    def protected_vms(self, namespace: Optional[str] = None) -> List[Tuple[str, str]]:
        """Protected VMs whose peer is one of the clusters of this Replicator (a cluster can be paired with
        several sites; VMs protected towards a third site are not ours)."""
        found = set()
        for name, k in self.k.items():
            try:
                for v in k.list("vm", namespace, label_selector=f"{L_MANAGED}=true"):
                    peer = (v["metadata"].get("annotations") or {}).get(A_PEER)
                    if peer and peer != name and peer not in self.k:
                        continue
                    found.add((v["metadata"]["namespace"], v["metadata"]["name"]))
            except KubeError:
                pass
        return sorted(found)

    def direction(self, ns: str, vm_name: str, a: str, b: str) -> Tuple[Optional[str], Optional[str]]:
        """(primary, replica) by VM role labels.

        Both copies claim primary after an UNPLANNED failover (the old site could not be demoted while it was
        down): the copy that became primary most recently (annotation harvey.io/primary-since) wins and the
        other one is reported in self.stale_primary until `harvey reprotect` demotes it."""
        roles, since = {}, {}
        for c in (a, b):
            try:
                v = self.k[c].get("vm", ns, vm_name)
            except KubeError:
                v = None
            md = (v or {}).get("metadata", {})
            roles[c] = (md.get("labels") or {}).get(L_ROLE)
            since[c] = (md.get("annotations") or {}).get(A_PRIMARY_SINCE) or ""
        self.stale_primary.pop((ns, vm_name), None)
        if roles[a] == "primary" and roles[b] != "primary":
            return a, b
        if roles[b] == "primary" and roles[a] != "primary":
            return b, a
        if roles[a] == roles[b] == "primary" and since[a] != since[b] and (since[a] or since[b]):
            new, old = (a, b) if since[a] > since[b] else (b, a)
            self.stale_primary[(ns, vm_name)] = old
            return new, old
        return None, None

    def volume_status(self, ns: str, vm_name: str, primary: str, replica: str) -> List[VolumeStatus]:
        """Rows for every volume of the VM's unit (its recovery group's volumes, for a member)."""
        unit = self.unit(primary, ns, vm_name)
        return self.unit_backend(primary, unit).volume_status(primary, replica, ns, unit.name, unit.claims)

    def expand_plan(self, plan: List[Tuple[str, str, int, int]], cluster: str) -> Tuple[list, List[str]]:
        """Adds the missing members of every recovery group in the plan (a group fails over as a whole).
        Returns (plan, notes). Raises if a VM shares a disk outside its group."""
        have = {(ns, vm) for ns, vm, _, _ in plan}
        out, notes = list(plan), []
        for ns, vm, _, _ in plan:
            unit = self.unit(cluster, ns, vm)
            for m in unit.vms:
                if (ns, m) in have:
                    continue
                v = self._get_vm(cluster, ns, m)
                lab = v["metadata"].get("labels") or {}
                try:
                    prio, delay = int(lab.get("failover-priority", 0)), int(lab.get("failover-startup-time", 0))
                except ValueError:
                    prio, delay = 0, 0
                out.append((ns, m, prio, delay))
                have.add((ns, m))
                notes.append(f"{ns}/{m} added: it is in recovery group {unit.group} with {vm}")
        return out, notes

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

    # ── failover ─────────────────────────────────────────────────────────────
    def failover(self, plan: List[Tuple[str, str, int, int]], src: str, dst: str, unplanned: bool = False,
                 force: bool = False, hooks_cb: Optional[Callable[[str], bool]] = None) -> FailoverResult:
        """plan = [(namespace, vm, priority, startup_delay)]. Higher priority = stopped last, started first.
        Storage is synced and promoted per unit (a recovery group as a whole), after all its VMs stopped."""
        res = FailoverResult()
        t_start = time.time()
        ks, kd = self.k[src], self.k[dst]
        hooks_cb = hooks_cb or (lambda phase: True)
        src_up = ks.reachable() if ks else False

        # ── preflight ──
        self.log("[bold]Preflight[/bold]")
        plan, notes = self.expand_plan(plan, dst)
        for n in notes:
            self.log(f"  [cyan]{n}[/cyan]")
        units: Dict[Tuple[str, str], Unit] = {}
        unit_of: Dict[Tuple[str, str], Tuple[str, str]] = {}
        be_of: Dict[Tuple[str, str], StorageBackend] = {}
        for ns, vm, _, _ in plan:
            dvm = self._get_vm(dst, ns, vm)
            if (dvm["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
                raise ReplicationError(f"{ns}/{vm} is already primary on {dst}")
            unit = self.unit(dst, ns, vm)
            unit_of[(ns, vm)] = unit.key
            if unit.key not in units:
                units[unit.key] = unit
                be = self.unit_backend(dst, unit)
                be.preflight(src, dst, ns, unit.name, unit.claims, src_up)
                be_of[unit.key] = be
                self.log(f"  {unit.describe()}: {len(unit.claims)} volume(s) ready ({be.engine})")
            if src_up:
                s = vm_status(ks.get("vm", ns, vm))
                if unplanned and s == "Running" and not force:
                    raise ReplicationError(f"{ns}/{vm} is still running on {src}, so an unplanned failover would cause a "
                                           f"split brain. Please stop it, use a planned failover, or add --force.")
            elif not unplanned:
                raise ReplicationError(f"{src} isn't reachable. To fail over anyway, please use --unplanned (this "
                                       f"accepts data loss up to the replication lag).")
        if not hooks_cb("pre_failover"):
            raise ReplicationError("The pre_failover hook stopped the failover.")

        ok_units = set()
        if not unplanned:
            # ── metadata sync (catch spec changes made since the last sync) ──
            self.log("[bold]Metadata sync[/bold]")
            for ns, vm, _, _ in plan:
                try:
                    self.sync_metadata(ns, vm, src, dst, backend=be_of[unit_of[(ns, vm)]])
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

            # ── final sync: once per unit, only when ALL of its VMs stopped ──
            t = time.time()
            self.log("[bold]Final sync[/bold]")
            for key, unit in units.items():
                members = [(unit.ns, m) for m in unit.vms]
                if not all(m in stop_times for m in members):
                    res.failed.append((unit.describe(), "final-sync", "not every VM of the unit stopped"))
                    self.log(f"  [red]✗ {unit.describe()}: not every VM stopped, so Harvey didn't promote it.[/red]")
                    continue
                last_stop = max([stop_times[m] for m in members] or [now_utc()])
                try:
                    for c, nbytes, secs in be_of[key].final_sync(src, dst, unit.ns, unit.name, unit.claims,
                                                                 last_stop):
                        res.synced.append((unit.describe(), c, nbytes, round(secs, 1)))
                        self.log(f"  [green]✓[/green] {unit.ns}/{c}: {nbytes} B in {secs:.1f}s")
                    ok_units.add(key)
                    for m in unit.vms:
                        res.rpo[f"{unit.ns}/{m}"] = 0
                except Exception as e:   # backend-specific errors (ONTAP, Stork, ...) must not abort the others
                    res.failed.append((unit.describe(), "final-sync", str(e)))
                    self.log(f"  [red]✗ {unit.describe()}: {e}; Harvey didn't promote it.[/red]")
            res.phases["final_sync"] = int(time.time() - t)
        else:
            # data loss window = age of the newest replicated recovery point, per unit (worst volume)
            for key, unit in units.items():
                ok_units.add(key)
                try:
                    age = be_of[key].recovery_point_age(src, dst, unit.ns, unit.name, unit.claims, src_up)
                except Exception:
                    age = None
                for m in unit.vms:
                    res.rpo[f"{unit.ns}/{m}"] = age

        # ── promote: once per unit ──
        t = time.time()
        self.log("[bold]Promote on " + dst + "[/bold]")
        promoted_units = set()
        for key in [k for k in units if k in ok_units]:
            unit = units[key]
            try:
                be_of[key].promote(src, dst, unit.ns, unit.name, unit.claims, src_up, unplanned)
                promoted_units.add(key)
                res.promoted += [f"{unit.ns}/{m}" for m in unit.vms]
                self.log(f"  [green]✓[/green] {unit.describe()}: {len(unit.claims)} volume(s) read-write")
            except Exception as e:
                res.failed.append((unit.describe(), "promote", str(e)))
                self.log(f"  [red]✗ {unit.describe()}: {e}[/red]")
        res.phases["promote"] = int(time.time() - t)
        if not hooks_cb("pre_start"):
            raise ReplicationError("The pre_start hook stopped the failover (the volumes are already promoted).")

        # ── start ──
        t = time.time()
        self.log("[bold]Start on " + dst + "[/bold]")
        to_start = [p for p in plan if unit_of[(p[0], p[1])] in promoted_units]
        for ns, vm, prio, delay in sorted(to_start, key=lambda x: x[2], reverse=True):
            unit = units[unit_of[(ns, vm)]]
            try:
                # the promoted side is the primary from now on, even if the VM is slow to boot
                ann = {A_BACKEND: be_of[unit.key].name}
                ann.update(self._unit_ann(unit))
                self._set_role(dst, ns, vm, "primary", src, ann)
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
    def reprotect(self, ns: str, vm_name: Optional[str], new_src: str, new_dst: str, wait_init: bool = False,
                  group: Optional[str] = None) -> List[str]:
        """After a failover to new_src: make new_dst a replica of new_src again, for the VM's whole unit.
        Overwrites new_dst's copy of the disks with new_src's."""
        kd = self.k[new_dst]
        unit = self.unit(new_src, ns, vm_name, group)
        out: List[str] = []
        if unit.group:
            out.append(f"recovery group {unit.group}: VMs {', '.join(unit.vms)}")
        for m in unit.vms:
            vm = self._get_vm(new_src, ns, m)
            if (vm["metadata"].get("labels") or {}).get(L_ROLE) != "primary":
                raise ReplicationError(f"{ns}/{m} isn't primary on {new_src}; reprotect runs from the current primary.")
            dvm = self._get_vm(new_dst, ns, m, required=False)
            if dvm is None:
                # an earlier reprotect removed it before replacing the disks; the metadata sync below recreates it
                out.append(f"{new_dst}: {m}: no VM object (an interrupted reprotect removed it); recreating it")
                continue
            if vm_status(dvm) not in ("Stopped", "Halted"):
                raise ReplicationError(f"{ns}/{m} is {vm_status(dvm)} on {new_dst}. Please stop it first, because "
                                       f"reprotect overwrites its disks.")
            dmd = dvm["metadata"]
            if (dmd.get("labels") or {}).get(L_ROLE) == "primary":
                mine = (vm["metadata"].get("annotations") or {}).get(A_PRIMARY_SINCE) or ""
                theirs = (dmd.get("annotations") or {}).get(A_PRIMARY_SINCE) or ""
                if not mine or mine <= theirs:
                    raise ReplicationError(f"Both sites claim to be primary for {ns}/{m}, and {new_src} isn't the newer "
                                           f"one ({mine or '?'} vs {theirs or '?'}). Please resolve this manually "
                                           f"before you reprotect.")
                # stale primary left behind by an unplanned failover: demote it before overwriting its disks
                self._set_role(new_dst, ns, m, "replica", new_src, {A_REPROTECT: "true"})
                out.append(f"{new_dst}: {m}: stale primary copy (unplanned failover) demoted to replica")
        be = self.unit_backend(new_src, unit)
        out += be.reprotect(new_src, new_dst, ns, unit.name, unit.claims, wait_init)
        for m in unit.vms:
            vm = self._get_vm(new_src, ns, m)
            out += [f"{new_dst}: {a}" for a in self.sync_metadata(ns, m, new_src, new_dst, vm=vm, backend=be,
                                                                  unit=unit)]
            kd.merge_patch("vm", ns, m, {"metadata": {"annotations": {A_REPROTECT: None}}})
        return out

    # ── unprotect ────────────────────────────────────────────────────────────
    def unprotect(self, ns: str, vm_name: Optional[str], primary: str, replica: str, delete_replica: bool,
                  group: Optional[str] = None) -> List[str]:
        """Stops replication for the VM's whole unit (a recovery group can't be half protected)."""
        kp, kr = self.k[primary], self.k[replica]
        unit = self.unit(primary, ns, vm_name, group)
        be = self.unit_backend(primary, unit)
        out = be.unprotect(primary, replica, ns, unit.name, unit.claims)
        if delete_replica:
            for m in unit.vms:
                rvm = kr.get("vm", ns, m)
                if rvm is not None:
                    if vm_status(rvm) not in ("Stopped", "Halted"):
                        raise ReplicationError(f"The replica of {ns}/{m} on {replica} is {vm_status(rvm)}, so Harvey didn't "
                                       f"delete it.")
                    kr.delete("vm", ns, m)
                    out.append(f"{replica}: vm/{m} deleted")
            deleter = getattr(be, "delete_replica_volumes", None)
            if deleter:
                out += deleter(replica, ns, unit.claims)
            else:
                for c in unit.claims:
                    if kr.get("pvc", ns, c) is not None:
                        kr.delete("pvc", ns, c)
                        out.append(f"{replica}: pvc/{c} deleted")
            for m in unit.vms:
                for s in vm_secret_names(self._get_vm(primary, ns, m)):
                    obj = kr.get("secret", ns, s)
                    if obj is not None and (obj["metadata"].get("labels") or {}).get(L_MANAGED) == "true":
                        kr.delete("secret", ns, s)
                        out.append(f"{replica}: secret/{s} deleted")
        for m in unit.vms:
            kp.merge_patch("vm", ns, m, {"metadata": {
                "labels": {L_MANAGED: None, L_VM: None, L_ROLE: None},
                "annotations": {A_PEER: None, A_RUN: None, A_REPROTECT: None, A_BACKEND: None,
                                A_PRIMARY_SINCE: None, A_UNIT: None, A_UNIT_CLAIMS: None}}})
        return out
