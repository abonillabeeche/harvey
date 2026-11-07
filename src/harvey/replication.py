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
import time
from typing import Callable, Dict, List, Optional, Tuple

from .backends import REGISTRY, StorageBackend, backend_for_driver
from .common import (A_BACKEND, A_PEER, A_PRIMARY_SINCE, A_REPROTECT, A_RUN, A_SOURCE, A_SYNCED, L_MANAGED, L_ROLE, L_VM,
                     FailoverResult, ReplicationError, VolumeStatus, clean_meta, iso, now_utc)
from .kube import KubeClient, KubeError

__all__ = ["A_BACKEND", "A_PEER", "A_REPROTECT", "A_RUN", "L_MANAGED", "L_ROLE", "L_VM", "FailoverResult",
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
        k = self.k[cluster]
        found = set()
        for c in vm_claims(vm):
            pvc = k.get("pvc", ns, c)
            pv = k.get("pv", None, pvc["spec"]["volumeName"]) if pvc and pvc["spec"].get("volumeName") else None
            drv = ((pv or {}).get("spec", {}).get("csi") or {}).get("driver")
            name = backend_for_driver(drv)
            if not name:
                raise ReplicationError(f"{cluster}: PVC {ns}/{c} uses '{drv}' — no Harvey storage backend replicates "
                                       f"it (supported: " + ", ".join(
                                           f"{n} ({'/'.join(b.drivers)})" for n, b in sorted(REGISTRY.items())) + ")")
            found.add(name)
        if not found:
            raise ReplicationError(f"{ns}/{vm['metadata']['name']} has no PVC volumes")
        if len(found) > 1:
            raise ReplicationError(f"{ns}/{vm['metadata']['name']} mixes storage backends {sorted(found)}; "
                                   f"one VM must be replicated by one engine")
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
                      backend: Optional[StorageBackend] = None) -> List[str]:
        """Copies VM spec + Secrets + ConfigMaps + NADs from src to dst. The dst VM always stays Halted."""
        ks, kd = self.k[src], self.k[dst]
        vm = vm or self._get_vm(src, ns, vm_name)
        backend = backend or self.detect_backend(src, ns, vm)
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
            raise ReplicationError(f"{dst} holds the PRIMARY copy of {ns}/{vm_name}; refusing to overwrite it "
                                   f"(sync runs from the primary to the replica)")
        if cur is not None and vm_status(cur) not in ("Stopped", "Halted", "Unknown"):
            raise ReplicationError(f"replica {ns}/{vm_name} on {dst} is {vm_status(cur)}; it must be stopped")
        body = build_replica_vm(vm, src, src, backend=backend.name, extra_drop=drop)
        actions.append(f"vm/{vm_name} {kd.apply('vm', ns, body)} (Halted)")
        return actions

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, ns: str, vm_name: str, src: str, dst: str, wait_init: bool = False) -> List[str]:
        kd = self.k[dst]
        vm = self._get_vm(src, ns, vm_name)
        cur_dst = kd.get("vm", ns, vm_name)
        if cur_dst is not None and (cur_dst["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
            raise ReplicationError(f"{ns}/{vm_name} is primary on {dst}; protect from {dst} instead")
        claims = vm_claims(vm)
        if not claims:
            raise ReplicationError(f"{ns}/{vm_name} has no PVC volumes")
        be = self.detect_backend(src, ns, vm)
        out = [f"storage backend: {be.name} ({be.engine})"]
        kd.ensure_namespace(ns)
        out += be.protect(src, dst, ns, vm_name, claims, wait_init)
        out += [f"{dst}: {a}" for a in self.sync_metadata(ns, vm_name, src, dst, vm=vm, backend=be)]
        self._set_role(src, ns, vm_name, "primary", dst, {A_BACKEND: be.name})
        return out

    def wait_initialized(self, src: str, dst: str, ns: str, vm_name: str):
        vm = self._get_vm(src, ns, vm_name)
        self.detect_backend(src, ns, vm).wait_initialized(src, dst, ns, vm_name, vm_claims(vm))

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
        vm = self._get_vm(primary, ns, vm_name)
        be = self.detect_backend(primary, ns, vm)
        return be.volume_status(primary, replica, ns, vm_name, vm_claims(vm))

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
        """plan = [(namespace, vm, priority, startup_delay)]. Higher priority = stopped last, started first."""
        res = FailoverResult()
        t_start = time.time()
        ks, kd = self.k[src], self.k[dst]
        hooks_cb = hooks_cb or (lambda phase: True)
        src_up = ks.reachable() if ks else False

        # ── preflight ──
        self.log("[bold]Preflight[/bold]")
        claims_of: Dict[Tuple[str, str], List[str]] = {}
        be_of: Dict[Tuple[str, str], StorageBackend] = {}
        for ns, vm, _, _ in plan:
            dvm = self._get_vm(dst, ns, vm)
            if (dvm["metadata"].get("labels") or {}).get(L_ROLE) == "primary":
                raise ReplicationError(f"{ns}/{vm} is already primary on {dst}")
            claims = vm_claims(dvm)
            be = self.detect_backend(dst, ns, dvm)
            be.preflight(src, dst, ns, vm, claims, src_up)
            claims_of[(ns, vm)] = claims
            be_of[(ns, vm)] = be
            if src_up:
                s = vm_status(ks.get("vm", ns, vm))
                if unplanned and s == "Running" and not force:
                    raise ReplicationError(f"{ns}/{vm} is still Running on {src}; an unplanned failover would "
                                           f"split-brain. Stop it, use a planned failover, or --force.")
            elif not unplanned:
                raise ReplicationError(f"{src} is unreachable: use --unplanned (accepts data loss up to the "
                                       f"replication lag)")
            self.log(f"  {ns}/{vm}: {len(claims)} volume(s) ready ({be.engine})")
        if not hooks_cb("pre_failover"):
            raise ReplicationError("pre_failover hook aborted")

        if not unplanned:
            # ── metadata sync (catch spec changes made since the last sync) ──
            self.log("[bold]Metadata sync[/bold]")
            for ns, vm, _, _ in plan:
                try:
                    self.sync_metadata(ns, vm, src, dst, backend=be_of[(ns, vm)])
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
            self.log("[bold]Final sync[/bold]")
            for ns, vm, _, _ in plan:
                if (ns, vm) not in stop_times:
                    continue
                try:
                    for c, nbytes, secs in be_of[(ns, vm)].final_sync(src, dst, ns, vm, claims_of[(ns, vm)],
                                                                      stop_times[(ns, vm)]):
                        res.synced.append((f"{ns}/{vm}", c, nbytes, round(secs, 1)))
                        self.log(f"  [green]✓[/green] {ns}/{c}: {nbytes} B in {secs:.1f}s")
                    res.rpo[f"{ns}/{vm}"] = 0
                except Exception as e:   # backend-specific errors (ONTAP, Stork, ...) must not abort the others
                    res.failed.append((f"{ns}/{vm}", "final-sync", str(e)))
                    self.log(f"  [red]✗ {ns}/{vm}: {e} — NOT promoting this VM[/red]")
                    stop_times.pop((ns, vm))
            res.phases["final_sync"] = int(time.time() - t)
            eligible = [p for p in plan if (p[0], p[1]) in stop_times]
        else:
            # data loss window = age of the newest replicated recovery point, per VM (worst volume)
            eligible = list(plan)
            for ns, vm, _, _ in plan:
                try:
                    res.rpo[f"{ns}/{vm}"] = be_of[(ns, vm)].recovery_point_age(src, dst, ns, vm,
                                                                               claims_of[(ns, vm)], src_up)
                except Exception:
                    res.rpo[f"{ns}/{vm}"] = None

        # ── promote ──
        t = time.time()
        self.log("[bold]Promote on " + dst + "[/bold]")
        promoted = []
        for ns, vm, prio, delay in eligible:
            try:
                be_of[(ns, vm)].promote(src, dst, ns, vm, claims_of[(ns, vm)], src_up, unplanned)
                res.promoted.append(f"{ns}/{vm}")
                promoted.append((ns, vm, prio, delay))
                self.log(f"  [green]✓[/green] {ns}/{vm}: {len(claims_of[(ns, vm)])} volume(s) read-write")
            except Exception as e:
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
                self._set_role(dst, ns, vm, "primary", src, {A_BACKEND: be_of[(ns, vm)].name})
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
        kd = self.k[new_dst]
        vm = self._get_vm(new_src, ns, vm_name)
        if (vm["metadata"].get("labels") or {}).get(L_ROLE) != "primary":
            raise ReplicationError(f"{ns}/{vm_name} is not primary on {new_src}")
        dvm = self._get_vm(new_dst, ns, vm_name)
        if vm_status(dvm) not in ("Stopped", "Halted"):
            raise ReplicationError(f"{ns}/{vm_name} is {vm_status(dvm)} on {new_dst}; it must be stopped "
                                   f"(its disks will be overwritten)")
        dmd = dvm["metadata"]
        if (dmd.get("labels") or {}).get(L_ROLE) == "primary":
            mine = (vm["metadata"].get("annotations") or {}).get(A_PRIMARY_SINCE) or ""
            theirs = (dmd.get("annotations") or {}).get(A_PRIMARY_SINCE) or ""
            if not mine or mine <= theirs:
                raise ReplicationError(f"both sites claim primary for {ns}/{vm_name} and {new_src} is not the "
                                       f"newer one ({mine or '?'} vs {theirs or '?'}); resolve by hand")
            # stale primary left behind by an unplanned failover: demote it before overwriting its disks
            self._set_role(new_dst, ns, vm_name, "replica", new_src, {A_REPROTECT: "true"})
        be = self.detect_backend(new_src, ns, vm)
        out = []
        if (dmd.get("labels") or {}).get(L_ROLE) == "primary":
            out.append(f"{new_dst}: stale primary copy (unplanned failover) demoted to replica")
        out += be.reprotect(new_src, new_dst, ns, vm_name, vm_claims(vm), wait_init)
        out += [f"{new_dst}: {a}" for a in self.sync_metadata(ns, vm_name, new_src, new_dst, vm=vm, backend=be)]
        kd.merge_patch("vm", ns, vm_name, {"metadata": {"annotations": {A_REPROTECT: None}}})
        return out

    # ── unprotect ────────────────────────────────────────────────────────────
    def unprotect(self, ns: str, vm_name: str, primary: str, replica: str, delete_replica: bool) -> List[str]:
        kp, kr = self.k[primary], self.k[replica]
        vm = self._get_vm(primary, ns, vm_name)
        be = self.detect_backend(primary, ns, vm)
        claims = vm_claims(vm)
        out = be.unprotect(primary, replica, ns, vm_name, claims)
        if delete_replica:
            rvm = kr.get("vm", ns, vm_name)
            if rvm is not None:
                if vm_status(rvm) not in ("Stopped", "Halted"):
                    raise ReplicationError(f"replica {ns}/{vm_name} on {replica} is {vm_status(rvm)}; not deleting")
                kr.delete("vm", ns, vm_name)
                out.append(f"{replica}: vm/{vm_name} deleted")
            deleter = getattr(be, "delete_replica_volumes", None)
            if deleter:
                out += deleter(replica, ns, claims)
            else:
                for c in claims:
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
            "annotations": {A_PEER: None, A_RUN: None, A_REPROTECT: None, A_BACKEND: None, A_PRIMARY_SINCE: None}}})
        return out
