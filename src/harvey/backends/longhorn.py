"""Longhorn backend: Longhorn disaster recovery (DR) volumes. V1 data engine: lab-tested. V2 data engine: ALPHA.

Longhorn has no cluster-to-cluster replication. Its DR story is backup based: the primary volume is backed up
to an S3/NFS backup target, and on the DR cluster a *DR volume* (a standby volume created from a backup)
keeps polling the same backup target and restores every new backup incrementally. Activating the DR volume
turns it into a normal volume. Harvey drives that, and moves the VM metadata as with the other backends.

Data path (per PVC of a protected VM; all Longhorn objects live in longhorn-system):
  both clusters   BackupTarget <target>   same URL + credentials (default name: harvey)
  source          Volume <pv-volume>      spec.backupTargetName=<target>, label
                                          recurring-job-group.longhorn.io/harvey-dr=enabled
                  RecurringJob harvey-backup-<N>m   task backup, cron */N, group harvey-dr
  destination     Volume <pv-volume>      Standby=true, fromBackup=<backup URL>, same size / engine /
                                          access mode; templates of the PV and PVC in annotations
                                          (Longhorn forbids PV/PVC on a standby volume)
  final sync      Snapshot + Backup of the stopped volume, BackupVolume sync on the destination, wait until
                  the DR volume's lastBackup is that backup (RPO 0)
  promote         activate (Standby=false, frontend blockdev), then create the PV and PVC from the templates.
                  An activated DR volume stops restoring, so a returning source can't overwrite it.
  reprotect       back up the new primary to the same target, delete the old primary's PVC/PV/volume and
                  recreate it as a DR volume of the new primary (a full restore).
  recovery point  snapshotCreatedAt of the last restored backup (known on the destination alone).

Config (`replication.longhorn:`):
  backup_target: harvey        # BackupTarget name on BOTH clusters (same URL); harvey points protected
                               # volumes at it, so their recurring and manual backups go there
  interval_minutes: 5          # recurring backup cron
  retain: 12                   # backups kept by the recurring job
  replicas: null               # DR volume replica count (default: same as the source)

Status: the V1 data engine path is lab-tested. V2 (SPDK) DR volumes are supported by Longhorn 1.12+ (not with
the sharded layout) but untested with Harvey, so V2 is ALPHA.
"""

from __future__ import annotations

import datetime
import json
import secrets
import time
from typing import Dict, List, Optional, Tuple

from ..common import L_MANAGED, L_VM, ReplicationError, VolumeStatus, iso, now_utc, parse_k8s_time
from ..kube import KubeError
from . import register
from .base import StorageBackend

LONGHORN = "driver.longhorn.io"
NS = "longhorn-system"
API = "longhorn.io/v1beta2"
GROUP = "harvey-dr"
L_GROUP = f"recurring-job-group.longhorn.io/{GROUP}"
L_BACKUP_VOLUME = "longhorn.io/backup-volume"
L_BACKUP_TARGET = "longhorn.io/backup-target"
A_PV = "harvey.io/pv-template"
A_PVC = "harvey.io/pvc-template"
A_SRC = "harvey.io/source-cluster"


@register
class LonghornBackend(StorageBackend):
    name = "longhorn"
    drivers = (LONGHORN,)
    drop_annotation_prefixes = ("longhorn.io/",)
    engine = "Longhorn DR volumes"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.target = self.settings.get("backup_target", "harvey")
        self.interval = int(self.settings.get("interval_minutes", 5))
        self.retain = int(self.settings.get("retain", 12))
        self.replicas = self.settings.get("replicas")

    # ── lookups ──────────────────────────────────────────────────────────────
    def _volume_name(self, cluster: str, ns: str, claim: str) -> str:
        k = self.k[cluster]
        pvc = k.get("pvc", ns, claim)
        if not pvc or not pvc["spec"].get("volumeName"):
            raise ReplicationError(f"{cluster}: PVC {ns}/{claim} isn't bound to a volume yet.")
        pv = k.get("pv", None, pvc["spec"]["volumeName"])
        return pv["spec"]["csi"]["volumeHandle"]

    def _dr_volume_name(self, ns: str, claim: str, dst: str) -> Optional[str]:
        """Name of the DR volume of `claim` on dst (same name as the source Longhorn volume; recorded in a label)."""
        for v in self.k[dst].list("lhvolume", NS, label_selector=f"{L_VM}-claim={ns}.{claim}"):
            return v["metadata"]["name"]
        return None

    def _check_target(self, cluster: str):
        bt = self.k[cluster].get("lhbackuptarget", NS, self.target)
        if bt is None:
            raise ReplicationError(
                f"{cluster}: Longhorn BackupTarget '{self.target}' not found. Please create a BackupTarget with this name, "
                f"the same URL and working credentials on both clusters (see docs/longhorn.md).")
        if not (bt.get("status") or {}).get("available"):
            raise ReplicationError(f"{cluster}: Longhorn BackupTarget '{self.target}' isn't available "
                                   f"({(bt.get('status') or {}).get('conditions')}). Please check its URL and credentials.")
        return bt["spec"].get("backupTargetURL")

    def _backup_volume(self, cluster: str, volume: str) -> Optional[dict]:
        for bv in self.k[cluster].list("lhbackupvolume", NS):
            s = bv.get("spec") or {}
            st = bv.get("status") or {}
            if (s.get("backupTargetName") == self.target and
                    (s.get("volumeName") == volume or st.get("volumeName") == volume)):
                return bv
        return None

    def _request_sync(self, cluster: str, volume: str):
        bv = self._backup_volume(cluster, volume)
        if bv is not None:
            self.k[cluster].merge_patch("lhbackupvolume", NS, bv["metadata"]["name"],
                                        {"spec": {"syncRequestedAt": iso(now_utc())}})
        elif self.k[cluster].get("lhbackuptarget", NS, self.target) is not None:
            # not discovered yet: ask the whole target to resync
            self.k[cluster].merge_patch("lhbackuptarget", NS, self.target,
                                        {"spec": {"syncRequestedAt": iso(now_utc())}})

    def _backup_time(self, cluster: str, backup: str) -> Optional[datetime.datetime]:
        b = self.k[cluster].get("lhbackup", NS, backup) if backup else None
        st = (b or {}).get("status") or {}
        return parse_k8s_time(st.get("snapshotCreatedAt") or st.get("backupCreatedAt"))

    # ── building blocks ──────────────────────────────────────────────────────
    def _schedule_backups(self, cluster: str, volume: str):
        k = self.k[cluster]
        job = f"harvey-backup-{self.interval}m"
        if k.get("lhrecurringjob", NS, job) is None:
            k.create("lhrecurringjob", NS, {
                "apiVersion": API, "kind": "RecurringJob",
                "metadata": {"name": job, "namespace": NS, "labels": {L_MANAGED: "true"}},
                "spec": {"name": job, "groups": [GROUP], "task": "backup", "cron": f"*/{self.interval} * * * *",
                         "retain": self.retain, "concurrency": 2, "labels": {L_MANAGED: "true"}}})
        k.merge_patch("lhvolume", NS, volume, {"metadata": {"labels": {L_GROUP: "enabled"}},
                                               "spec": {"backupTargetName": self.target}})

    def _unschedule_backups(self, cluster: str, volume: str):
        k = self.k[cluster]
        if k.get("lhvolume", NS, volume) is not None:
            k.merge_patch("lhvolume", NS, volume, {"metadata": {"labels": {L_GROUP: None}}})

    def backup_now(self, cluster: str, volume: str, tag: str) -> dict:
        """Snapshot + backup of `volume` (Longhorn attaches a detached volume for it). Returns the Backup CR."""
        k = self.k[cluster]
        stamp = now_utc().strftime("%Y%m%d%H%M%S")
        snap = f"harvey-{tag}-{stamp}-{volume[-8:]}"
        k.create("lhsnapshot", NS, {"apiVersion": API, "kind": "Snapshot",
                                    "metadata": {"name": snap, "namespace": NS, "labels": {L_MANAGED: "true"}},
                                    "spec": {"volume": volume, "createSnapshot": True}})
        k.wait_for(lambda: ((k.get("lhsnapshot", NS, snap) or {}).get("status") or {}).get("readyToUse"),
                   self.timeouts["tmr"], what=f"snapshot {snap}")
        bname = f"backup-{secrets.token_hex(8)}"      # Longhorn-style name; tag kept in a label
        k.create("lhbackup", NS, {"apiVersion": API, "kind": "Backup",
                                  "metadata": {"name": bname, "namespace": NS,
                                               "labels": {L_BACKUP_TARGET: self.target, L_BACKUP_VOLUME: volume,
                                                          L_MANAGED: "true", "harvey.io/backup-kind": tag}},
                                  "spec": {"snapshotName": snap, "labels": {}, "backupMode": "incremental"}})
        deadline = time.time() + self.timeouts["transfer"]
        while time.time() < deadline:
            b = k.get("lhbackup", NS, bname) or {}
            st = (b.get("status") or {}).get("state")
            if st == "Completed":
                return b
            if st == "Error":
                raise ReplicationError(f"{cluster}: backup {bname} of {volume} failed: "
                                       f"{(b.get('status') or {}).get('error')}")
            time.sleep(3)
        raise ReplicationError(f"{cluster}: backup {bname} of {volume} didn't complete within "
                               f"{self.timeouts['transfer']}s.")

    def wait_restored(self, dst: str, volume: str, backup: str):
        """Wait until the DR volume on dst has restored `backup` (polls the backup volume sync)."""
        k = self.k[dst]
        deadline = time.time() + self.timeouts["transfer"]
        last_sync = 0.0
        while time.time() < deadline:
            if time.time() - last_sync > 30:
                try:
                    self._request_sync(dst, volume)
                except KubeError:
                    pass
                last_sync = time.time()
            v = k.get("lhvolume", NS, volume) or {}
            if self._restored_at_least(dst, volume, backup):
                return v
            if (v.get("status") or {}).get("robustness") == "faulted":
                why = next((c.get("message") for c in (v.get("status") or {}).get("conditions") or []
                            if c.get("type") == "Restore" and c.get("status") == "False" and c.get("message")), "")
                raise ReplicationError(
                    f"{dst}: DR volume {volume} is faulted{f' ({why})' if why else ''}. Longhorn V2 DR volumes before "
                    f"1.13.1 and 1.12.2 fault on a backup-store lock conflict instead of retrying (longhorn#14085). "
                    f"Please rerun the command to replace it with a new DR volume.")
            time.sleep(5)
        raise ReplicationError(f"{dst}: DR volume {volume} didn't restore backup {backup} within "
                               f"{self.timeouts['transfer']}s (latest backup: "
                               f"{((k.get('lhvolume', NS, volume) or {}).get('status') or {}).get('lastBackup')}).")

    def wait_backup_visible(self, dst: str, volume: str, backup: str, block_size: Optional[str] = None):
        """Longhorn's admission webhook needs the Backup CR on the DR cluster before a DR volume can reference it,
        so wait for the backup target sync to bring it over.

        A Backup CR that a cluster learns about through a backup target sync has spec.backupBlockSize = -1
        ("unknown"); seen both ways between Longhorn 1.12.1 and 1.13.0. The volume mutator copies that into a new DR
        volume and the CRD schema rejects it (only 2Mi/16Mi are valid; the mutator's fallback covers 0, not -1).
        Set the source's real block size on the synced Backup CR first."""
        k = self.k[dst]
        deadline = time.time() + self.timeouts["transfer"]
        last_sync = 0.0
        while time.time() < deadline:
            if time.time() - last_sync > 20:
                try:
                    self._request_sync(dst, volume)
                except KubeError:
                    pass
                last_sync = time.time()
            b = k.get("lhbackup", NS, backup)
            labels = ((b or {}).get("metadata") or {}).get("labels") or {}
            # the volume mutator reads the synced Backup CR's status.backupTargetName (filled in by the DR cluster's
            # backup controller a little after the CR appears) and then the BackupVolume CR, which for a brand-new
            # volume can also show up after the Backup
            if b is not None and ((b.get("status") or {}).get("backupTargetName") or "") and \
                    labels.get("backup-volume") and self._backup_volume(dst, volume) is not None:
                cur = str((b.get("spec") or {}).get("backupBlockSize") or "")
                if cur not in ("2097152", "16777216"):
                    k.merge_patch("lhbackup", NS, backup, {"spec": {"backupBlockSize": str(block_size or 2097152)}})
                    self.log(f"  [yellow]{dst}: backup {backup} synced with backupBlockSize={cur or 'unset'}; set to "
                             f"{block_size or 2097152} (synced backups carry -1)[/yellow]")
                return
            time.sleep(3)
        raise ReplicationError(f"{dst}: backup {backup} isn't visible through BackupTarget '{self.target}' after "
                               f"{self.timeouts['transfer']}s. Please check that both clusters can reach the target.")

    def _engine_restored(self, cluster: str, volume: str) -> Optional[str]:
        """The backup the volume's engine reports as restored. The volume's status.lastBackup only says which
        backup is the newest one to restore: seen with the V2 engine, it moved to the new backup while the
        engine never restored it (V2 restore stuck on the host's NVMe-TCP initiator). Only the engine's
        status.lastRestoredBackup proves the data is there."""
        for e in self.k[cluster].list("lhengine", NS, label_selector=f"longhornvolume={volume}"):
            r = (e.get("status") or {}).get("lastRestoredBackup")
            if r:
                return r
        return None

    def _restored_at_least(self, cluster: str, volume: str, backup: str) -> bool:
        """True when the DR volume's engine restored `backup` or a newer backup of the same volume. A DR volume
        always follows the newest backup on the target, so while Harvey waits, a scheduled backup can arrive and be
        restored instead of the one Harvey took (seen in reprotect: the 5-minute job on the new primary kept the DR
        volume moving past the reprotect backup). Backups are ordered by their snapshot time."""
        restored = self._engine_restored(cluster, volume)
        if not restored:
            return False
        if restored == backup:
            return True
        wanted, got = self._backup_time(cluster, backup), self._backup_time(cluster, restored)
        return bool(wanted and got and got >= wanted)

    def _is_dr_volume_of(self, cluster: str, volume: str, primary: str) -> bool:
        """True when `volume` on `cluster` is a standby DR volume that Harvey created from `primary`'s backups,
        so a reprotect that stopped half way can reuse it instead of replacing it with another full restore."""
        v = self.k[cluster].get("lhvolume", NS, volume) or {}
        return bool((v.get("spec") or {}).get("Standby")) and \
            ((v.get("metadata") or {}).get("annotations") or {}).get(A_SRC) == primary and \
            ((v.get("status") or {}).get("robustness") != "faulted")

    def _templates(self, cluster: str, ns: str, claim: str) -> Tuple[dict, dict]:
        """Clean PV and PVC specs to recreate on the DR site at promotion."""
        k = self.k[cluster]
        pvc = k.get("pvc", ns, claim)
        pv = k.get("pv", None, pvc["spec"]["volumeName"])
        pv_t = {"name": pv["metadata"]["name"],
                "spec": {key: pv["spec"][key] for key in ("accessModes", "capacity", "csi", "volumeMode",
                                                          "storageClassName", "persistentVolumeReclaimPolicy",
                                                          "mountOptions") if key in pv["spec"]}}
        pvc_t = {"labels": {k2: v for k2, v in (pvc["metadata"].get("labels") or {}).items()
                            if k2 == "harvey.io/recovery-group" or not k2.startswith("harvey.io/")},
                 "annotations": {k2: v for k2, v in (pvc["metadata"].get("annotations") or {}).items()
                                 if k2.startswith(("harvesterhci.io/owned-by", "field.cattle.io/"))},
                 "spec": {key: pvc["spec"][key] for key in ("accessModes", "volumeMode", "resources",
                                                            "storageClassName") if key in pvc["spec"]}}
        return pv_t, pvc_t

    def _create_dr_volume(self, src: str, dst: str, ns: str, vm: str, claim: str, volume: str, backup: dict):
        ks, kd = self.k[src], self.k[dst]
        sv = ks.get("lhvolume", NS, volume)
        s = sv["spec"]
        if s.get("backingImage") and kd.get("lhbackingimage", NS, s["backingImage"]) is None:
            raise ReplicationError(
                f"{dst}: Longhorn volume {volume} (V1) is built on backing image '{s['backingImage']}', which doesn't "
                f"exist on {dst} under that name. Harvester names backing images after the image UID, so importing or "
                f"restoring the image in Harvester creates a different name. Please restore the backing image on {dst} "
                f"as a Longhorn BackingImage named '{s['backingImage']}' (sourceType: restore), or use CDI-backed "
                f"images (see docs/longhorn.md).")
        if s.get("dataEngine") == "v2":
            self.log(f"  [yellow]{claim}: Longhorn V2 data engine. Longhorn 1.13.1 or later is recommended: earlier "
                     f"versions can fault V2 DR volumes (longhorn#14085).[/yellow]")
        pv_t, pvc_t = self._templates(src, ns, claim)
        body = {"apiVersion": API, "kind": "Volume",
                "metadata": {"name": volume, "namespace": NS,
                             "labels": {L_MANAGED: "true", L_VM: vm, f"{L_VM}-claim": f"{ns}.{claim}",
                                        L_BACKUP_VOLUME: volume},
                             "annotations": {A_PV: json.dumps(pv_t), A_PVC: json.dumps(pvc_t), A_SRC: src}},
                "spec": {"size": s["size"], "fromBackup": backup["status"]["url"], "Standby": True,
                         "frontend": "", "numberOfReplicas": int(self.replicas or s.get("numberOfReplicas", 3)),
                         "dataEngine": s.get("dataEngine", "v1"), "accessMode": s.get("accessMode", "rwo"),
                         "migratable": bool(s.get("migratable")), "dataLocality": s.get("dataLocality", "disabled"),
                         "backupTargetName": self.target, "encrypted": bool(s.get("encrypted"))}}
        # Longhorn 1.13's CRD schema requires a valid backupBlockSize before the webhook copies it from the backup
        body["spec"]["backupBlockSize"] = str((backup.get("spec") or {}).get("backupBlockSize")
                                              or s.get("backupBlockSize") or 2097152)
        for key in ("staleReplicaTimeout", "diskSelector", "nodeSelector", "replicaAutoBalance",
                    "unmapMarkSnapChainRemoved", "backupCompressionMethod"):
            if key in s:
                body["spec"][key] = s[key]
        kd.create("lhvolume", NS, body)

    def _release_vms_using(self, cluster: str, ns: str, claim: str, out: List[str]):
        """Harvester refuses to delete a PVC that a VM references, even a stopped one. Before replacing the old
        primary's disk, delete every (stopped) VM object that uses it (one VM, or several for a shared disk in a
        recovery group); the orchestrator recreates them from the new primary right after. Orphan propagation:
        nothing else is deleted with them."""
        k = self.k[cluster]
        for v in k.list("vm", ns):
            vols = v["spec"]["template"]["spec"].get("volumes", [])
            uses = any((x.get("persistentVolumeClaim") or {}).get("claimName") == claim or
                       (x.get("dataVolume") or {}).get("name") == claim for x in vols)
            if not uses:
                continue
            name = v["metadata"]["name"]
            status = (v.get("status") or {}).get("printableStatus")
            if status not in ("Stopped", "Halted"):
                raise ReplicationError(f"{cluster}: VM {ns}/{name} is {status}. Please stop it before Harvey replaces "
                                       f"disk {claim}.")
            k.delete("vm", ns, name, propagation="Orphan")
            k.wait_for(lambda: k.get("vm", ns, name) is None, self.timeouts["tmr"], what=f"VM {ns}/{name} removal")
            out.append(f"{cluster}: vm/{name} removed (stopped replica; recreated from the new primary below)")

    def _delete_volume(self, cluster: str, ns: str, claim: str, volume: str) -> List[str]:
        """Delete PVC, PV and Longhorn volume of a claim (used on the replica side)."""
        k = self.k[cluster]
        out = []
        if k.get("dv", ns, claim) is not None:      # CDI would otherwise recreate the PVC it owns
            k.delete("dv", ns, claim)
            out.append(f"{cluster}: datavolume/{claim} deleted")
        pvc = k.get("pvc", ns, claim)
        pvn = (pvc or {}).get("spec", {}).get("volumeName")
        if pvc is not None:
            k.delete("pvc", ns, claim)
            k.wait_for(lambda: k.get("pvc", ns, claim) is None, self.timeouts["tmr"], what=f"pvc {claim} removal")
            out.append(f"{cluster}: pvc/{claim} deleted")
        if pvn:
            try:
                k.wait_for(lambda: k.get("pv", None, pvn) is None, 120, what=f"pv {pvn} removal")
            except KubeError:
                k.delete("pv", None, pvn)
        if k.get("lhvolume", NS, volume) is not None:
            k.delete("lhvolume", NS, volume)
            k.wait_for(lambda: k.get("lhvolume", NS, volume) is None, self.timeouts["tmr"],
                       what=f"longhorn volume {volume} removal")
            out.append(f"{cluster}: longhorn volume {volume} deleted")
        return out

    # ── protect ──────────────────────────────────────────────────────────────
    def protect(self, src: str, dst: str, ns: str, vm: str, claims: List[str], wait_init: bool) -> List[str]:
        ks, kd = self.k[src], self.k[dst]
        self.check_claims(src, ns, claims)
        u1, u2 = self._check_target(src), self._check_target(dst)
        if u1 != u2:
            raise ReplicationError(f"BackupTarget '{self.target}' points to different locations ({src}: {u1}, "
                                   f"{dst}: {u2}). Please use the same URL on both clusters.")
        out = [f"backup target '{self.target}': {u1}"]
        vols = {c: self._volume_name(src, ns, c) for c in claims}
        for c, v in vols.items():
            existing = kd.get("lhvolume", NS, v)
            if existing is not None and not (existing["metadata"].get("labels") or {}).get(L_MANAGED):
                raise ReplicationError(f"{dst}: a Longhorn volume named {v} already exists and wasn't created by Harvey. "
                                       f"Please check what uses it before protecting this VM.")
        for c, v in vols.items():
            self._schedule_backups(src, v)
            out.append(f"{src}: volume {v} → backups to '{self.target}' every {self.interval} min")
        for c, v in vols.items():
            cur = kd.get("lhvolume", NS, v)
            if cur is not None and (cur.get("spec") or {}).get("Standby") and \
                    (cur.get("status") or {}).get("robustness") == "faulted":
                # a faulted standby DR volume never restores again (Longhorn V2 before 1.13.1 / 1.12.2 faults on a
                # backupstore lock conflict, longhorn#14085); it has no PV/PVC yet, so replace it
                self.log(f"  [yellow]{c}: DR volume {v} on {dst} is faulted; replacing it[/yellow]")
                kd.delete("lhvolume", NS, v)
                kd.wait_for(lambda: kd.get("lhvolume", NS, v) is None,
                            self.timeouts["tmr"], interval=3, what=f"deletion of faulted DR volume {v}")
                out.append(f"{dst}: faulted DR volume {v} deleted")
                cur = None
            if cur is not None:
                out.append(f"{dst}: DR volume {v} already present")
                continue
            self.log(f"  {c}: first backup of {v} ...")
            b = self.backup_now(src, v, "base")
            self.wait_backup_visible(dst, v, b["metadata"]["name"], (b.get("spec") or {}).get("backupBlockSize"))
            self._create_dr_volume(src, dst, ns, vm, c, v, b)
            out.append(f"{dst}: DR volume {v} created from {b['metadata']['name']} "
                       f"({(b.get('status') or {}).get('size', '?')} B)")
        for c, v in vols.items():
            st = (kd.get("lhvolume", NS, v) or {}).get("status") or {}
            if not st.get("lastBackup"):
                self.log(f"  waiting for {v} to finish its first restore on {dst} ...")
                kd.wait_for(lambda: ((kd.get("lhvolume", NS, v) or {}).get("status") or {}).get("lastBackup"),
                            self.timeouts["transfer"], interval=5, what=f"first restore of {v}")
        return out

    def wait_initialized(self, src: str, dst: str, ns: str, vm: str, claims: List[str]):
        for c in claims:
            v = self._volume_name(src, ns, c)
            kd = self.k[dst]
            kd.wait_for(lambda: ((kd.get("lhvolume", NS, v) or {}).get("status") or {}).get("lastBackup"),
                        self.timeouts["transfer"], interval=5, what=f"first restore of {v}")

    # ── status ───────────────────────────────────────────────────────────────
    def volume_status(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[VolumeStatus]:
        kp, kr = self.k[primary], self.k[replica]
        rows = []
        for c in claims:
            vs = VolumeStatus(claim=c)
            try:
                v = self._volume_name(primary, ns, c)
            except (ReplicationError, KubeError, TypeError) as e:
                vs.note = str(e)[:120]
                rows.append(vs)
                continue
            pvol = kp.get("lhvolume", NS, v) or {}
            plabels = pvol.get("metadata", {}).get("labels") or {}
            vs.src_handle = v
            vs.src_state = (f"backup/{self.interval}m" if plabels.get(L_GROUP) == "enabled" else "unscheduled")
            rvol = kr.get("lhvolume", NS, v) if kr else None
            rst = (rvol or {}).get("status") or {}
            if rvol is None:
                vs.dst_state, vs.healthy, vs.note = "-", False, f"no DR volume on {replica}"
                rows.append(vs)
                continue
            vs.dst_handle = v
            vs.dst_state = "standby" if rst.get("isStandby") else "active"
            last = rst.get("lastBackup") or ""
            pbv = self._backup_volume(primary, v)
            newest = ((pbv or {}).get("status") or {}).get("lastBackupName") or ""
            vs.recovery_point = self._backup_time(replica, last) or self._backup_time(primary, last)
            vs.lag_s = int((now_utc() - vs.recovery_point).total_seconds()) if vs.recovery_point else None
            if not rst.get("isStandby"):
                vs.repl_state = "activated"
            else:
                restored = self._engine_restored(replica, v)
                if rst.get("robustness") == "faulted":
                    vs.repl_state = "faulted"
                elif restored and restored != last:
                    vs.repl_state = f"restoring {last}"
                elif not restored and last:
                    vs.repl_state = "restore not confirmed"
                else:
                    vs.repl_state = "up to date" if last and last == newest else ("restoring" if last else "initial restore")
            vs.transfer = "idle" if last == newest else "restoring"
            vs.healthy = bool(rst.get("isStandby") and last and plabels.get(L_GROUP) == "enabled"
                              and rst.get("robustness") != "faulted" and self._engine_restored(replica, v))
            eng = pvol.get("spec", {}).get("dataEngine", "v1")
            vs.note = "v2 · alpha" if eng == "v2" else eng
            if rst.get("robustness") == "faulted":
                vs.note += " · faulted DR volume: rerun protect/reprotect to replace it"
            rows.append(vs)
        return rows

    # ── failover ─────────────────────────────────────────────────────────────
    def preflight(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool):
        kd = self.k[dst]
        for c in claims:
            v = self._dr_volume_name(ns, c, dst)
            if not v:
                raise ReplicationError(f"{dst}: no Harvey DR volume found for {ns}/{c}. Is {ns}/{vm} protected from "
                                       f"{src} to {dst}?")
            st = (kd.get("lhvolume", NS, v) or {}).get("status") or {}
            if not st.get("isStandby"):
                raise ReplicationError(f"{dst}: Longhorn volume {v} is not a standby DR volume (it may already be activated).")
            if not st.get("lastBackup"):
                raise ReplicationError(f"{dst}: DR volume {v} has not restored any backup yet. Please wait for the first "
                                       f"restore, or check harvey replication status.")

    def final_sync(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                   not_before: datetime.datetime) -> List[Tuple[str, int, float]]:
        out = []
        for c in claims:
            t0 = time.time()
            v = self._volume_name(src, ns, c)
            b = self.backup_now(src, v, "final")
            snap_t = parse_k8s_time((b.get("status") or {}).get("snapshotCreatedAt"))
            if snap_t and snap_t < not_before.replace(microsecond=0):
                raise ReplicationError(f"{c}: final backup snapshot {iso(snap_t)} is older than the stop time "
                                       f"{iso(not_before)}")
            self.wait_restored(dst, v, b["metadata"]["name"])
            nbytes = int((b.get("status") or {}).get("newlyUploadDataSize") or 0)
            out.append((c, nbytes, time.time() - t0))
        return out

    def recovery_point_age(self, src: str, dst: str, ns: str, vm: str, claims: List[str],
                           src_up: bool) -> Optional[int]:
        worst = 0
        for c in claims:
            v = self._dr_volume_name(ns, c, dst)
            last = (((self.k[dst].get("lhvolume", NS, v) or {}).get("status") or {}).get("lastBackup")) if v else None
            t = self._backup_time(dst, last) if last else None
            if not t:
                return None
            worst = max(worst, int((now_utc() - t).total_seconds()))
        return worst

    def promote(self, src: str, dst: str, ns: str, vm: str, claims: List[str], src_up: bool, unplanned: bool):
        kd = self.k[dst]
        if src_up:   # stop scheduled backups from the old primary (an active DR volume no longer restores anyway)
            for c in claims:
                try:
                    self._unschedule_backups(src, self._volume_name(src, ns, c))
                except (ReplicationError, KubeError, TypeError):
                    pass
        for c in claims:
            v = self._dr_volume_name(ns, c, dst)
            vol = kd.get("lhvolume", NS, v)
            if (vol.get("status") or {}).get("isStandby"):
                try:
                    self._request_sync(dst, v)
                except KubeError:
                    pass
                kd.merge_patch("lhvolume", NS, v, {"spec": {"Standby": False, "frontend": "blockdev"}})
            kd.wait_for(lambda: not ((kd.get("lhvolume", NS, v) or {}).get("status") or {}).get("isStandby"),
                        self.timeouts["transfer"], interval=3, what=f"activation of {v}")
            ann = vol["metadata"].get("annotations") or {}
            pv_t, pvc_t = json.loads(ann[A_PV]), json.loads(ann[A_PVC])
            pvname = pv_t["name"]
            if kd.get("pv", None, pvname) is None:
                spec = dict(pv_t["spec"])
                spec["claimRef"] = {"namespace": ns, "name": c}
                spec["csi"] = dict(spec["csi"], volumeHandle=v)
                kd.create("pv", None, {"apiVersion": "v1", "kind": "PersistentVolume",
                                       "metadata": {"name": pvname, "labels": {L_MANAGED: "true", L_VM: vm}},
                                       "spec": spec})
            if kd.get("pvc", ns, c) is None:
                meta = {"name": c, "namespace": ns, "labels": dict(pvc_t.get("labels") or {}, **{L_MANAGED: "true",
                                                                                                L_VM: vm}),
                        "annotations": pvc_t.get("annotations") or {}}
                kd.create("pvc", ns, {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": meta,
                                      "spec": dict(pvc_t["spec"], volumeName=pvname)})
            kd.wait_for(lambda: ((kd.get("pvc", ns, c) or {}).get("status") or {}).get("phase") == "Bound",
                        self.timeouts["tmr"], what=f"PVC {ns}/{c} bound")

    # ── reprotect / unprotect ────────────────────────────────────────────────
    def reprotect(self, new_src: str, new_dst: str, ns: str, vm: str, claims: List[str],
                  wait_init: bool) -> List[str]:
        ks, kd = self.k[new_src], self.k[new_dst]
        self.check_claims(new_src, ns, claims)
        self._check_target(new_src)
        self._check_target(new_dst)
        out, created = [], {}
        for c in claims:
            v = self._volume_name(new_src, ns, c)
            self._unschedule_backups(new_src, v)       # a rerun inherits them from the failed run
            # Point the volume at the target for the one-off backup only. Scheduled backups start after the DR
            # side has confirmed its restore, so they can't race the (full) restore that follows.
            ks.merge_patch("lhvolume", NS, v, {"spec": {"backupTargetName": self.target}})
            self.log(f"  {c}: backup of the new primary ...")
            b = self.backup_now(new_src, v, "reprotect")
            bname = b["metadata"]["name"]
            if self._is_dr_volume_of(new_dst, v, new_src):
                # an earlier reprotect already replaced the old copy: keep restoring into that DR volume
                self.log(f"  {c}: {new_dst} already has DR volume {v} of {new_src}; reusing it")
                out.append(f"{new_dst}: DR volume {v} reused (restores {bname} incrementally)")
            else:
                # the old primary's copy is replaced by a DR volume of the new primary (full restore)
                old = self._volume_name(new_dst, ns, c) if kd.get("pvc", ns, c) else (
                    v if kd.get("lhvolume", NS, v) else None)
                if old:
                    self._release_vms_using(new_dst, ns, c, out)
                    self._unschedule_backups(new_dst, old)
                    out += self._delete_volume(new_dst, ns, c, old)
                self.wait_backup_visible(new_dst, v, bname, (b.get("spec") or {}).get("backupBlockSize"))
                self._create_dr_volume(new_src, new_dst, ns, vm, c, v, b)
                out.append(f"{new_dst}: DR volume {v} created from {bname} (full restore)")
            created[v] = bname
        for v, bname in created.items():
            self.log(f"  waiting for {v} to restore {bname} (or newer) on {new_dst} ...")
            self.wait_restored(new_dst, v, bname)
        for v in created:
            self._schedule_backups(new_src, v)
            out.append(f"{new_src}: volume {v} → backups to '{self.target}' every {self.interval} min")
        return out

    def unprotect(self, primary: str, replica: str, ns: str, vm: str, claims: List[str]) -> List[str]:
        out = []
        for c in claims:
            try:
                v = self._volume_name(primary, ns, c)
            except (ReplicationError, KubeError, TypeError):
                continue
            self._unschedule_backups(primary, v)
            out.append(f"{primary}: volume {v}: scheduled backups to '{self.target}' stopped (backups kept)")
        return out

    def delete_replica_volumes(self, replica: str, ns: str, claims: List[str]) -> List[str]:
        out = []
        for c in claims:
            v = self._dr_volume_name(ns, c, replica)
            if v:
                out += self._delete_volume(replica, ns, c, v)
        return out
