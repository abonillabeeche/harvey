import copy
import datetime

import pytest

from harvey.backends import REGISTRY, backend_for_driver
from harvey.backends.portworx import (A_MIGRATED, PortworxBackend, last_successful, migration_name_time,
                                      migration_spec, schedule_items, stork_time)
from harvey.common import A_BACKEND, L_VM, ReplicationError
from harvey.replication import Replicator, build_replica_vm


class FakeKube:
    """Minimal in-memory stand-in for KubeClient (get/list/create/merge_patch/delete)."""

    def __init__(self, name, objs=None):
        self.name = name
        self.objs = {}   # (kind, ns, name) -> obj
        for kind, ns, obj in objs or []:
            self.objs[(kind, ns, obj["metadata"]["name"])] = copy.deepcopy(obj)

    def get(self, kind, ns, name):
        o = self.objs.get((kind, ns, name))
        return copy.deepcopy(o) if o else None

    def list(self, kind, ns=None, label_selector=None):
        out = [copy.deepcopy(o) for (k, n, _), o in self.objs.items() if k == kind and (ns is None or n == ns)]
        if label_selector:
            for term in label_selector.split(","):
                key, _, val = term.partition("=")
                out = [o for o in out if (o["metadata"].get("labels") or {}).get(key) == val]
        return out

    def create(self, kind, ns, body):
        self.objs[(kind, ns, body["metadata"]["name"])] = copy.deepcopy(body)
        return body

    def merge_patch(self, kind, ns, name, patch):
        def merge(a, b):
            for k, v in b.items():
                if v is None:
                    a.pop(k, None)
                elif isinstance(v, dict) and isinstance(a.get(k), dict):
                    merge(a[k], v)
                else:
                    a[k] = copy.deepcopy(v)
        merge(self.objs[(kind, ns, name)], patch)
        return copy.deepcopy(self.objs[(kind, ns, name)])

    def delete(self, kind, ns, name):
        self.objs.pop((kind, ns, name), None)

    def ensure_namespace(self, name):
        pass

    def wait_for(self, fn, timeout_s, interval=3.0, what=""):
        v = fn()
        if not v:
            raise AssertionError(f"wait_for({what}) would block")
        return v


def _pvc(name, ns, pv, ann=None):
    return {"metadata": {"name": name, "namespace": ns, "annotations": ann or {}},
            "spec": {"volumeName": pv}, "status": {"phase": "Bound"}}


def _pv(name, driver):
    return {"metadata": {"name": name}, "spec": {"csi": {"driver": driver}}}


def _vm(ns="demo", name="vm1", claims=("vm1-root",)):
    return {"apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine",
            "metadata": {"name": name, "namespace": ns, "labels": {}, "annotations": {}},
            "spec": {"runStrategy": "RerunOnFailure", "template": {"spec": {
                "volumes": [{"name": c, "persistentVolumeClaim": {"claimName": c}} for c in claims]}}},
            "status": {"printableStatus": "Running"}}


def test_registry_and_driver_mapping():
    assert set(REGISTRY) >= {"netapp", "portworx"}
    assert backend_for_driver("csi.trident.netapp.io") == "netapp"
    assert backend_for_driver("pxd.portworx.com") == "portworx"
    assert backend_for_driver("driver.longhorn.io") == "longhorn"
    assert backend_for_driver("rbd.csi.ceph.com") is None


def test_detect_backend_from_pv_driver_and_annotation():
    k = FakeKube("a", [("pvc", "demo", _pvc("vm1-root", "demo", "pv1")), ("pv", None, _pv("pv1", "pxd.portworx.com"))])
    rep = Replicator({"a": k}, {}, log=lambda *_: None)
    assert rep.detect_backend("a", "demo", _vm()).name == "portworx"
    vm = _vm()
    vm["metadata"]["annotations"][A_BACKEND] = "netapp"      # recorded choice wins (source may be down)
    assert rep.detect_backend("a", "demo", vm).name == "netapp"
    assert Replicator({"a": k}, {"backend": "netapp"}, log=print).detect_backend("a", "demo", _vm()).name == "netapp"


def test_detect_backend_rejects_unsupported_and_mixed():
    k = FakeKube("a", [("pvc", "demo", _pvc("r", "demo", "pv1")), ("pv", None, _pv("pv1", "rbd.csi.ceph.com")),
                       ("pvc", "demo", _pvc("d1", "demo", "pv2")), ("pv", None, _pv("pv2", "pxd.portworx.com")),
                       ("pvc", "demo", _pvc("d2", "demo", "pv3")), ("pv", None, _pv("pv3", "csi.trident.netapp.io"))])
    rep = Replicator({"a": k}, {}, log=lambda *_: None)
    with pytest.raises(ReplicationError, match="no Harvey storage backend"):
        rep.detect_backend("a", "demo", _vm(claims=("r",)))
    with pytest.raises(ReplicationError, match="mixes storage backends"):
        rep.detect_backend("a", "demo", _vm(claims=("d1", "d2")))


def test_netapp_backend_keeps_legacy_top_level_settings():
    rep = Replicator({}, {"policy": "MirrorLatest", "schedule": "10min"}, log=print)
    be = rep.backend("netapp")
    assert (be.policy, be.schedule) == ("MirrorLatest", "10min")


def test_replica_vm_records_backend_and_drops_stork_annotations():
    vm = _vm()
    vm["metadata"]["annotations"]["stork.libopenstorage.org/migrated"] = "true"
    r = build_replica_vm(vm, "a", "a", backend="portworx", extra_drop=("stork.libopenstorage.org/",))
    assert r["metadata"]["annotations"][A_BACKEND] == "portworx"
    assert "stork.libopenstorage.org/migrated" not in r["metadata"]["annotations"]


# ── Portworx ──────────────────────────────────────────────────────────────────
def test_px_migration_spec_is_volume_only():
    s = migration_spec("demo", "vm1", "harvey-to-dr")
    assert s["includeResources"] is False and s["includeVolumes"] is True and s["startApplications"] is False
    assert s["namespaces"] == ["demo"] and s["selectors"] == {"harvey.io/protection-unit": "vm1"}
    assert s["ignoreOwnerReferencesCheck"] is True and s["purgeDeletedResources"] is False


def test_px_names_and_pairs():
    be = PortworxBackend({}, {"interval_minutes": 15, "pairs": {"a->b": "harvey-ab", "b->a": "kube-system/harvey-ba"}},
                         {}, print)
    assert be.pair_ref("a", "b", "demo") == ("demo", "harvey-ab")
    assert be.pair_ref("b", "a", "demo") == ("kube-system", "harvey-ba")
    assert be.pair_ref("a", "c", "demo") == ("demo", "harvey-c")
    assert be.schedule_name("demo", "vm1", "demo") == "harvey-vm1"
    assert be.schedule_name("demo", "vm1", "kube-system") == "harvey-demo-vm1"
    assert len(be.schedule_name("demo", "v" * 80, "demo")) <= 63
    assert be.policy_name == "harvey-15min"


def test_px_schedule_status_helpers():
    sched = {"status": {"items": {"Interval": [
        {"name": "m1", "creationTimestamp": "2026-11-06T10:00:00Z", "status": "Successful"},
        {"name": "m3", "creationTimestamp": "2026-11-06T10:30:00Z", "status": "InProgress"},
        {"name": "m2", "creationTimestamp": "2026-11-06T10:15:00Z", "status": "Failed"}]}}}
    assert [i["name"] for i in schedule_items(sched)] == ["m1", "m2", "m3"]
    assert last_successful(sched)["name"] == "m1"
    assert last_successful({}) is None
    assert stork_time("2026-11-06-231500") == datetime.datetime(2026, 11, 6, 23, 15, tzinfo=datetime.timezone.utc)
    assert stork_time("garbage") is None


def test_px_promote_fences_both_sides():
    sched = {"apiVersion": "x", "kind": "MigrationSchedule",
             "metadata": {"name": "harvey-vm1", "namespace": "demo"},
             "spec": {"suspend": False, "template": {"spec": migration_spec("demo", "vm1", "harvey-b")}},
             "status": {"applicationActivated": True, "items": {}}}
    copy_ = copy.deepcopy(sched)
    copy_["metadata"]["annotations"] = {"stork.libopenstorage.org/static-copy": "true"}
    copy_["spec"]["suspend"] = True
    copy_["status"] = {}
    a = FakeKube("a", [("migrationschedule", "demo", sched)])
    b = FakeKube("b", [("migrationschedule", "demo", copy_),
                       ("pvc", "demo", _pvc("vm1-root", "demo", "pv9", {A_MIGRATED: "true"}))])
    be = PortworxBackend({"a": a, "b": b}, {}, {"tmr": 1, "transfer": 1}, lambda *_: None)
    be.preflight("a", "b", "demo", "vm1", ["vm1-root"], src_up=True)
    be.promote("a", "b", "demo", "vm1", ["vm1-root"], src_up=True, unplanned=False)
    src = a.get("migrationschedule", "demo", "harvey-vm1")
    dst = b.get("migrationschedule", "demo", "harvey-vm1")
    assert src["spec"]["suspend"] is True and src["status"]["applicationActivated"] is False
    assert dst["spec"]["suspend"] is True and dst["status"]["applicationActivated"] is True
    assert "harvey.io/promoted-at" in b.get("pvc", "demo", "vm1-root")["metadata"]["annotations"]


def test_px_preflight_rejects_non_migrated_pvc():
    b = FakeKube("b", [("pvc", "demo", _pvc("vm1-root", "demo", "pv9"))])
    be = PortworxBackend({"b": b}, {}, {}, print)
    with pytest.raises(ReplicationError, match="not a Stork-migrated replica"):
        be.preflight("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)


def test_px_unplanned_rpo_from_destination_annotation():
    t = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=7)).strftime("%Y-%m-%d-%H%M%S")
    b = FakeKube("b", [("pvc", "demo", _pvc("vm1-root", "demo", "pv9",
                                            {A_MIGRATED: "true", "stork.libopenstorage.org/migrationTime": t}))])
    be = PortworxBackend({"b": b}, {}, {}, print)
    age = be.recovery_point_age("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)
    assert 7 * 60 - 5 <= age <= 7 * 60 + 5


def test_px_schedule_items_tolerates_null_status():
    assert schedule_items({"status": {"items": None}}) == []
    assert schedule_items({"status": None}) == []
    assert schedule_items(None) == []


def test_px_unplanned_rpo_prefers_migration_name_over_apply_time():
    # observed 2026-11-07: migration triggered 06:02:50, PVC applied 06:04:51 -> loss must count from 06:02:50
    now = datetime.datetime.now(datetime.timezone.utc)
    trig = (now - datetime.timedelta(seconds=180)).strftime("%Y-%m-%d-%H%M%S")
    applied = (now - datetime.timedelta(seconds=60)).strftime("%Y-%m-%d-%H%M%S")
    b = FakeKube("b", [("pvc", "demo", _pvc("vm1-root", "demo", "pv9", {
        A_MIGRATED: "true", "stork.libopenstorage.org/migrationTime": applied,
        "stork.libopenstorage.org/migrationName": f"harvey-vm1-interval-{trig}"}))])
    age = PortworxBackend({"b": b}, {}, {}, print).recovery_point_age("a", "b", "demo", "vm1", ["vm1-root"], False)
    assert 175 <= age <= 185
    assert migration_name_time("harvey-vm1-final-20261107060250") == datetime.datetime(
        2026, 11, 7, 6, 2, 50, tzinfo=datetime.timezone.utc)
    assert migration_name_time("no-time-here") is None


def test_direction_resolves_stale_primary_after_unplanned_failover():
    old = _vm(); old["metadata"]["labels"]["harvey.io/role"] = "primary"
    old["metadata"]["annotations"]["harvey.io/primary-since"] = "2026-11-07T05:00:00Z"
    new = _vm(); new["metadata"]["labels"]["harvey.io/role"] = "primary"
    new["metadata"]["annotations"]["harvey.io/primary-since"] = "2026-11-07T06:05:34Z"
    a = FakeKube("a", [("vm", "demo", old)])
    b = FakeKube("b", [("vm", "demo", new)])
    rep = Replicator({"a": a, "b": b}, {}, log=print)
    assert rep.direction("demo", "vm1", "a", "b") == ("b", "a")
    assert rep.stale_primary[("demo", "vm1")] == "a"
    del new["metadata"]["annotations"]["harvey.io/primary-since"]
    del old["metadata"]["annotations"]["harvey.io/primary-since"]
    rep2 = Replicator({"a": FakeKube("a", [("vm", "demo", old)]), "b": FakeKube("b", [("vm", "demo", new)])}, {}, print)
    assert rep2.direction("demo", "vm1", "a", "b") == (None, None)



# ── Longhorn ──────────────────────────────────────────────────────────────────
import json as _json

from harvey.backends.longhorn import A_PV, A_PVC, L_GROUP, LonghornBackend


def _lh_dr_cluster(standby=True, last="backup-aaa", snap_time="2026-11-07T10:00:00Z"):
    pv_t = {"name": "pvc-123", "spec": {"accessModes": ["ReadWriteMany"], "capacity": {"storage": "1Gi"},
                                         "csi": {"driver": "driver.longhorn.io", "volumeHandle": "pvc-123",
                                                 "volumeAttributes": {"numberOfReplicas": "2"}},
                                         "volumeMode": "Block", "storageClassName": "tworeplicas",
                                         "persistentVolumeReclaimPolicy": "Delete"}}
    pvc_t = {"labels": {}, "annotations": {}, "spec": {"accessModes": ["ReadWriteMany"], "volumeMode": "Block",
                                                      "resources": {"requests": {"storage": "1Gi"}},
                                                      "storageClassName": "tworeplicas"}}
    vol = {"metadata": {"name": "pvc-123", "namespace": "longhorn-system",
                        "labels": {"harvey.io/managed": "true", "harvey.io/vm-claim": "demo.vm1-root"},
                        "annotations": {A_PV: _json.dumps(pv_t), A_PVC: _json.dumps(pvc_t)}},
           "spec": {"Standby": standby, "frontend": ""},
           "status": {"isStandby": standby, "lastBackup": last}}
    bk = {"metadata": {"name": last, "namespace": "longhorn-system"}, "status": {"snapshotCreatedAt": snap_time}}
    eng = {"metadata": {"name": "pvc-123-e-0", "namespace": "longhorn-system", "labels": {"longhornvolume": "pvc-123"}},
           "status": {"lastRestoredBackup": last}}
    return FakeKube("b", [("lhvolume", "longhorn-system", vol), ("lhbackup", "longhorn-system", bk),
                          ("lhengine", "longhorn-system", eng)])


class _ActivatingKube(FakeKube):
    """Longhorn flips status.isStandby after spec.Standby=false; a bound PVC appears once created."""
    def merge_patch(self, kind, ns, name, patch):
        r = super().merge_patch(kind, ns, name, patch)
        if kind == "lhvolume" and patch.get("spec", {}).get("Standby") is False:
            self.objs[(kind, ns, name)]["status"]["isStandby"] = False
        return r

    def create(self, kind, ns, body):
        body = copy.deepcopy(body)
        if kind == "pvc":
            body["status"] = {"phase": "Bound"}
        return super().create(kind, ns, body)


def test_lh_preflight_requires_standby_dr_volume_with_a_restored_backup():
    be = LonghornBackend({"b": _lh_dr_cluster()}, {}, {"tmr": 1, "transfer": 1}, print)
    be.preflight("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)
    be2 = LonghornBackend({"b": _lh_dr_cluster(last="")}, {}, {}, print)
    with pytest.raises(ReplicationError, match="has not restored any backup"):
        be2.preflight("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)
    be3 = LonghornBackend({"b": _lh_dr_cluster(standby=False)}, {}, {}, print)
    with pytest.raises(ReplicationError, match="not a standby DR volume"):
        be3.preflight("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)


def test_lh_promote_activates_and_creates_pv_pvc_from_templates():
    b = _ActivatingKube("b", list((k, n, o) for (k, n, _), o in _lh_dr_cluster().objs.items()))
    be = LonghornBackend({"b": b}, {}, {"tmr": 1, "transfer": 1}, lambda *_: None)
    be.promote("a", "b", "demo", "vm1", ["vm1-root"], src_up=False, unplanned=True)
    vol = b.get("lhvolume", "longhorn-system", "pvc-123")
    assert vol["spec"]["Standby"] is False and vol["spec"]["frontend"] == "blockdev"
    pv = b.get("pv", None, "pvc-123")
    assert pv["spec"]["claimRef"] == {"namespace": "demo", "name": "vm1-root"}
    assert pv["spec"]["csi"]["volumeHandle"] == "pvc-123" and pv["spec"]["volumeMode"] == "Block"
    pvc = b.get("pvc", "demo", "vm1-root")
    assert pvc["spec"]["volumeName"] == "pvc-123" and pvc["spec"]["storageClassName"] == "tworeplicas"


def test_lh_unplanned_rpo_from_last_restored_backup_snapshot_time():
    t = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=300)).strftime("%Y-%m-%dT%H:%M:%SZ")
    be = LonghornBackend({"b": _lh_dr_cluster(snap_time=t)}, {}, {}, print)
    age = be.recovery_point_age("a", "b", "demo", "vm1", ["vm1-root"], src_up=False)
    assert 295 <= age <= 305


def test_lh_status_reports_up_to_date_standby():
    a = FakeKube("a", [("pvc", "demo", _pvc("vm1-root", "demo", "pvc-123")),
                       ("pv", None, {"metadata": {"name": "pvc-123"},
                                     "spec": {"csi": {"driver": "driver.longhorn.io", "volumeHandle": "pvc-123"}}}),
                       ("lhvolume", "longhorn-system", {"metadata": {"name": "pvc-123", "labels": {L_GROUP: "enabled"}},
                                                        "spec": {"dataEngine": "v2"}}),
                       ("lhbackupvolume", "longhorn-system", {"metadata": {"name": "bv-1"},
                                                              "spec": {"backupTargetName": "harvey", "volumeName": "pvc-123"},
                                                              "status": {"lastBackupName": "backup-aaa"}})])
    be = LonghornBackend({"a": a, "b": _lh_dr_cluster()}, {}, {}, print)
    (row,) = be.volume_status("a", "b", "demo", "vm1", ["vm1-root"])
    assert row.src_state == "backup/5m" and row.dst_state == "standby" and row.repl_state == "up to date"
    assert row.healthy is True and "v2" in row.note and "alpha" in row.note



def test_lh_status_does_not_trust_volume_lastbackup_without_the_engine():
    # seen with the V2 engine: volume.status.lastBackup moved on, the engine never restored it
    a = FakeKube("a", [("pvc", "demo", _pvc("vm1-root", "demo", "pvc-123")),
                       ("pv", None, {"metadata": {"name": "pvc-123"},
                                     "spec": {"csi": {"driver": "driver.longhorn.io", "volumeHandle": "pvc-123"}}}),
                       ("lhvolume", "longhorn-system", {"metadata": {"name": "pvc-123", "labels": {L_GROUP: "enabled"}},
                                                        "spec": {"dataEngine": "v2"}})])
    b = _lh_dr_cluster()
    b.objs[("lhengine", "longhorn-system", "pvc-123-e-0")]["status"]["lastRestoredBackup"] = ""
    (row,) = LonghornBackend({"a": a, "b": b}, {}, {}, print).volume_status("a", "b", "demo", "vm1", ["vm1-root"])
    assert row.repl_state == "restore not confirmed" and row.healthy is False
