import copy

import pytest

from harvey.backends import REGISTRY
from harvey.backends.base import StorageBackend
from harvey.common import L_GROUP, ReplicationError
from harvey.replication import Replicator
from harvey.units import resolve_unit
from test_backends import FakeKube


def _vm(name, claims, group=None, ns="prod", status="Running", role=None):
    labels = {}
    if group:
        labels[L_GROUP] = group
    if role:
        labels["harvey.io/role"] = role
    return ("vm", ns, {"metadata": {"name": name, "namespace": ns, "labels": labels, "annotations": {}},
                       "spec": {"runStrategy": "RerunOnFailure", "template": {"spec": {
                           "volumes": [{"name": c, "persistentVolumeClaim": {"claimName": c}} for c in claims]}}},
                       "status": {"printableStatus": status}})


def _pvc(name, group=None, ns="prod"):
    return ("pvc", ns, {"metadata": {"name": name, "namespace": ns, "labels": {L_GROUP: group} if group else {}},
                        "spec": {}})


# ── unit resolution and the shared-disk guard ────────────────────────────────
def test_ungrouped_vm_is_a_unit_of_one():
    k = FakeKube("a", [_vm("web", ["web-root"])])
    u = resolve_unit(k, "prod", "web")
    assert (u.name, u.group, u.vms, u.claims) == ("web", None, ["web"], ["web-root"])


def test_group_unions_disks_once_and_adds_standalone_pvcs():
    k = FakeKube("a", [_vm("node1", ["n1-root", "quorum", "shared-data"], "wsfc"),
                       _vm("node2", ["n2-root", "quorum", "shared-data"], "wsfc"),
                       _pvc("archive", "wsfc"), _vm("other", ["other-root"])])
    u = resolve_unit(k, "prod", "node2")
    assert u.name == "wsfc" and u.vms == ["node1", "node2"]
    assert u.claims == ["n1-root", "quorum", "shared-data", "n2-root", "archive"]
    assert u.standalone == ["archive"]
    assert resolve_unit(k, "prod", group="wsfc").claims == u.claims


def test_guard_refuses_disk_shared_outside_the_group():
    k = FakeKube("a", [_vm("node1", ["n1-root", "quorum"]), _vm("node2", ["n2-root", "quorum"])])
    with pytest.raises(ReplicationError, match=r"(?s)shares disks.*node2 \(shares quorum\).*harvey group add <name> -n prod node1 node2"):
        resolve_unit(k, "prod", "node1")
    # one in the group, one not: still refused
    k2 = FakeKube("a", [_vm("node1", ["n1-root", "quorum"], "wsfc"), _vm("node2", ["n2-root", "quorum"])])
    with pytest.raises(ReplicationError, match="shares disks"):
        resolve_unit(k2, "prod", "node1")


def test_replica_side_recovers_standalone_claims_from_the_recorded_list():
    # on the DR side a standalone PVC may not exist yet (Longhorn); the unit's claim list travels on the VMs
    vm = _vm("node1", ["n1-root", "quorum"], "wsfc")
    vm[2]["metadata"]["annotations"]["harvey.io/unit-claims"] = '["n1-root", "quorum", "archive"]'
    u = resolve_unit(FakeKube("b", [vm]), "prod", "node1")
    assert u.claims == ["n1-root", "quorum", "archive"] and u.standalone == ["archive"]


# ── failover acts once per unit, after every member stopped ──────────────────
class _Recorder(StorageBackend):
    name = "recorder"
    drivers = ("test.example.com",)
    calls = []

    def preflight(self, src, dst, ns, vm, claims, src_up):
        self.calls.append(("preflight", vm, tuple(claims)))

    def final_sync(self, src, dst, ns, vm, claims, not_before):
        self.calls.append(("final_sync", vm, tuple(claims)))
        return [(c, 0, 0.0) for c in claims]

    def promote(self, src, dst, ns, vm, claims, src_up, unplanned):
        self.calls.append(("promote", vm, tuple(claims)))


class _ClusterKube(FakeKube):
    """Enough of a cluster for failover: stopping/starting flips printableStatus."""
    def reachable(self):
        return True

    def merge_patch(self, kind, ns, name, patch):
        r = super().merge_patch(kind, ns, name, patch)
        rs = (patch.get("spec") or {}).get("runStrategy")
        if kind == "vm" and rs:
            self.objs[(kind, ns, name)]["status"]["printableStatus"] = "Stopped" if rs == "Halted" else "Running"
        return r


def test_failover_of_one_member_takes_the_whole_group_and_syncs_shared_disks_once():
    REGISTRY["recorder"] = _Recorder
    _Recorder.calls = []
    src = _ClusterKube("a", [_vm("node1", ["n1-root", "quorum"], "wsfc", role="primary"),
                             _vm("node2", ["n2-root", "quorum"], "wsfc", role="primary")])
    dst = _ClusterKube("b", [_vm("node1", ["n1-root", "quorum"], "wsfc", status="Stopped", role="replica"),
                             _vm("node2", ["n2-root", "quorum"], "wsfc", status="Stopped", role="replica")])
    rep = Replicator({"a": src, "b": dst}, {"backend": "recorder"}, log=lambda *_: None)
    rep.sync_metadata = lambda *a, **k: []          # metadata copy is covered elsewhere
    res = rep.failover([("prod", "node1", 0, 0)], "a", "b")
    assert sorted(res.started) == ["prod/node1", "prod/node2"]      # node2 was pulled in
    kinds = [c[0] for c in _Recorder.calls]
    assert kinds == ["preflight", "final_sync", "promote"]          # once for the unit, not per VM
    assert _Recorder.calls[1] == ("final_sync", "wsfc", ("n1-root", "quorum", "n2-root"))
    assert all(src.get("vm", "prod", n)["status"]["printableStatus"] == "Stopped" for n in ("node1", "node2"))
    assert res.rpo == {"prod/node1": 0, "prod/node2": 0}
    del REGISTRY["recorder"]


def test_failover_refuses_shared_disk_outside_a_group():
    REGISTRY["recorder"] = _Recorder
    dst = _ClusterKube("b", [_vm("node1", ["n1-root", "quorum"], status="Stopped", role="replica"),
                             _vm("node2", ["n2-root", "quorum"], status="Stopped", role="replica")])
    rep = Replicator({"a": _ClusterKube("a"), "b": dst}, {"backend": "recorder"}, log=lambda *_: None)
    with pytest.raises(ReplicationError, match="shares disks"):
        rep.failover([("prod", "node1", 0, 0)], "a", "b")
    del REGISTRY["recorder"]


def test_expand_plan_keeps_member_priorities():
    k = FakeKube("b", [_vm("db", ["db-root", "shared"], "app"), _vm("web", ["web-root", "shared"], "app")])
    k.objs[("vm", "prod", "db")]["metadata"]["labels"]["failover-priority"] = "100"
    plan, notes = Replicator({"b": k}, {}, log=print).expand_plan([("prod", "web", 10, 0)], "b")
    assert ("prod", "db", 100, 0) in plan and len(plan) == 2 and "recovery group app" in notes[0]


def test_reprotect_resumes_when_the_old_primary_vm_object_is_gone():
    """An interrupted reprotect deletes the old primary's VM before replacing its disks. A rerun must not stop on
    the missing VM: the backend reprotects the disks and the metadata sync recreates the replica VM."""
    class _ReprotectRecorder(_Recorder):
        def reprotect(self, new_src, new_dst, ns, vm, claims, wait_init):
            self.calls.append(("reprotect", vm, tuple(claims)))
            return []
    REGISTRY["recorder"] = _ReprotectRecorder
    _Recorder.calls = []
    new_src = _ClusterKube("b", [_vm("app", ["app-root"], role="primary")])
    old_primary = _ClusterKube("a")                              # its VM object was already removed
    rep = Replicator({"a": old_primary, "b": new_src}, {"backend": "recorder"}, log=lambda *_: None)
    synced = []
    def fake_sync(ns, m, s, d, **k):                             # the real sync applies the replica VM
        synced.append((m, s, d))
        old_primary.create("vm", ns, {"metadata": {"name": m, "namespace": ns, "annotations": {}}})
        return [f"vm/{m} created (Halted)"]
    rep.sync_metadata = fake_sync
    out = rep.reprotect("prod", "app", "b", "a")
    assert _Recorder.calls == [("reprotect", "app", ("app-root",))]
    assert synced == [("app", "b", "a")]
    assert any("no VM object" in line for line in out)
    del REGISTRY["recorder"]
