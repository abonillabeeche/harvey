"""NetApp failover/reprotect robustness, from lab findings (Trident 25.10, ONTAP 9.19.1 simulators)."""
import pytest

from harvey.backends.netapp import NetAppBackend
from harvey.common import ReplicationError
from test_backends import FakeKube

H = "svm_dr:trident_pvc_1"
TIMEOUTS = {"tmr": 1, "transfer": 1, "stall": 1}


def _tmr(name, state, handle=H):
    return {"metadata": {"name": name, "namespace": "demo"}, "spec": {"state": state, "volumeMappings": [{}]},
            "status": {"conditions": [{"state": state, "localVolumeHandle": handle}]}}


def test_preflight_accepts_a_half_promoted_failover_so_it_can_be_resumed():
    # seen in the lab: an unplanned failover failed after promoting one of two volumes; the re-run was refused
    dst = FakeKube("b", [("tmr", "demo", _tmr("vm1-root", "promoted")), ("tmr", "demo", _tmr("vm1-data", "established"))])
    be = NetAppBackend({"b": dst}, {}, TIMEOUTS, log=print)
    be.preflight("a", "b", "demo", "vm1", ["vm1-root", "vm1-data"], src_up=False)
    dst.objs[("tmr", "demo", "vm1-data")] = _tmr("vm1-data", "reestablishing")
    with pytest.raises(ReplicationError, match="expected established"):
        be.preflight("a", "b", "demo", "vm1", ["vm1-root", "vm1-data"], src_up=False)


def test_set_state_waits_for_the_ontap_job_and_raises_its_error(monkeypatch):
    from harvey.ontap import OntapClient, OntapError
    o = OntapClient("h", "u", "p")
    calls = []

    def req(method, path, **kw):
        calls.append((method, path, kw.get("json")))
        if method == "PATCH":
            return {"job": {"uuid": "j1"}}
        return {"state": "failure", "message": "Relationship is not quiesced"}
    monkeypatch.setattr(o, "_req", req)
    with pytest.raises(OntapError, match="not quiesced"):
        o.set_state("r1", "broken_off")
    assert calls[0] == ("PATCH", "snapmirror/relationships/r1", {"state": "broken_off"}) and calls[1][1] == "cluster/jobs/j1"
    calls.clear()
    o.set_state("r1", "snapmirrored", wait=False)
    assert [c[0] for c in calls] == ["PATCH"]


class _TridentKube(FakeKube):
    """TMR status follows the spec like Trident does (state only)."""

    def merge_patch(self, kind, ns, name, patch):
        r = super().merge_patch(kind, ns, name, patch)
        st = (patch.get("spec") or {}).get("state")
        if kind == "tmr" and st:
            cond = self.objs[(kind, ns, name)]["status"]["conditions"][0]
            cond["state"] = st
            for vm in (patch["spec"].get("volumeMappings") or []):
                cond["remoteVolumeHandle"] = vm.get("remoteVolumeHandle")
        return r


def _reprotect_setup(dst_tmr_state, dst_status):
    src = FakeKube("b", [("tmr", "demo", _tmr("vm1-root", "promoted", "svm_b:v1"))])
    t = _tmr("vm1-root", dst_status, "svm_a:v1")
    t["spec"] = {"state": dst_tmr_state, "replicationPolicy": "MirrorAllSnapshots", "volumeMappings": [{"localPVCName": "vm1-root"}]}
    dst = _TridentKube("a", [("tmr", "demo", t)])
    return NetAppBackend({"b": src, "a": dst}, {}, TIMEOUTS, log=print), dst


@pytest.mark.parametrize("status, expected", [("failed", ["promoted", "reestablished"]),
                                               ("reestablishing", ["reestablished"])])
def test_reprotect_retries_a_failed_reestablish(status, expected):
    # seen in the lab: a reprotect that failed while the peering was down could not be retried (same spec: Trident did nothing)
    be, dst = _reprotect_setup("reestablished", status)
    states = []
    real = dst.merge_patch

    def merge_patch(kind, ns, name, patch):
        states.append((patch.get("spec") or {}).get("state"))
        return real(kind, ns, name, patch)
    dst.merge_patch = merge_patch
    out = be.reprotect("b", "a", "demo", "vm1", ["vm1-root"], wait_init=False)
    assert states == expected and any("retrying the reestablish" in o for o in out) == (status == "failed")
