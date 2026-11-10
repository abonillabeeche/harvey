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
    with pytest.raises(ReplicationError, match="instead of established"):
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


def _gen_tmr(name, state, gen=2, observed=2, message="", handle=H):
    t = _tmr(name, state, handle)
    t["metadata"]["generation"] = gen
    t["status"]["conditions"][0].update({"observedGeneration": observed, "message": message,
                                         "remoteVolumeHandle": "svm0:src_" + name})
    return t


class _CountingKube(FakeKube):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.calls = []

    def get(self, kind, ns, name):
        self.calls.append(("get", name))
        return super().get(kind, ns, name)

    def list(self, kind, ns=None, label_selector=None):
        self.calls.append(("list", kind))
        return super().list(kind, ns, label_selector)


def test_wait_tmrs_fails_fast_and_reports_every_failed_disk(monkeypatch):
    # seen in the lab: with the intercluster LIF down both TMRs went 'failed' 34 s in, but reprotect waited 300 s and
    # then printed the raw condition dict of the first disk only
    import harvey.backends.netapp as na
    monkeypatch.setattr(na.time, "sleep", lambda s: (_ for _ in ()).throw(AssertionError("should not poll again")))
    k = _CountingKube("a", [("tmr", "demo", _gen_tmr("vm1-root", "failed", message="Could not reestablish mirror")),
                            ("tmr", "demo", _gen_tmr("vm1-data", "failed", message="Could not reestablish mirror"))])
    be = NetAppBackend({"a": k}, {}, {"tmr": 300, "transfer": 1}, log=print)
    with pytest.raises(ReplicationError) as e:
        be._wait_tmrs("a", "demo", ["vm1-root", "vm1-data"], ("reestablished", "established"))
    msg = str(e.value)
    assert "a: tmr/vm1-root failed: Could not reestablish mirror (remote svm0:src_vm1-root)" in msg
    assert "a: tmr/vm1-data failed: Could not reestablish mirror" in msg and "{" not in msg
    assert k.calls == [("list", "tmr")]          # one LIST for all disks, not one GET per TMR


def test_wait_tmrs_ignores_a_failed_status_left_from_the_previous_spec():
    # Trident hasn't reconciled the new spec yet (observedGeneration behind): the old 'failed' must not count
    k = FakeKube("a", [("tmr", "demo", _gen_tmr("vm1-root", "failed", gen=3, observed=2, message="old failure"))])
    be = NetAppBackend({"a": k}, {}, {"tmr": 0, "transfer": 1}, log=print)
    with pytest.raises(ReplicationError, match=r"tmr/vm1-root still 'failed' after 0s \(waiting for reestablished\): old failure"):
        be._wait_tmrs("a", "demo", ["vm1-root"], ("reestablished",))


def test_wait_tmrs_polls_until_all_are_ready(monkeypatch):
    import harvey.backends.netapp as na
    k = _CountingKube("a", [("tmr", "demo", _gen_tmr("d1", "reestablishing")), ("tmr", "demo", _gen_tmr("d2", "reestablished"))])

    def sleep(_):
        k.objs[("tmr", "demo", "d1")] = _gen_tmr("d1", "reestablished")
    monkeypatch.setattr(na.time, "sleep", sleep)
    be = NetAppBackend({"a": k}, {}, {"tmr": 60, "transfer": 1}, log=print)
    assert set(be._wait_tmrs("a", "demo", ["d1", "d2"], ("reestablished",))) == {"d1", "d2"}
    assert k.calls == [("list", "tmr"), ("get", "d1")]   # the second poll only has one TMR left


def test_recovery_point_of_a_volume_promoted_by_an_interrupted_failover(monkeypatch):
    # seen in the lab: --unplanned after a half-promoted failover printed RPO 'unknown'
    import datetime
    from harvey.backends import netapp as na
    k = FakeKube("b", [("tmr", "demo", _tmr("vm1-root", "established", "svm_dr:v1")),
                       ("tmr", "demo", _tmr("vm1-data", "promoted", "svm_dr:v2"))])
    be = NetAppBackend({"b": k}, {}, TIMEOUTS, log=print)

    class O:
        def relationship(self, h):
            return {"exported_snapshot": "snapmirror.x_2026-11-10_145100"} if h == "svm_dr:v1" else None

        def newest_snapmirror_snapshot(self, h):
            return "snapmirror.x_2026-11-10_145000"
    monkeypatch.setattr(be, "ontap_for_claim", lambda *a: O())
    monkeypatch.setattr(na, "now_utc", lambda: datetime.datetime(2026, 11, 10, 14, 52, 0, tzinfo=datetime.timezone.utc))
    assert be.recovery_point_age("a", "b", "demo", "vm1", ["vm1-root", "vm1-data"], src_up=True) == 120


def test_reprotect_retries_a_failed_reestablish_once_by_itself(monkeypatch):
    # seen in the lab: right after the peering came back one resync failed on a transient ONTAP state; a second
    # attempt worked, so harvey makes it before giving up
    be, dst = _reprotect_setup("promoted", "promoted")
    attempts = []
    real = dst.merge_patch

    def merge_patch(kind, ns, name, patch):
        r = real(kind, ns, name, patch)
        if (patch.get("spec") or {}).get("state") == "reestablished":
            attempts.append(1)
            cond = dst.objs[(kind, ns, name)]["status"]["conditions"][0]
            if len(attempts) == 1:
                cond.update({"state": "failed", "message": "Could not reestablish mirror"})
        return r
    dst.merge_patch = merge_patch
    be.reprotect("b", "a", "demo", "vm1", ["vm1-root"], wait_init=False)
    assert len(attempts) == 2


def test_reprotect_gives_up_after_the_automatic_retry_with_a_hint():
    be, dst = _reprotect_setup("promoted", "promoted")
    real = dst.merge_patch

    def merge_patch(kind, ns, name, patch):
        r = real(kind, ns, name, patch)
        if (patch.get("spec") or {}).get("state") == "reestablished":
            dst.objs[(kind, ns, name)]["status"]["conditions"][0].update({"state": "failed", "message": "Could not reestablish mirror"})
        return r
    dst.merge_patch = merge_patch
    with pytest.raises(ReplicationError, match=r"tmr/vm1-root failed: Could not reestablish mirror.*trident-controller.*run harvey reprotect again"):
        be.reprotect("b", "a", "demo", "vm1", ["vm1-root"], wait_init=False)


def test_promote_requests_every_volume_before_waiting():
    # a recovery group's disks are promoted together: all patches first, then one shared wait (one LIST per poll)
    events = []

    class K(_TridentKube):
        def merge_patch(self, kind, ns, name, patch):
            events.append(("patch", name))
            return super().merge_patch(kind, ns, name, patch)

        def list(self, kind, ns=None, label_selector=None):
            events.append(("list", kind))
            return super().list(kind, ns, label_selector)
    dst = K("b", [("tmr", "demo", _tmr(f"d{i}", "established", None)) for i in range(3)])
    be = NetAppBackend({"b": dst}, {}, TIMEOUTS, log=print)
    be.promote("a", "b", "demo", "vm1", ["d0", "d1", "d2"], src_up=True, unplanned=False)
    assert events == [("patch", "d0"), ("patch", "d1"), ("patch", "d2"), ("list", "tmr")]
