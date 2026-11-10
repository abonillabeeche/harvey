"""CLI output details: the failover summary's failure lines and the status DR State column."""
from harvey import cli
from harvey.common import A_REPROTECT


def _vm(status, annotations=None):
    return {"metadata": {"annotations": annotations or {}}, "status": {"printableStatus": status}}


def test_failure_lines_keep_the_phase_and_drop_the_placeholder_vm(capsys):
    cli._print_failover_summary({"stopped": [], "started": [], "synced": [], "rpo": {}, "phases": {},
                                 "rto_seconds": None,
                                 "failed": [("-", "preflight", "ns/db on b holds [old] data. Please run harvey reprotect first."),
                                            ("ns/web", "start", "timed out")]})
    out = capsys.readouterr().out
    assert "• (preflight): ns/db on b holds [old] data." in out     # brackets survive, no "- :" placeholder
    assert "• ns/web (start): timed out" in out


def test_status_flags_a_copy_that_needs_reprotect():
    assert cli._dr_state(_vm("Running"), _vm("Stopped", {A_REPROTECT: "true"}))[2] == "reprotect"
    assert cli._dr_state(_vm("Stopped", {A_REPROTECT: "true"}), _vm("Running"))[2] == "reprotect"
    assert cli._dr_state(_vm("Running"), _vm("Stopped"))[2] == "ready"
    assert cli._dr_state(_vm("Running"), _vm("Running"))[2] == "split"
    assert cli._dr_state(_vm("Running"), None)[2] == "standalone"



def test_summary_names_the_sites_and_flags_vms_down_on_both(capsys):
    # seen in the lab: a planned failover of a half-promoted pair stopped the VM, failed at final sync, printed
    # "Total RTO" and "started on DR" for a DR -> retail failover, and gave no hint
    cli._print_failover_summary({"stopped": [("ns", "vm1")], "started": [], "synced": [], "rpo": {},
                                 "phases": {"stop": 10}, "rto_seconds": 31,
                                 "failed": [("ns/vm1", "final-sync", "no SnapMirror relationship")]},
                                "disaster-recovery", "retail-store")
    out = capsys.readouterr().out
    assert "VMs stopped on disaster-recovery" in out and "VMs started on retail-store" in out
    assert "Total RTO" not in out and "Elapsed (incomplete)" in out
    assert "1 VM(s) are stopped on disaster-recovery and weren't started on retail-store: ns/vm1" in out
    assert "--unplanned" in out


def test_summary_of_a_clean_failover_has_no_hint(capsys):
    cli._print_failover_summary({"stopped": [("ns", "vm1")], "started": ["ns/vm1"], "synced": [], "rpo": {"ns/vm1": 0},
                                 "phases": {}, "rto_seconds": 73, "failed": []}, "a", "b")
    out = capsys.readouterr().out
    assert "Total RTO" in out and "weren't started" not in out
