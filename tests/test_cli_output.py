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

