import datetime

from harvey.ontap import parse_iso_duration, snapshot_time
from harvey.replication import (A_RUN, L_ROLE, build_replica_vm, original_run_strategy, vm_claims,
                                vm_configmap_names, vm_nads, vm_secret_names)


def _vm(**spec_overrides):
    vm = {
        "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine",
        "metadata": {
            "name": "db-1", "namespace": "prod", "uid": "abc", "resourceVersion": "42",
            "labels": {"app": "db", "harvey.io/role": "primary"},
            "annotations": {
                "harvesterhci.io/volumeClaimTemplates": "[...]",
                "kubevirt.io/latest-observed-api-version": "v1",
                "kubectl.kubernetes.io/last-applied-configuration": "{}",
                "field.cattle.io/description": "keep me",
            },
        },
        "spec": {
            "runStrategy": "RerunOnFailure",
            "dataVolumeTemplates": [{"metadata": {"name": "db-1-dv"}}],
            "template": {"spec": {
                "domain": {"devices": {"interfaces": [{"name": "nic", "macAddress": "02:00:00:00:00:01"}]}},
                "networks": [{"name": "default", "pod": {}},
                             {"name": "nic", "multus": {"networkName": "vlan10"}},
                             {"name": "nic2", "multus": {"networkName": "infra/vlan20"}}],
                "volumes": [
                    {"name": "root", "persistentVolumeClaim": {"claimName": "db-1-root"}},
                    {"name": "data", "dataVolume": {"name": "db-1-dv"}},
                    {"name": "ci", "cloudInitNoCloud": {"secretRef": {"name": "db-1-ci"},
                                                        "networkDataSecretRef": {"name": "db-1-net"}}},
                    {"name": "cfg", "configMap": {"name": "db-1-cfg"}},
                ],
                "accessCredentials": [{"sshPublicKey": {"source": {"secret": {"secretName": "keys"}}}}],
            }},
        },
        "status": {"printableStatus": "Running"},
    }
    vm["spec"].update(spec_overrides)
    return vm


def test_discovery():
    vm = _vm()
    assert vm_claims(vm) == ["db-1-root", "db-1-dv"]
    assert vm_secret_names(vm) == ["db-1-ci", "db-1-net", "keys"]
    assert vm_configmap_names(vm) == ["db-1-cfg"]
    assert vm_nads(vm) == [("infra", "vlan20"), ("prod", "vlan10")]


def test_replica_is_halted_and_clean():
    r = build_replica_vm(_vm(), "site-a", "site-a")
    assert r["spec"]["runStrategy"] == "Halted"
    assert "dataVolumeTemplates" not in r["spec"]
    assert "status" not in r
    md = r["metadata"]
    assert "uid" not in md and "resourceVersion" not in md
    assert md["labels"]["app"] == "db" and md["labels"][L_ROLE] == "replica"
    assert "harvesterhci.io/volumeClaimTemplates" not in md["annotations"]
    assert "kubectl.kubernetes.io/last-applied-configuration" not in md["annotations"]
    assert md["annotations"]["field.cattle.io/description"] == "keep me"
    assert md["annotations"][A_RUN] == "RerunOnFailure"
    vols = {v["name"]: v for v in r["spec"]["template"]["spec"]["volumes"]}
    assert vols["data"] == {"name": "data", "persistentVolumeClaim": {"claimName": "db-1-dv"}}
    # MACs travel with the spec
    assert r["spec"]["template"]["spec"]["domain"]["devices"]["interfaces"][0]["macAddress"] == "02:00:00:00:00:01"


def test_original_run_strategy():
    assert original_run_strategy(_vm(runStrategy="Always")) == "Always"
    assert original_run_strategy(_vm(runStrategy="Halted")) == "RerunOnFailure"
    vm = _vm(runStrategy="Halted")
    vm["metadata"]["annotations"][A_RUN] = "Manual"
    assert original_run_strategy(vm) == "Manual"
    legacy = _vm()
    legacy["spec"].pop("runStrategy")
    legacy["spec"]["running"] = True
    assert original_run_strategy(legacy) == "Always"


def test_ontap_parsing():
    assert parse_iso_duration("PT1M5S") == 65
    assert parse_iso_duration("PT4M17S") == 257
    assert parse_iso_duration("P1DT2H") == 93600
    assert parse_iso_duration(None) is None
    t = snapshot_time("snapmirror.c74be8fc-d730-11f1-b26e-0050563a0200_2148306386.2026-11-03_033100")
    assert t == datetime.datetime(2026, 11, 3, 3, 31, 0, tzinfo=datetime.timezone.utc)
    assert snapshot_time("hourly.0") is None
