import pytest
from typer.testing import CliRunner

import harvey.cli as cli


class NsScopedClient:
    """Like a namespace-scoped user: a cluster-wide VM list fails (403 -> None), a namespaced one works."""
    def __init__(self, role):
        self.role, self.listed = role, []

    def get_vms(self, namespace=None):
        self.listed.append(namespace)
        if namespace is None:
            return None
        return {"items": [{"metadata": {"namespace": namespace, "name": "vm1", "labels": {cli.L_ROLE: self.role}},
                           "status": {"printableStatus": "Running"}}]}


class Rep:
    def direction(self, ns, vm, a, b):
        return "a", "b"

    def unit(self, cluster, ns, vm):
        return type("U", (), {"vms": [vm]})()


@pytest.fixture
def env(monkeypatch):
    clients = {"a": NsScopedClient("primary"), "b": NsScopedClient("replica")}
    plans = []
    monkeypatch.setattr(cli, "load_config", lambda: {})
    monkeypatch.setattr(cli, "resolve_pair", lambda c, s, d: ("a", "b"))
    monkeypatch.setattr(cli, "get_clients", lambda *a: (clients["a"], clients["b"]))
    monkeypatch.setattr(cli, "make_client", lambda config, name: clients[name])
    monkeypatch.setattr(cli, "make_replicator", lambda *a: Rep())
    monkeypatch.setattr(cli, "_ad_hoc_failover", lambda config, plan, *a, **k: plans.append(plan))
    return clients, plans


@pytest.mark.parametrize("args", [["failover", "vm", "-n", "ns1", "vm1"], ["failover", "namespace", "ns1"],
                                  ["failback", "vm1", "-n", "ns1", "--no-reprotect", "--yes"]])
def test_failover_and_failback_list_only_the_given_namespace(env, args):
    clients, plans = env
    r = CliRunner().invoke(cli.app, args)
    assert r.exit_code == 0, r.output
    assert [c.listed for c in clients.values()] == [["ns1"], ["ns1"]]
    assert plans == [[("ns1", "vm1", 0, 0)]]


def test_status_lists_only_the_given_namespace(env):
    clients, _ = env
    r = CliRunner().invoke(cli.app, ["status", "a", "b", "-n", "ns1"])
    assert [c.listed for c in clients.values()] == [["ns1"], ["ns1"]]
    assert "vm1" in r.output and "Could not retrieve" not in r.output
