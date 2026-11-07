"""Minimal Kubernetes REST client used by Harvey.

Two ways to reach a cluster:
  * Rancher proxy  : <rancher_url>/k8s/clusters/<cluster_id> + Rancher API token
  * kubeconfig     : server + bearer token (or client cert) + CA from a kubeconfig file

Only plain Kubernetes / KubeVirt / Trident APIs are used, so both modes behave the same.
Errors raise KubeError; callers decide whether a failure is fatal.
"""

from __future__ import annotations

import atexit
import base64
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

import certifi
import requests
import yaml

requests.packages.urllib3.disable_warnings()  # self-signed lab endpoints are common


class KubeError(Exception):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


# ── resource paths ────────────────────────────────────────────────────────────
GROUPS = {
    "vm":        ("apis/kubevirt.io/v1", "virtualmachines"),
    "vmi":       ("apis/kubevirt.io/v1", "virtualmachineinstances"),
    "pvc":       ("api/v1", "persistentvolumeclaims"),
    "pv":        ("api/v1", "persistentvolumes"),
    "secret":    ("api/v1", "secrets"),
    "configmap": ("api/v1", "configmaps"),
    "namespace": ("api/v1", "namespaces"),
    "nad":       ("apis/k8s.cni.cncf.io/v1", "network-attachment-definitions"),
    "tmr":       ("apis/trident.netapp.io/v1", "tridentmirrorrelationships"),
    "tvol":      ("apis/trident.netapp.io/v1", "tridentvolumes"),
    "tbc":       ("apis/trident.netapp.io/v1", "tridentbackendconfigs"),
    "clusterpair":       ("apis/stork.libopenstorage.org/v1alpha1", "clusterpairs"),
    "migration":         ("apis/stork.libopenstorage.org/v1alpha1", "migrations"),
    "migrationschedule": ("apis/stork.libopenstorage.org/v1alpha1", "migrationschedules"),
    "schedulepolicy":    ("apis/stork.libopenstorage.org/v1alpha1", "schedulepolicies"),
    "dv":             ("apis/cdi.kubevirt.io/v1beta1", "datavolumes"),
    "lhvolume":       ("apis/longhorn.io/v1beta2", "volumes"),
    "lhengine":       ("apis/longhorn.io/v1beta2", "engines"),
    "lhsnapshot":     ("apis/longhorn.io/v1beta2", "snapshots"),
    "lhbackup":       ("apis/longhorn.io/v1beta2", "backups"),
    "lhbackupvolume": ("apis/longhorn.io/v1beta2", "backupvolumes"),
    "lhbackuptarget": ("apis/longhorn.io/v1beta2", "backuptargets"),
    "lhrecurringjob": ("apis/longhorn.io/v1beta2", "recurringjobs"),
    "lhbackingimage": ("apis/longhorn.io/v1beta2", "backingimages"),
    "clusternetwork": ("apis/network.harvesterhci.io/v1beta1", "clusternetworks"),
    "storageclass":   ("apis/storage.k8s.io/v1", "storageclasses"),
}
CLUSTER_SCOPED = {"pv", "namespace", "clusternetwork", "storageclass", "schedulepolicy"}


def path(kind: str, namespace: Optional[str] = None, name: Optional[str] = None) -> str:
    prefix, plural = GROUPS[kind]
    if kind in CLUSTER_SCOPED or namespace is None:
        p = f"{prefix}/{plural}"
    else:
        p = f"{prefix}/namespaces/{namespace}/{plural}"
    return f"{p}/{name}" if name else p


class KubeClient:
    """Thin REST wrapper. `name` is the friendly cluster name from Harvey's config."""

    def __init__(self, name: str, base_url: str, session: requests.Session, debug: bool = False):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.debug = debug

    # ── constructors ──────────────────────────────────────────────────────────
    @classmethod
    def from_rancher(cls, name: str, rancher_url: str, token: str, cluster_id: str, **kw) -> "KubeClient":
        s = requests.Session()
        s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})
        s.verify = False
        return cls(name, f"{rancher_url.rstrip('/')}/k8s/clusters/{cluster_id}", s, **kw)

    @classmethod
    def from_kubeconfig(cls, name: str, kubeconfig: str, context: Optional[str] = None, **kw) -> "KubeClient":
        cfg = yaml.safe_load(Path(os.path.expanduser(kubeconfig)).read_text())
        ctx_name = context or cfg.get("current-context")
        ctx = next(c for c in cfg["contexts"] if c["name"] == ctx_name)["context"]
        cluster = next(c for c in cfg["clusters"] if c["name"] == ctx["cluster"])["cluster"]
        user = next(u for u in cfg["users"] if u["name"] == ctx["user"])["user"]

        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        if cluster.get("insecure-skip-tls-verify"):
            s.verify = False
        elif cluster.get("certificate-authority-data"):
            # public roots + the kubeconfig CA: OpenSSL (unlike Go) rejects a partial chain, so a CA bundle that
            # holds only an intermediate fails against e.g. a public wildcard cert served by Rancher/Harvester.
            s.verify = _tmpfile(Path(certifi.where()).read_bytes() + b"\n"
                                + base64.b64decode(cluster["certificate-authority-data"]))
        elif cluster.get("certificate-authority"):
            s.verify = os.path.expanduser(cluster["certificate-authority"])
        if user.get("token"):
            s.headers["Authorization"] = f"Bearer {user['token']}"
        elif user.get("client-certificate-data"):
            s.cert = (_tmpfile(base64.b64decode(user["client-certificate-data"])),
                      _tmpfile(base64.b64decode(user["client-key-data"])))
        elif user.get("client-certificate"):
            s.cert = (os.path.expanduser(user["client-certificate"]), os.path.expanduser(user["client-key"]))
        else:
            raise KubeError(f"kubeconfig {kubeconfig}: unsupported auth (need token or client cert)")
        return cls(name, cluster["server"], s, **kw)

    # ── raw verbs ─────────────────────────────────────────────────────────────
    def request(self, method: str, p: str, *, params=None, json_body=None, content_type=None,
                allow_404: bool = False, timeout: int = 30):
        url = f"{self.base_url}/{p.lstrip('/')}"
        headers = {}
        if json_body is not None:
            headers["Content-Type"] = content_type or "application/json"
        if self.debug:
            print(f"DEBUG {self.name}: {method} {url}")
        try:
            r = self.session.request(method, url, params=params, json=json_body, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            raise KubeError(f"{self.name}: {method} {p}: {e}") from e
        if r.status_code == 404 and allow_404:
            return None
        if r.status_code >= 400:
            msg = r.text
            try:
                msg = r.json().get("message", msg)
            except ValueError:
                pass
            raise KubeError(f"{self.name}: {method} {p}: HTTP {r.status_code}: {msg[:400]}", r.status_code)
        return r.json() if r.text else {}

    def get(self, kind: str, namespace: Optional[str], name: str):
        """Returns the object or None if it does not exist."""
        return self.request("GET", path(kind, namespace, name), allow_404=True)

    def list(self, kind: str, namespace: Optional[str] = None, label_selector: Optional[str] = None) -> list:
        params = {"labelSelector": label_selector} if label_selector else None
        data = self.request("GET", path(kind, namespace), params=params, allow_404=True)
        return (data or {}).get("items", [])

    def create(self, kind: str, namespace: Optional[str], body: dict):
        return self.request("POST", path(kind, namespace), json_body=body)

    def merge_patch(self, kind: str, namespace: Optional[str], name: str, patch: dict):
        return self.request("PATCH", path(kind, namespace, name), json_body=patch,
                            content_type="application/merge-patch+json")

    def replace(self, kind: str, namespace: Optional[str], name: str, body: dict):
        return self.request("PUT", path(kind, namespace, name), json_body=body)

    def delete(self, kind: str, namespace: Optional[str], name: str, propagation: Optional[str] = None):
        body = {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": propagation} if propagation else None
        return self.request("DELETE", path(kind, namespace, name), json_body=body, allow_404=True)

    def apply(self, kind: str, namespace: Optional[str], body: dict) -> str:
        """Create, or update in place keeping resourceVersion. Returns 'created' / 'updated'."""
        name = body["metadata"]["name"]
        cur = self.get(kind, namespace, name)
        if cur is None:
            self.create(kind, namespace, body)
            return "created"
        body = dict(body)
        body["metadata"] = dict(body["metadata"], resourceVersion=cur["metadata"]["resourceVersion"])
        self.replace(kind, namespace, name, body)
        return "updated"

    def ensure_namespace(self, name: str):
        if self.get("namespace", None, name) is None:
            self.create("namespace", None, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})

    def reachable(self) -> bool:
        try:
            self.request("GET", "version", timeout=10)
            return True
        except KubeError:
            return False

    def wait_for(self, fn, timeout_s: int, interval: float = 3.0, what: str = "condition"):
        """Polls fn() until it returns a truthy value; returns it, or raises KubeError on timeout."""
        deadline = time.time() + timeout_s
        last_err = None
        while time.time() < deadline:
            try:
                v = fn()
                if v:
                    return v
            except KubeError as e:
                last_err = e
            time.sleep(interval)
        raise KubeError(f"{self.name}: timed out after {timeout_s}s waiting for {what}"
                        + (f" (last error: {last_err})" if last_err else ""))


_TMPFILES: list = []


def _tmpfile(data: bytes) -> str:
    f = tempfile.NamedTemporaryFile(prefix="harvey-", delete=False)
    f.write(data)
    f.close()
    os.chmod(f.name, 0o600)
    _TMPFILES.append(f.name)
    return f.name


@atexit.register
def _cleanup_tmpfiles():
    for f in _TMPFILES:
        try:
            os.unlink(f)
        except OSError:
            pass
