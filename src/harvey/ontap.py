"""ONTAP REST client: just the SnapMirror calls Harvey needs.

Credentials are normally discovered from the cluster itself: the Trident backend (TridentBackendConfig) that
hosts a volume carries the ONTAP management LIF and a Secret with username/password. An SVM-scoped (vsadmin)
login is enough: SnapMirror relationships are visible and updatable from the destination SVM.
"""

from __future__ import annotations

import datetime
import re
import time
from typing import Optional

import requests

requests.packages.urllib3.disable_warnings()


class OntapError(Exception):
    pass


class StalledTransfer(OntapError):
    """A transfer stopped making progress (seen on overloaded ONTAP simulators)."""


REL_FIELDS = ("uuid,state,healthy,unhealthy_reason,lag_time,source.path,destination.path,policy.name,"
              "transfer_schedule.name,transfer.state,transfer.uuid,transfer.bytes_transferred,"
              "transfer.last_updated_time,last_transfer_type,exported_snapshot")


def parse_iso_duration(s: Optional[str]) -> Optional[int]:
    """'PT1H2M3S' / 'P1DT2H' -> seconds. None stays None."""
    if not s:
        return None
    m = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", s)
    if not m:
        return None
    d, h, mi, se = (int(x) if x else 0 for x in m.groups())
    return ((d * 24 + h) * 60 + mi) * 60 + se


def snapshot_time(exported_snapshot: Optional[str]) -> Optional[datetime.datetime]:
    """SnapMirror snapshot names end in _YYYY-MM-DD_HHMMSS (UTC) -> datetime. That is the recovery point."""
    if not exported_snapshot:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{6})$", exported_snapshot)
    if not m:
        return None
    return datetime.datetime.strptime(m.group(1) + m.group(2), "%Y-%m-%d%H%M%S").replace(tzinfo=datetime.timezone.utc)


class OntapClient:
    def __init__(self, host: str, username: str, password: str, verify: bool = False, debug: bool = False):
        self.host = host
        self.base = f"https://{host}/api"
        self.s = requests.Session()
        self.s.auth = (username, password)
        self.s.verify = verify
        self.s.headers.update({"Accept": "application/json"})
        self.debug = debug

    def _req(self, method: str, p: str, **kw):
        url = f"{self.base}/{p.lstrip('/')}"
        if self.debug:
            print(f"DEBUG ontap {self.host}: {method} {url} {kw.get('params') or ''}")
        try:
            r = self.s.request(method, url, timeout=30, **kw)
        except requests.RequestException as e:
            raise OntapError(f"ONTAP {self.host}: {e}") from e
        if r.status_code >= 400:
            try:
                msg = r.json()["error"]["message"]
            except (ValueError, KeyError):
                msg = r.text[:300]
            raise OntapError(f"ONTAP {self.host}: {method} {p}: HTTP {r.status_code}: {msg}")
        return r.json() if r.text else {}

    # ── SnapMirror ────────────────────────────────────────────────────────────
    def relationship(self, destination_path: str) -> Optional[dict]:
        """destination_path = 'svm:volume'. Returns the relationship record (queried on the destination ONTAP)."""
        d = self._req("GET", "snapmirror/relationships",
                      params={"destination.path": destination_path, "fields": REL_FIELDS})
        recs = d.get("records", [])
        return recs[0] if recs else None

    def relationship_by_source(self, source_path: str) -> Optional[dict]:
        """Relationship whose source is 'svm:volume' (queried on the destination ONTAP)."""
        d = self._req("GET", "snapmirror/relationships",
                      params={"source.path": source_path, "fields": REL_FIELDS})
        recs = d.get("records", [])
        return recs[0] if recs else None

    def update(self, rel_uuid: str) -> str:
        """Starts a SnapMirror update (transfer). Returns the transfer uuid (may be '' if ONTAP returns a job)."""
        d = self._req("POST", f"snapmirror/relationships/{rel_uuid}/transfers",
                      params={"return_records": "true"}, json={})
        recs = d.get("records") or []
        return recs[0].get("uuid", "") if recs else ""

    def transfer(self, rel_uuid: str, transfer_uuid: str) -> dict:
        return self._req("GET", f"snapmirror/relationships/{rel_uuid}/transfers/{transfer_uuid}",
                         params={"fields": "state,bytes_transferred,end_time,error_info"})

    def set_state(self, rel_uuid: str, state: str):
        """PATCH the relationship state: 'paused' (= quiesce), 'broken_off' (= break), 'snapmirrored' (= resync)."""
        self._req("PATCH", f"snapmirror/relationships/{rel_uuid}", json={"state": state})

    def volume_type(self, handle: str) -> Optional[str]:
        """'svm:volume' -> 'rw' | 'dp' | 'ls' (None if not found)."""
        svm, vol = handle.split(":", 1)
        d = self._req("GET", "storage/volumes", params={"svm.name": svm, "name": vol, "fields": "type"})
        recs = d.get("records", [])
        return recs[0].get("type") if recs else None

    def abort(self, rel_uuid: str, transfer_uuid: str):
        self._req("PATCH", f"snapmirror/relationships/{rel_uuid}/transfers/{transfer_uuid}", json={"state": "aborted"})

    def wait_update(self, rel_uuid: str, transfer_uuid: str, timeout_s: int = 600, stall_s: int = 120) -> dict:
        """Waits for a transfer to finish. Returns the final transfer record. Raises StalledTransfer (after aborting
        it) if bytes_transferred does not move for stall_s, OntapError on failure/timeout."""
        deadline = time.time() + timeout_s
        last_bytes, last_change = -1, time.time()
        while time.time() < deadline:
            t = self.transfer(rel_uuid, transfer_uuid)
            st = t.get("state")
            if st == "success":
                return t
            if st in ("failed", "aborted", "hard_aborted"):
                raise OntapError(f"transfer {transfer_uuid} {st}: {t.get('error_info')}")
            b = int(t.get("bytes_transferred") or 0)
            if b != last_bytes:
                last_bytes, last_change = b, time.time()
            elif time.time() - last_change > stall_s:
                try:
                    self.abort(rel_uuid, transfer_uuid)
                except OntapError:
                    pass
                raise StalledTransfer(f"transfer {transfer_uuid} stalled at {b} B for {stall_s}s (aborted)")
            time.sleep(3)
        raise OntapError(f"transfer {transfer_uuid} still running after {timeout_s}s")
