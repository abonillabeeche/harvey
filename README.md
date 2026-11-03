# Harvey — DR Orchestration CLI for KubeVirt / SUSE Virtualization

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/abonillabeeche/harvey)](https://github.com/abonillabeeche/harvey/releases)

Harvey automates storage-aware Disaster Recovery of virtual machines between two KubeVirt
(SUSE Virtualization / Harvester) clusters:

- **Disks** are replicated by **NetApp ONTAP SnapMirror**, driven through Trident
  `TridentMirrorRelationship` (TMR) objects, so the replica volumes are normal PVCs on the DR cluster.
- **Metadata** (the VM spec, cloud-init / credential Secrets, ConfigMaps, NetworkAttachmentDefinitions) is
  copied by Harvey. The replica VM sits on the DR cluster, stopped.
- **Failover** = stop on source → final SnapMirror update → promote the replica volumes → start on DR
  (lossless when planned). **Unplanned** failover promotes the last replicated data and reports the data loss
  window. **Reprotect** reverse-resyncs; **failback** = planned failover back + reprotect.

Who is primary is never stored locally: Harvey derives it from labels on the VMs and the TMR states on both
clusters.

---

## What Harvey does

- Protect VMs: per-PVC SnapMirror + metadata copy, replica VM created stopped
- Replication status: per-volume SnapMirror state, health, **lag** and recovery point (from the ONTAP REST API)
- Metadata sync (one-shot or `--watch`), so spec changes reach the replica
- Planned failover with a final SnapMirror update → RPO 0, RTO measured per phase
- Unplanned failover (`--unplanned`) with an RPO estimate per VM
- Reprotect (reverse resync) and one-command failback
- Recovery Plans (YAML runbooks), priority groups, startup delays, pre/post shell hooks
- Split-brain detection, health-check (incl. replication lag threshold), JSONL audit log

---

## Requirements

- Two Harvester / KubeVirt clusters, each with **Trident ≥ 22.10** (tested 26.06) on an ONTAP backend
- The two ONTAP clusters **cluster-peered** and the SVMs **vserver-peered** (`-applications snapmirror`),
  SnapMirror licensed on both; **unique SVM names** on the two sides
- An ONTAP job schedule (e.g. the built-in `5min`, or a custom cron) on both ONTAP clusters
- VM disks on Trident `ontap-san` (iSCSI / NVMe-TCP) or `ontap-nas` StorageClasses. VMs created from Harvester
  images (FlexClones of the image volume) work: SnapMirror transfers them as full, independent copies
- ONTAP credentials are **discovered**: for every PVC Harvey follows PV → TridentVolume → TridentBackendConfig
  and uses its `managementLIF` + credentials Secret. An SVM-scoped `vsadmin` login is enough.

---

## Installation

### Recommended: uv

[uv](https://docs.astral.sh/uv/) manages CLI tools in isolated environments and bundles its own Python — no venv setup, no Homebrew Python conflicts.

```bash
# Install uv if you don't have it
brew install uv        # macOS
# or: curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Harvey
uv tool install git+https://github.com/abonillabeeche/harvey.git
```

After installation, run `harvey --help`.

### Upgrading

```bash
uv tool upgrade harvey
```

### Alternative: pipx

```bash
# Install pipx if you don't have it
pip install --user pipx && pipx ensurepath

pipx install git+https://github.com/abonillabeeche/harvey.git

# Upgrade
pipx upgrade harvey
```

### Specific version from a release

Download the `.whl` from the [Releases page](https://github.com/abonillabeeche/harvey/releases) and install it directly:

```bash
uv tool install harvey-X.Y.Z-py3-none-any.whl
```

### From source

```bash
git clone https://github.com/abonillabeeche/harvey.git
cd harvey
pip install .
```

---

## Configuration

```bash
harvey config init        # writes ~/.config/harvey/config.yaml  (or $HARVEY_CONFIG_DIR/config.yaml)
```

```yaml
# Needed only for clusters reached through the Rancher proxy
rancher_url: "https://rancher.mydomain.com"
api_token:   "token-abcde:1234567890abcdefghijklmnopqrstuvwxyz"

clusters:
  primary:
    rancher_cluster_id: c-m-xxxxxxxx      # via Rancher ...
  dr:
    kubeconfig: ~/.kube/dr.yaml           # ... or a kubeconfig (token or client cert); context: optional
    # ontap: {host: 192.0.2.10, username: vsadmin, password_env: DR_ONTAP_PW}   # optional override

defaults:                                  # so --source/--destination can be omitted
  source: primary
  destination: dr

replication:
  policy: MirrorAllSnapshots               # async SnapMirror policy (Trident supports async only)
  schedule: 5min                           # ONTAP schedule name; sets the RPO for unplanned failovers.
                                           # 5 minutes is NetApp's supported minimum (1 min works but is unsupported).
  storage_class_map: {}                    # source SC -> DR SC when the names differ
  max_lag_seconds: 900                     # health-check threshold
  timeouts: {stop: 300, start: 600, tmr: 300, transfer: 1800, stall: 120}
```

The old format (`clusters: {primary: "c-m-xxxx"}`) still works.

---

## Commands

### protect / unprotect

```bash
harvey protect  -n production db-1 app-1 [--wait]        # --wait: block until the baseline transfer is done
harvey unprotect -n production db-1 [--delete-replica]
```

`protect` (idempotent):
1. creates a TMR `state: promoted` per PVC on the source to get each ONTAP volume handle;
2. copies Secrets / ConfigMaps / NADs and creates the replica VM (stopped, `runStrategy: Halted`) on DR;
3. creates a TMR `state: established` + a PVC annotated `trident.netapp.io/mirrorRelationship` per disk on DR.

The replica VM keeps the same name, namespace, MAC addresses and disk names. PVC templates and image references are
dropped on the replica (its disks are the mirrored PVCs). NADs that already exist on DR are left alone (they may be
site-specific); a missing cluster network is reported.

### replication status

```bash
harvey replication status [-n production]
```

Per volume: TMR state on each side, SnapMirror state, healthy, **lag** (time since the newest replicated
snapshot = worst-case data loss for an unplanned failover), recovery point, notes. Shows `SPLIT BRAIN` if a VM
runs on both sides and `needs reprotect` after a failover.

### sync

```bash
harvey sync [-n production] [vm ...] [--watch 300]
```

Copies VM metadata from each VM's current primary to its replica (direction detected per VM). The replica
always stays stopped. Run it from cron / `--watch` to keep spec changes in step; failover also syncs first.

### failover

```bash
harvey failover vm db-1 app-1 -n production [--dry-run] [--yes]
harvey failover namespace production
harvey failover all
harvey failover vm db-1 -n production --unplanned [--force]   # source site down
```

Planned: metadata sync → stop on source (lowest priority first) → final SnapMirror update per volume (waits for
the transfer, aborts + retries a stalled one) → promote → start on DR (highest priority first) → summary with RTO
per phase and **RPO 0**.

Unplanned: no stop, no final sync; promote + start. RPO = age of the newest replicated snapshot (reported).
Refuses if the VM is still running on a reachable source (split-brain) unless `--force`.

### reprotect / failback

```bash
harvey reprotect -n production db-1 [--wait]     # after a failover: replicate back (overwrites the old copy)
harvey failback  -n production db-1              # planned failover back + reprotect
```

### Ordering labels (on the VirtualMachine)

```yaml
metadata:
  labels:
    failover-priority:     "100"   # higher = stopped LAST, started FIRST
    failover-startup-time: "60"    # seconds to pause after starting this VM
```

### plan — Recovery Plans

```bash
harvey plan new production-dr && harvey plan edit production-dr
harvey plan run production-dr --dry-run
harvey plan run production-dr                  # planned failover with hooks
harvey plan run production-dr --unplanned      # site down
harvey plan run production-dr --reverse        # run it destination → source (failback, after reprotect)
harvey plan run production-dr --test           # shadow-namespace boot test (see limitations)
```

Plan files keep the same structure (protection groups with `priority`, `startup_delay`, `vms`, and
`hooks.pre_failover|pre_start|post_failover` with `HARVEY_SOURCE` / `HARVEY_DESTINATION` in the environment).

### health-check, status, audit

```bash
harvey health-check primary dr     # API reachability, split-brain, replication health + lag threshold, plans
harvey status primary dr           # VM state side by side
harvey audit --limit 100           # ~/.config/harvey/audit.log (JSONL), incl. RTO/RPO per failover
```

---

## Typical workflow

```
harvey protect -n prod db-1 app-1 --wait            # once
harvey sync --watch 300 &                           # keep metadata in step
harvey health-check primary dr                      # cron
harvey failover vm db-1 app-1 -n prod               # planned: RPO 0
harvey reprotect -n prod db-1 app-1                 # DR -> primary replication
harvey failback  -n prod db-1 app-1                 # back home, replication primary -> DR again
```

---

## Known limitations

- **Async only.** Trident TMRs support asynchronous SnapMirror policies only, with a supported minimum schedule of
  5 minutes. The ONTAP `Sync` policy works between the clusters but has no Trident integration (NetApp's zero-RPO
  answer is MetroCluster). Keep the array's concurrent-transfer limit (100 or 250, by platform) in mind at scale.
- **Per-volume consistency.** Each disk is its own SnapMirror relationship; after an *unplanned* failover the disks
  of one VM can come from different points in time (seen: one disk 1 min older than the others). Planned failovers
  are unaffected (VM stopped + final update). ONTAP consistency groups are not available through Trident.
- **Trident promote quirk (26.06 / ONTAP 9.16.1).** A TMR can report `promoted` while the volume is still DP:
  when a scheduled transfer is running (break fails with "snapmirror transfer already in progress") or when the
  relationship is already quiesced. Harvey aborts a running transfer before promoting and then verifies the volume
  type in ONTAP, breaking the relationship itself if needed.
- **`--test` mode** boots copies of the VM spec in shadow namespaces but does not clone the replica volumes yet.

---

## Releasing

To publish a new release, tag the commit and push the tag. GitHub Actions builds the wheel and creates the release automatically:

```bash
git tag v0.2.0
git push origin v0.2.0
```

The workflow attaches `harvey-X.Y.Z-py3-none-any.whl` and the source tarball to the GitHub Release.
Remember to update `version` in `pyproject.toml` and `src/harvey/__init__.py` before tagging.

---

## Contributing

Issues and pull requests are welcome. Please open an issue first to discuss significant changes.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

---

## Version history

| Version | Key additions |
|---|---|
| 0.2 | Storage-aware DR: NetApp SnapMirror via Trident TMRs, protect/unprotect, replication status with lag, metadata sync, planned (lossless) and unplanned failover, reprotect, failback; kubeconfig or Rancher access; ONTAP credential discovery |
| 0.1 | Basic stop/start failover, namespace/all commands, priority labels, startup delays, dry-run, recovery plans, DR test mode, hooks, RTO tracking, audit log, health-check, reprotect guidance |
