# Changelog

All notable changes to Harvey are documented here.

## [0.3.0] — unreleased

Pluggable storage backends and Portworx async DR.

### Added
- **Storage backend layer** (`harvey.backends`): `StorageBackend` interface plus a registry keyed by CSI driver.
  The backend is picked per VM and recorded in `harvey.io/storage-backend`.
- **Portworx backend**: Stork volume-only `MigrationSchedule` per VM (PV+PVC selected by `harvey.io/vm`),
  final one-off Migration for planned failovers (RPO 0), fencing through Stork's `applicationActivated` flag with
  `autoSuspend`, reprotect that replaces the old direction's schedules, `--delete-replica` restoring the PV reclaim
  policy. See docs/portworx.md.
- **Longhorn backend**: Longhorn DR volumes (V1 lab-tested; V2 data engine alpha). Backups go to a shared Longhorn
  BackupTarget (dedicated `harvey` target by default); standby DR volumes restore them incrementally; planned
  failover = final backup + wait for its restore (RPO 0); promote = activate + PV/PVC from stored templates;
  reprotect = DR volume of the new primary. Works around synced Backup CRs with `backupBlockSize: -1` and
  Harvester's PVC-in-use webhook. V1 lab-tested between Longhorn 1.12.1 and 1.13.0. See docs/longhorn.md.
- **Recovery groups**: `harvey group add/remove/list/show` (label `harvey.io/recovery-group` on VMs and standalone
  PVCs); protect, failover, reprotect, failback and unprotect act on whole groups; a group's storage is synced and
  promoted once, after all its VMs stopped; `failover group`; `--group` on protect/reprotect/unprotect
- Longhorn: a restore counts only when the engine reports it (`lastRestoredBackup`), not the volume's
  `lastBackup`; wait for the synced Backup CR's `status.backupTargetName` before creating a DR volume
- **Shared-disk guard**: any operation on a VM that shares a disk with a VM outside its group is refused, with
  the `harvey group add` command to fix it
- `harvey.io/primary-since`: after an unplanned failover the newer primary wins, the stale copy is shown in
  `replication status` / `health-check` and demoted by `reprotect` (only while stopped)
- `replication status` shows the backend; config sections `replication.netapp` and `replication.portworx`

### Changed
- NetApp logic moved to `backends/netapp.py` without behaviour changes; 0.2 configs (top-level
  `replication.policy` / `schedule`) still work
- `health-check` and `replication status` ignore VMs protected towards a third cluster
- Portworx selects PVCs by `harvey.io/protection-unit` (was `harvey.io/vm`, which a shared disk overwrote)

### Fixed
- `plan run --test`: Harvester and KubeVirt accept the shadow VM (no copied MACs, no `spec.running` next to
  `runStrategy`, no `harvey.io/*` labels or `dataVolumeTemplates`); `--yes` skips the confirmation
- Portworx unplanned RPO uses the migration trigger time (the PVC `migrationTime` is the apply time and
  under-reported the loss by the transfer time)

## [0.2.0] — unreleased

Storage-aware DR with NetApp ONTAP SnapMirror (through Trident TridentMirrorRelationships).

### Added
- **protect / unprotect**: per-PVC SnapMirror + copy of VM spec, Secrets, ConfigMaps, NADs; replica VM created stopped
- **replication status**: per-volume TMR/SnapMirror state, health, lag and recovery point from the ONTAP REST API
- **sync** (`--watch`): metadata sync from each VM's primary to its replica
- **failover**: planned = metadata sync, stop, final SnapMirror update (stall detection), promote, start; RPO 0
- **failover --unplanned / plan run --unplanned**: promote + start, reports RPO per VM
- **reprotect** (reverse resync) and **failback** (planned failover back + reprotect); `plan run --reverse`
- health-check checks replication health and lag (`replication.max_lag_seconds`)
- clusters can be reached through a kubeconfig as well as the Rancher proxy; `HARVEY_CONFIG_DIR`
- ONTAP endpoint + credentials discovered from Trident (TridentBackendConfig managementLIF + Secret)
- `version` command

### Changed
- start/stop use `spec.runStrategy` (plain Kubernetes API; no Harvester action API needed)
- failover only acts on protected VMs (primary on source, replica on destination)
- `failback` is no longer advisory: it performs the failback

### Worked around
- Trident 26.06 TMR reporting `promoted` while the volume is still DP (verified and broken via ONTAP)

## [0.1.0] — 2026-05-13

Initial public release.

### Features
- **status** — compare VM state across primary and DR clusters with split-brain detection
- **health-check** — pre-flight readiness: API connectivity, VM coverage, saved plans
- **failover vm / namespace / all** — ordered stop → verify → start with priority groups
- **plan new / list / show / run** — YAML recovery runbooks with protection groups and shell hooks
- **plan run --test** — non-disruptive DR test via shadow namespaces (mirrors VMware SRM Test Recovery)
- **plan run --dry-run** — print ordered execution plan without touching VMs
- **reprotect** — guided reverse-failover workflow after DR activation
- **audit** — structured JSONL audit log of all operations with RTO tracking
- **config init / show** — bootstrap and inspect configuration (token masked)
- `--debug` global flag for verbose API tracing
