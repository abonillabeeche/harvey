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
- **README: "Design for scale"**: reprotect is heavy, Harvey runs one unit at a time, and recovery groups are the
  main lever for consistency, speed and blast radius at hundreds of VMs.
- **`plan run --test` is a roadmap item.** It booted the VM spec in a shadow namespace without the replica
  volumes, so the test VMs couldn't start and the test proved little. It now explains that DR test mode isn't
  available yet and changes nothing. The shadow-VM spec builder stays for the real feature.
- **Clearer, more courteous messages.** Errors, warnings and hints now say what's wrong and then what to do
  ("Please name the group explicitly", "Please stop it first, because reprotect overwrites its disks"),
  in full sentences, without blunt imperatives or all-caps. The Longhorn V2 warning now recommends Longhorn
  1.13.1 or later instead of calling V2 untested.
- NetApp logic moved to `backends/netapp.py` without behaviour changes; 0.2 configs (top-level
  `replication.policy` / `schedule`) still work
- `health-check` and `replication status` ignore VMs protected towards a third cluster
- Portworx selects PVCs by `harvey.io/protection-unit` (was `harvey.io/vm`, which a shared disk overwrote)
- `failover vm -n`, `failover namespace`, `failback` and `status -n` list VMs only in the given namespace (a
  namespace-scoped identity got HTTP 403 from the cluster-wide list)

### Fixed
- **NetApp: a unit's volumes are promoted together**: every promote is requested first, then one shared wait,
  then the read-write check per volume. A recovery group's promote takes about as long as its slowest disk.
- **NetApp: TMR waits fail fast and scale.** A TMR whose status is `failed` for the current spec ends the wait at
  once instead of after `timeouts.tmr` (a reprotect with the intercluster LIF down: 306 s → 13 s). Waits for several
  disks use one LIST of the namespace's TMRs per poll instead of one GET per TMR. Failures are reported one line per
  disk with Trident's message and remote volume, not as a raw condition dict of the first disk.
- **NetApp: reprotect retries a failed reestablish once by itself**, then points to Trident's log for ONTAP's reason
  (seen in the lab: right after the peering came back, a stale relationship's scheduled update made one resync fail).
- **`harvey reprotect` with several VMs keeps going** after a failed VM and exits 1 at the end with the list,
  instead of stopping at the first failure.
- **Failover summary:** row labels name the clusters (not "DR" for the reverse direction); a failover with failures
  shows "Elapsed (incomplete)" instead of "Total RTO"; VMs stopped on the source but not started on the destination
  get a hint (rerun, finish with `--unplanned`, or start them on the source again). `plan run` exits 1 on failures.
- **NetApp: RPO of a resumed failover.** `--unplanned` after a half-promoted failover reported RPO `unknown`; the
  recovery point of an already-promoted volume is now read from its newest SnapMirror snapshot.
- **Failover summaries show the phase of each failure again.** Rich read `[preflight]` as markup and dropped it,
  and a preflight refusal showed a `-` placeholder instead of a VM (`• - : …`). Failure lines are now escaped and
  read `• (preflight): …` or `• ns/vm (start): …`.
- **`harvey status` flags copies that need a reprotect.** A stopped copy left by a failover
  (`harvey.io/needs-reprotect`) showed as Ready although failover refuses it; it now shows `⚠ Needs reprotect`
  and status ends with a hint to run `harvey reprotect`.
- **Failures now exit non-zero.** A failover (or failback) that recorded failures, and a failed
  `health-check`, exited 0, so scripts couldn't tell. They now exit 1, and failback stops before reprotecting
  when its failover failed.
- **Naming one VM of a recovery group no longer acts on the whole group.** `failover vm`, `failback`, `protect`,
  `reprotect` and `unprotect` used to add the other members silently, so with `--yes` one VM name could stop
  and move a large group. They now refuse, list the group's VMs and standalone PVCs, and print the exact
  `--group` / `failover group` command. `failback` gained `--group`.
- **Longhorn reprotect could wait forever**: it scheduled the new primary's recurring backups first, and the
  waits required the DR volume to restore exactly Harvey's backup. A scheduled backup that landed during the
  (full) restore was restored instead, so the wait never matched and reprotect timed out half way. Waits now
  accept the requested backup or a newer one, recurring backups start after the restore is confirmed, and a
  rerun reuses the DR volumes an interrupted reprotect already created.
- `plan run --test`: Harvester and KubeVirt accept the shadow VM (no copied MACs, no `spec.running` next to
  `runStrategy`, no `harvey.io/*` labels or `dataVolumeTemplates`); `--yes` skips the confirmation
- Portworx unplanned RPO uses the migration trigger time (the PVC `migrationTime` is the apply time and
  under-reported the loss by the transfer time)
- NetApp: a failover that stopped after promoting some volumes can be re-run (`--unplanned`); reprotect retries a
  failed reestablish; quiesce/break wait for the ONTAP job and report its error
- failover refuses a destination that still holds the copy from before the last failover (run reprotect first)
- Longhorn: protect's wait for the first restore and the failover preflight require the engine's
  `lastRestoredBackup` instead of the volume's `lastBackup`

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
