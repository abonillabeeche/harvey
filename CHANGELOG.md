# Changelog

All notable changes to Harvey are documented here.

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
