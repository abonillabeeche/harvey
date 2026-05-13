# Changelog

All notable changes to Harvey are documented here.

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
