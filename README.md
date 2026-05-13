# Harvey — DR Orchestration CLI for KubeVirt / SUSE Virtualization

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/abonillabeeche/harvey)](https://github.com/abonillabeeche/harvey/releases)

Harvey automates Disaster Recovery across two Rancher-managed KubeVirt (SUSE Virtualization / Harvester) clusters.

---

## What Harvey does

| Capability |
|---|
| Compare VM state across both sites |
| Ordered stop → verify → start failover |
| Priority groups with startup delays |
| Recovery Plans (YAML runbooks) |
| Pre / post shell hooks |
| **Non-disruptive DR test** (shadow namespaces) |
| RTO measurement |
| Audit log (JSONL) |
| Split-brain detection |
| Health-check pre-flight |
| Reprotect guidance |

---

## Installation

### Recommended: pipx (no manual venv required)

[pipx](https://pipx.pypa.io) installs CLI tools in isolated environments invisibly — you just get a `harvey` command.

```bash
# Install pipx if you don't have it
pip install --user pipx
pipx ensurepath

# Install Harvey from the latest GitHub release
pipx install "https://github.com/abonillabeeche/harvey/releases/latest/download/harvey-0.1.0-py3-none-any.whl"
```

After installation, run `harvey --help` — no virtual environment activation needed.

### Alternative: pip

```bash
pip install "https://github.com/abonillabeeche/harvey/releases/latest/download/harvey-0.1.0-py3-none-any.whl"
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
harvey config init
# then edit ~/.config/harvey/config.yaml
```

```yaml
rancher_url: "https://rancher.mydomain.com"
api_token:   "token-abcde:1234567890abcdefghijklmnopqrstuvwxyz"
clusters:
  primary: "c-m-xxxxxxxx"   # Rancher cluster ID for primary site
  dr:      "c-m-yyyyyyyy"   # Rancher cluster ID for DR site
```

Show config (token masked):
```bash
harvey config show
```

---

## Commands

### status — Compare VM state across sites

```bash
harvey status primary dr
harvey status primary dr --namespace production
```

Failover State column:

| State | Meaning |
|---|---|
| ✅ Ready | One site running, other stopped — safe to fail over |
| ✅ Synced | Same state on both sides (both stopped) |
| ❌ SPLIT BRAIN | Running on **both** clusters — requires immediate manual action |
| Standalone | VM exists on only one cluster |

---

### health-check — Pre-flight DR readiness

```bash
harvey health-check primary dr
```

Checks:
- API connectivity to both clusters
- VM inventory coverage (VMs present on both sides)
- Split-brain detection
- Saved recovery plans

---

### failover — Ad-hoc failover operations

All failover commands share these flags:

| Flag | Meaning |
|---|---|
| `--dry-run` | Print the ordered plan without touching any VMs |
| `--yes / -y` | Skip confirmation prompt |
| `--source` | Cluster where VMs are currently running |
| `--destination` | Cluster to start VMs on |

```bash
# Specific VMs
harvey failover vm db-primary app-server-1 \
    --namespace production --source primary --destination dr

# All VMs in a namespace
harvey failover namespace production \
    --source primary --destination dr

# Everything
harvey failover all --source primary --destination dr --dry-run
```

#### Ordering labels (set on VirtualMachine objects in Harvester)

```yaml
metadata:
  labels:
    failover-priority:    "100"   # higher = stopped first, started last
    failover-startup-time: "60"   # seconds to pause after issuing 'start'
```

Stop order: **descending** priority (100 → 50 → 0)  
Start order: **ascending** priority (0 → 50 → 100) — foundations before dependents

---

### plan — Recovery Plans (YAML runbooks)

Recovery plans encode your full DR runbook — protection groups, VM ordering, startup delays, and shell hooks — so the same procedure runs identically every time.

#### Create a plan

```bash
harvey plan new production-dr
# edit ~/.config/harvey/plans/production-dr.yaml
```

Plan file structure (`~/.config/harvey/plans/<name>.yaml`):

```yaml
name: production-dr
description: "Full production site failover"
source:      primary
destination: dr

protection_groups:
  - name: database-tier
    priority: 100          # stopped first, started last
    startup_delay: 60      # seconds to wait after 'start' before next group
    vms:
      - namespace: production
        name: db-primary
      - namespace: production
        name: db-replica

  - name: app-tier
    priority: 50
    startup_delay: 30
    vms:
      - namespace: production
        name: app-server-1

  - name: web-tier
    priority: 10
    startup_delay: 0
    vms:
      - namespace: production
        name: web-proxy

hooks:
  pre_failover:
    - name: "Page on-call"
      command: "curl -sf -X POST -d 'DR starting' https://hooks.slack.com/..."
      timeout: 30
      on_failure: warn      # warn | abort

  pre_start:
    - name: "Drain load balancer"
      command: "./scripts/lb-drain.sh --site primary"

  post_failover:
    - name: "Update DNS"
      command: "./scripts/update-dns.sh --target dr"
      timeout: 60
```

Hook environment variables available in your scripts:

| Variable | Value |
|---|---|
| `HARVEY_SOURCE` | Source cluster name |
| `HARVEY_DESTINATION` | Destination cluster name |

#### List / inspect plans

```bash
harvey plan list
harvey plan show production-dr
```

#### Run a plan — dry run

```bash
harvey plan run production-dr --dry-run
```

#### Run a plan — non-disruptive DR test ⭐

```bash
harvey plan run production-dr --test
```

The test runs in three phases:

1. **Clone** — copies VM specs into shadow namespaces (`*-harvey-test`) on the DR cluster
2. **Verify** — starts each shadow VM and polls for Running state (5-minute timeout per VM)
3. **Cleanup** — deletes all shadow namespaces regardless of test outcome

Production VMs on both clusters are **never touched**.

> **Storage note:** Shadow VMs retain their original PVC/DataVolume references.
> If those volumes don't exist on the DR cluster the VM will time out — which is
> exactly the kind of readiness gap this test is designed to surface before a
> real DR event.

#### Run a plan — live failover

```bash
harvey plan run production-dr
harvey plan run production-dr --yes   # skip confirmation
```

---

### reprotect — Reverse protection after failover

After a successful failover the DR cluster is your new production site. `reprotect` shows what is running there and prints the exact command to fail back once the original site recovers.

```bash
harvey reprotect --source primary --destination dr
```

---

### audit — Operation history

```bash
harvey audit
harvey audit --limit 100
```

All Harvey operations are appended to `~/.config/harvey/audit.log` (newline-delimited JSON).

---

## Debug mode

Add `--debug` before any command for verbose API request/response output:

```bash
harvey --debug status primary dr
harvey --debug plan run production-dr --dry-run
```

---

## Typical DR workflow

```
1. Day-to-day verification
   harvey health-check primary dr         # automated / cron

2. Pre-event readiness test (non-disruptive)
   harvey plan run production-dr --test   # runs weekly / monthly

3. DR event declared
   harvey status primary dr               # confirm split-brain / ready state
   harvey plan run production-dr --dry-run
   harvey plan run production-dr          # live failover with RTO tracking

4. After DR site stabilises
   harvey reprotect --source primary --destination dr

5. Original site recovers — fail back
   harvey failover all --source dr --destination primary
```

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
| 0.1 | Basic stop/start failover, namespace/all commands, priority labels, startup delays, dry-run, recovery plans, DR test mode, hooks, RTO tracking, audit log, health-check, reprotect |
