# Configuration reference

Everything you can set for Harvey, in one place: the config file, environment variables, labels on your VMs,
recovery plans, and what Harvey writes into your clusters.

## Where the config lives

| | |
|---|---|
| Config file | `~/.config/harvey/config.yaml` |
| Recovery plans | `~/.config/harvey/plans/<name>.yaml` |
| Audit log | `~/.config/harvey/audit.log` (JSON lines) |
| Override the directory | `HARVEY_CONFIG_DIR=/path/to/dir` (all three move there) |

`harvey config init` writes a commented template, and `harvey config show` prints the active config with
passwords masked. Keeping one directory per environment and switching with `HARVEY_CONFIG_DIR` works well
(for example `prod-dr/` and `lab/`).

## Complete example

```yaml
# Rancher: only needed for clusters reached through the Rancher proxy (rancher_cluster_id)
rancher_url: "https://rancher.example.com"
api_token:   "token-abcde:..."

clusters:
  on-prem:
    kubeconfig: ~/.kube/on-prem.yaml        # token or client-certificate auth
    context: on-prem-admin                  # optional; default = current-context
  cloud:
    rancher_cluster_id: c-m-xxxxxxxx        # through the Rancher proxy
    ontap:                                  # optional NetApp override (see below)
      host: 192.0.2.10
      username: vsadmin
      password_env: CLOUD_ONTAP_PASSWORD

defaults:
  source: on-prem
  destination: cloud

replication:
  backend: null                             # netapp | portworx | longhorn; null = per VM from the CSI driver
  max_lag_seconds: 900
  storage_class_map: {}
  timeouts: {stop: 300, start: 600, tmr: 300, transfer: 1800, stall: 120}

  netapp:
    policy: MirrorAllSnapshots
    schedule: 5min

  portworx:
    interval_minutes: 15
    pairs:
      on-prem->cloud: harvey-to-cloud
      cloud->on-prem: harvey-to-onprem

  longhorn:
    backup_target: harvey
    interval_minutes: 5
    retain: 12
    replicas: null
```

## `clusters`

Each entry names a cluster. That name is what you use on the command line (`-s on-prem -d cloud`), in plans and in
`replication.portworx.pairs`. It's also what Harvey writes into the `harvey.io/peer-cluster` annotations, so keep it
stable.

| Key | Required | Default | Meaning |
|---|---|---|---|
| `kubeconfig` | one of these two | | path to a kubeconfig. Token and client-certificate auth are supported; exec/auth plugins are not |
| `rancher_cluster_id` | one of these two | | cluster ID in Rancher (`c-m-…`); needs `rancher_url` + `api_token` at the top level |
| `context` | no | the kubeconfig's `current-context` | context to use from `kubeconfig` |
| `ontap.host` | no | discovered from Trident | ONTAP management LIF (IP or name) of this cluster's backend |
| `ontap.username` | with `ontap` | | ONTAP login (an SVM-scoped `vsadmin` is enough) |
| `ontap.password` / `ontap.password_env` | with `ontap` | | the password, or the name of an environment variable that holds it (preferred) |

A bare string still works for old configs: `on-prem: c-m-xxxxxxxx` means `rancher_cluster_id`.

The identity in the kubeconfig or Rancher token needs to read and write VirtualMachines, PVCs, PVs, Secrets,
ConfigMaps, NetworkAttachmentDefinitions and namespaces, plus the storage CRDs of your backend. A cluster-admin
binding is the simple choice for a DR tool.

## `defaults`

| Key | Meaning |
|---|---|
| `source` | cluster used when `--source/-s` is omitted |
| `destination` | cluster used when `--destination/-d` is omitted |

Most commands find each VM's current direction from the role labels, so after a failover you don't have to swap
these.

## `replication` (all backends)

| Key | Default | Meaning |
|---|---|---|
| `backend` | `null` | force one backend for every VM. Leave unset: Harvey picks per VM from the CSI driver of its PVs and records the choice on the VM |
| `max_lag_seconds` | `900` | `health-check` fails a VM whose recovery point is older than this. Use the replication interval plus transfer time, with some headroom |
| `storage_class_map` | `{}` | `source-class: dr-class` for StorageClass names that differ between sites (used where Harvey creates the DR PVC: NetApp) |
| `timeouts.stop` | `300` | seconds to wait for a VM to stop |
| `timeouts.start` | `600` | seconds to wait for a VM to reach Running |
| `timeouts.tmr` | `300` | seconds for short storage state changes (TMR states, Stork schedule changes, Longhorn activation and snapshots) |
| `timeouts.transfer` | `1800` | seconds for data movement: baselines, final syncs, migrations, backups and restores. Raise it for large disks or slow links |
| `timeouts.stall` | `120` | NetApp: seconds without transfer progress before Harvey aborts and retries a transfer |

### `replication.netapp`

| Key | Default | Meaning |
|---|---|---|
| `policy` | `MirrorAllSnapshots` | asynchronous SnapMirror policy on the destination ONTAP cluster |
| `schedule` | `5min` | ONTAP job schedule on the destination; sets the unplanned RPO. 5 minutes is NetApp's supported minimum |

0.2 configs with `policy` and `schedule` directly under `replication:` still work. ONTAP endpoints and credentials
are discovered from Trident unless a cluster has an `ontap:` block. See [netapp.md](netapp.md).

### `replication.portworx`

| Key | Default | Meaning |
|---|---|---|
| `interval_minutes` | `15` | migration interval. Harvey creates the Stork SchedulePolicy `harvey-<N>min` |
| `pairs` | `harvey-<destination>` in the VM's namespace | Stork ClusterPair per direction, keyed `source->destination`. Value `name` (in the VM's namespace) or `namespace/name` (that namespace must be Stork's admin namespace, the same in both directions) |

The ClusterPairs themselves are created once with `storkctl`. See [portworx.md](portworx.md).

### `replication.longhorn`

| Key | Default | Meaning |
|---|---|---|
| `backup_target` | `harvey` | name of the Longhorn BackupTarget that exists, with the same URL, on both clusters. `default` reuses Harvester's backup target |
| `interval_minutes` | `5` | recurring backup cron. Harvey creates the RecurringJob `harvey-backup-<N>m` |
| `retain` | `12` | backups the recurring job keeps per volume |
| `replicas` | the source volume's | replica count of the DR volumes |

See [longhorn.md](longhorn.md) for the BackupTarget setup and what Harvey changes on the source volumes.

## Environment variables

| Variable | Used for |
|---|---|
| `HARVEY_CONFIG_DIR` | config, plans and audit log directory |
| any name in `ontap.password_env` | ONTAP password for that cluster |
| `HARVEY_SOURCE`, `HARVEY_DESTINATION` | set **by** Harvey for recovery-plan hooks |

Global flag: `harvey --debug <command>` prints every API request.

## Labels you set on VMs

| Label | Default | Meaning |
|---|---|---|
| `failover-priority` | `0` | higher = stopped last, started first |
| `failover-startup-time` | `0` | seconds to wait after starting this VM before starting the next one |

Inside a recovery plan, the group's `priority` and `startup_delay` take precedence over these labels.

## Recovery plans

`~/.config/harvey/plans/<name>.yaml`. Create one with `harvey plan new <name>`, or save one from
`harvey failover … --dry-run`.

| Key | Meaning |
|---|---|
| `name`, `description` | shown by `plan list` / `plan show` |
| `source`, `destination` | clusters (`--reverse` swaps them) |
| `protection_groups[].name` / `.description` | group label |
| `protection_groups[].priority` | higher = stopped last, started first |
| `protection_groups[].startup_delay` | seconds to wait after starting each VM in the group |
| `protection_groups[].vms[]` | `{namespace, name}` of each VM |
| `hooks.pre_failover[]` | runs before anything changes. `on_failure: abort` stops the failover |
| `hooks.pre_start[]` | runs after the volumes are promoted, before VMs start |
| `hooks.post_failover[]` | runs at the end |
| hook `name`, `command` | shell command; `HARVEY_SOURCE` / `HARVEY_DESTINATION` are set |
| hook `timeout` | seconds, default `60` |
| hook `on_failure` | `warn` (default) or `abort` |

## What Harvey writes into your clusters

So that you can find, audit or clean up everything Harvey manages:

| Where | Key | Meaning |
|---|---|---|
| everything Harvey creates or owns | label `harvey.io/managed=true` | selector for Harvey's objects |
| VMs, Secrets, ConfigMaps, PVCs | label `harvey.io/vm=<vm>` | the VM the object belongs to (Portworx also uses it as the Stork selector) |
| VMs | label `harvey.io/role` | `primary` or `replica` |
| VMs | annotations `harvey.io/peer-cluster`, `harvey.io/source-cluster` | the other site |
| VMs | annotation `harvey.io/storage-backend` | backend that replicates the disks |
| VMs | annotation `harvey.io/run-strategy` | runStrategy to restore when the VM is started |
| VMs | annotation `harvey.io/primary-since` | when this site became primary (resolves stale primaries) |
| VMs | annotation `harvey.io/needs-reprotect` | set on the old primary after a failover |
| VMs | annotation `harvey.io/synced-at` | last metadata sync |
| Portworx | `SchedulePolicy harvey-<N>min`, `MigrationSchedule harvey-<vm>`, `Migration harvey-<vm>-final-*` | see [portworx.md](portworx.md) |
| Portworx PVCs | annotation `harvey.io/promoted-at` | failover time |
| NetApp | `TridentMirrorRelationship <pvc>` on both sides | see [netapp.md](netapp.md) |
| Longhorn | `RecurringJob harvey-backup-<N>m`, label `recurring-job-group.longhorn.io/harvey-dr`, snapshots `harvey-*`, backups labelled `harvey.io/backup-kind` | see [longhorn.md](longhorn.md) |
| Longhorn DR volumes | annotations `harvey.io/pv-template`, `harvey.io/pvc-template`; label `harvey.io/vm-claim` | used to create the PV/PVC at failover |
