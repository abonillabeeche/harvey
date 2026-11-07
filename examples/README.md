# Example: protect a VM with three disks

One VM, **app-01** in namespace `production`, with three disks:

| Disk | PVC | Size | Notes |
|---|---|---|---|
| root | `app-01-root` | 20 Gi | cloned from a CDI-backed image |
| data | `app-01-data` | 50 Gi | mounted at `/data` |
| logs | `app-01-logs` | 10 Gi | mounted at `/logs` (on NFS in the NetApp example) |

Pick the folder for your storage. Each one has the same four steps:

| File | What it is | Where |
|---|---|---|
| `setup.yaml` | StorageClasses / backup target and the image | as noted in the file |
| `vm.yaml` | namespace, cloud-init Secret, the VM | **source** cluster only |
| `config.yaml` | Harvey config (`~/.config/harvey/config.yaml`) | where you run Harvey |
| `clusterpairs.sh` | Portworx only: one-time ClusterPairs | where you run `storkctl` |
| `replication.yaml` | **reference only**: the per-volume replication objects `harvey protect` creates | don't apply |

The two clusters are called `site-a` (where app-01 runs) and `site-b` (DR). Replace the kubeconfig paths, image URL
and credentials with yours.

## 1. One-time storage setup

```bash
# NetApp: StorageClasses on both clusters, image on site-a
kubectl --kubeconfig site-a.yaml apply -f netapp/setup.yaml
kubectl --kubeconfig site-b.yaml apply -f netapp/setup.yaml     # the image object is harmless there

# Portworx: image on site-a, then one ClusterPair per direction
kubectl --kubeconfig site-a.yaml apply -f portworx/setup.yaml
./portworx/clusterpairs.sh

# Longhorn: the same BackupTarget on both clusters, image on site-a
kubectl --kubeconfig site-a.yaml apply -f longhorn/setup.yaml
kubectl --kubeconfig site-b.yaml apply -f longhorn/setup.yaml
```

## 2. Create the VM on site-a

```bash
kubectl --kubeconfig site-a.yaml apply -f <backend>/vm.yaml
```

Harvester creates the three PVCs from the VM's `volumeClaimTemplates` annotation, the same way its UI does. The
labels `failover-priority` and `failover-startup-time` are optional. They only matter when several VMs fail over
together.

## 3. Protect it

```bash
cp <backend>/config.yaml ~/.config/harvey/config.yaml
harvey protect app-01 -n production
```

Harvey finds all three disks from the VM spec. You never list volumes. Per backend, protect does this:

| Backend | Per disk | Then |
|---|---|---|
| NetApp | a `TridentMirrorRelationship` on each side and a mirrored PVC on site-b; waits for the SnapMirror baseline | copies the Secret, creates app-01 on site-b (Halted) |
| Portworx | one `MigrationSchedule` for **all three** disks together; waits for the first migration (PV + PVC appear on site-b) | the same |
| Longhorn | scheduled backups, a first backup, a standby DR volume on site-b; waits for its first restore | the same (the PVCs on site-b are created at failover) |

### What makes each volume replicate

You don't configure replication per volume. `harvey protect` reads the VM's disks and creates the storage
objects for each one. `replication.yaml` in each folder shows exactly what it creates for app-01's three disks:

| Backend | Created per disk | Created per VM |
|---|---|---|
| NetApp | a `TridentMirrorRelationship` on site-a (`promoted`) and on site-b (`established`, with the source volume handle, policy and schedule), plus the mirrored PVC on site-b. One ONTAP SnapMirror relationship per disk | — |
| Portworx | the label `harvey.io/vm=app-01` on the PVC and PV | one Stork `MigrationSchedule` that selects the three PVCs by that label and moves them together |
| Longhorn | on site-a the volume joins the backup group and points at the shared target; on site-b a standby DR volume | one `RecurringJob` (shared by all protected volumes) |

The settings that shape those objects (policy, schedule, interval, pair names, backup target, retention, replica
count) come from `config.yaml`, not from the VM. To look at them on a live system:

```bash
# NetApp
kubectl -n production get tridentmirrorrelationships                 # 3 per site, one per disk
# Portworx
kubectl -n production get migrationschedules,migrations               # site-a
kubectl -n production get pvc -l harvey.io/vm=app-01                  # the 3 PVCs it selects
# Longhorn
kubectl -n longhorn-system get recurringjobs harvey-backup-5m
kubectl -n longhorn-system get volumes.longhorn.io -l harvey.io/vm=app-01   # site-b: 3 standby DR volumes
```

`harvey unprotect app-01 -n production` removes the replication objects again.

Check it:

```console
$ harvey replication status -n production
│ production/app-01   │ app-01-root │ …backend… │ … │ yes │ 41s │ 10:15:02 │
│ site-a → site-b     │ app-01-data │           │ … │ yes │ 41s │ 10:15:02 │
│ Running / Stopped   │ app-01-logs │           │ … │ yes │ 38s │ 10:15:05 │
```

<sub>Abbreviated. Every disk gets its own row with health, lag and recovery point.</sub>

## 4. Fail over, come back

```bash
harvey failover vm app-01 -n production --dry-run     # shows the plan, changes nothing
harvey failover vm app-01 -n production               # planned: stop → final sync (all 3 disks) → promote → start on site-b
harvey reprotect app-01 -n production                 # replicate site-b → site-a
harvey failback  app-01 -n production                 # planned failover back to site-a + reprotect
```

A planned failover syncs **each** of the three disks after the VM has stopped, and doesn't start the VM unless all
three reached the stop time. The summary shows RPO 0 for the VM:

```console
  ✓ production/app-01-root: … in 21.0s
  ✓ production/app-01-data: … in 21.0s
  ✓ production/app-01-logs: … in 21.0s
  …
│ RPO production/app-01 │ 0s (lossless) │
```

If site-a is gone:

```bash
harvey failover vm app-01 -n production --unplanned   # starts the last replicated data, reports the RPO
```

With an unplanned failover the RPO is per VM, taken from its oldest disk. Portworx copies all three disks in
one migration, so they come from the same point in time. NetApp and Longhorn replicate each disk on its own,
so the disks can differ by a little.

## Variations

- **More VMs:** list them (`harvey protect app-01 app-02 db-01 -n production`), or use
  `harvey failover namespace production`, and order them with `failover-priority`.
- **Different StorageClass names on site-b (NetApp):** `replication.storage_class_map: {netapp-iscsi: dr-iscsi}`.
- **VMs that share a disk** (for example a cluster quorum disk): put them in one recovery group first,
  `harvey group add app -n production app-01 app-02`. Harvey refuses to protect or fail over one of them
  alone. See [Recovery groups](../README.md#recovery-groups-and-shared-disks).
- **A volume no VM uses yet:** add it to a group with `--pvc` and it's replicated and promoted with the group.
- **A disk Harvey can't replicate** (for example a scratch disk on another CSI driver): Harvey refuses to protect
  a VM whose disks span backends, so move that disk, or keep it out of the VM.
- **Everything you can configure:** [docs/configuration.md](../docs/configuration.md).
