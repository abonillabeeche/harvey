# Longhorn backend: DR volumes (V1 data engine; V2 is alpha)

All Harvey settings are listed in the [configuration reference](configuration.md).

> **Status.** **V1** data engine: passed a full lab cycle (protect, planned failover, reprotect, failback, unplanned
> failover, reprotect of a stale primary) between Longhorn 1.12.1 and 1.13.0. **V2** (SPDK) data engine: **alpha**.
> Longhorn supports V2 DR volumes, but they're untested with Harvey.

Longhorn doesn't replicate between clusters. Its disaster recovery is built on backups: the primary volume is
backed up to a backup target (S3 or NFS), and on the DR cluster a **DR volume** (a standby volume created from a
backup) keeps polling the same target and restores each new backup incrementally. When you need it, you
*activate* the DR volume and it becomes a normal volume.
([Longhorn docs: disaster recovery volumes](https://longhorn.io/docs/1.12.1/snapshots-and-backups/setup-disaster-recovery-volumes/))

Harvey drives all of that per VM and moves the VM metadata, as with the other backends. The backend is selected
automatically for VMs whose PVs use the `driver.longhorn.io` CSI driver.

## Expectations and requirements

Read these before protecting a VM. Each one is something Harvey relies on or changes.

### Longhorn and Harvester versions

| | Required | Why |
|---|---|---|
| Longhorn | **1.8 or later** on both clusters | multiple named BackupTargets (`BackupTarget` CRs) and DR volumes through the CRDs |
| V2 data engine | **Longhorn 1.12 or later**, V2 enabled on **both** clusters | V2 DR volumes are listed as supported from 1.12, *not* with the sharded (erasure-coded) layout |
| Harvester | any release that ships one of the above | Harvester 1.10.0-rc1 ships Longhorn 1.13.0, Harvester 1.9.0 ships 1.12.1 |

The two clusters may run different Longhorn versions, as long as the DR side can read the backups (the same or a
newer version than the source).

### One backup target that both clusters can reach

- Both clusters need a Longhorn `BackupTarget` **with the same name**, the **same URL** and working credentials.
  Harvey refuses to protect a VM if the URLs differ or a target isn't `available`.
- Both clusters must reach the object store (or NFS server) directly. Data never passes through the machine that
  runs Harvey.
- The target's `pollInterval` decides how soon the DR cluster notices a new backup. Use something short such as
  `30s`. Harvey also requests a sync before every step that waits for a backup.

**Option A: dedicated target (recommended).** Create a target named `harvey` (the default in Harvey's config) on
both clusters. Harvester's own *Backup Target* setting, which is Longhorn's `default` target used for VM backups,
stays as it is:

```yaml
apiVersion: v1
kind: Secret
metadata: {name: harvey-backup-target-secret, namespace: longhorn-system}
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "..."
  AWS_SECRET_ACCESS_KEY: "..."
  AWS_ENDPOINTS: "https://s3.example.com"     # S3-compatible endpoint (omit for AWS)
  # AWS_CERT: "<PEM>"                         # only for a private CA
---
apiVersion: longhorn.io/v1beta2
kind: BackupTarget
metadata: {name: harvey, namespace: longhorn-system}
spec:
  backupTargetURL: s3://harvey-dr@us-east-1/   # bucket@region/; any region string works for S3-compatible stores
  credentialSecret: harvey-backup-target-secret
  pollInterval: 30s
```

Apply both objects to **both** clusters.

**Option B: reuse Harvester's backup target.** Set `replication.longhorn.backup_target: default`. That works only
if Harvester's *Backup Target* setting points at the **same bucket** on both clusters. Harvester manages that
target, so changing the setting later breaks DR for every protected VM.

### What Harvey changes on the source volumes

For every PVC of a protected VM, Harvey changes the Longhorn volume on the primary cluster:

- `spec.backupTargetName` → the configured target. **All** Longhorn backups of that volume go there from now on:
  Harvey's recurring and manual backups, and any other backup created for the volume through Longhorn.
- The label `recurring-job-group.longhorn.io/harvey-dr=enabled`, and a `RecurringJob` `harvey-backup-<N>m`
  (task `backup`, cron every `interval_minutes`, `retain` backups kept).
- One manual snapshot + backup at protect time, and one at every planned failover and reprotect. Their names
  start with `harvey-` (snapshots) or carry the label `harvey.io/backup-kind` (backups). The recurring job's
  `retain` doesn't prune these, so remove them yourself.

`harvey unprotect` removes the recurring-job label. It keeps the backups and leaves `backupTargetName` as it is.

### What Harvey creates on the DR cluster

- A Longhorn volume with **the same name** as the source volume, `Standby: true`, `fromBackup` = the latest
  backup, and the same size, data engine, access mode and migratable flag as the source.
  `replication.longhorn.replicas` sets the replica count (default: the source's).
- **No PV or PVC until failover.** Longhorn forbids them on a standby volume. Harvey stores templates of the
  source PV and PVC in annotations on the DR volume and creates both when it activates the volume.
- The replica VM (Halted), as with every backend. Until failover its PVCs don't exist yet.
- The DR volume uses its full replica capacity on the DR cluster. Plan for `size × replicas` per disk.

### VM images: the V1 backing-image dependency

This is the expectation most likely to bite. A V1 root disk created from a Harvester image (the default
`backingimage` backend) references a Longhorn **backing image by name**, and Longhorn can only restore the DR
volume if a backing image **with exactly that name** exists on the DR cluster.

Harvester names backing images after the image, not after its content:

| Harvester image | Backing image name | Same name on another cluster? |
|---|---|---|
| created on this cluster (current releases) | `vmi-<VirtualMachineImage UID>` | **no**: the UID is random per cluster |
| older images, or Longhorn-level restores | `<namespace>-<image name>`, e.g. `default-image-c6n8b` | only if created with that exact name |

So these do **not** satisfy the dependency:

- importing "the same" image on the DR cluster from the same URL (new UID → new `vmi-…` name);
- restoring the image with Harvester's image restore. In the lab, restoring `default-image-c6n8b` on the DR
  cluster created the backing image `vmi-e26bc01f-…`.

What works:

1. **CDI-backed images (recommended)**: `spec.backend: cdi` on the VirtualMachineImage. The VM's disks are plain
   Longhorn volumes without a backing image, and nothing has to match.
2. **Restore the backing image under its original name** with a Longhorn `BackingImage` CR on the DR cluster
   (`sourceType: restore`, `sourceParameters.backup-url` = the backing image's backup URL). Harvey doesn't
   have to see a Harvester image for it. Longhorn only needs the BackingImage.
3. **V2 volumes** have no backing images (Longhorn uses CDI for V2), so this doesn't apply.

Harvey checks this at protect time and stops with an error that names the missing backing image.

### Harvester won't delete a PVC that a VM uses

Reprotect replaces the old primary's disks, and Harvester's admission webhook refuses to delete a PVC that any VM
references, even a stopped one. So during reprotect Harvey deletes the **stopped** replica VM object on the old
primary (orphan propagation, nothing else goes with it), deletes the CDI DataVolume that owns the root PVC (if
any), then the PVCs. Harvey recreates the VM from the new primary right after. It refuses if that VM is running.

### A new volume's BackupVolume can sync after its Backup

Longhorn's volume mutator reads the synced Backup CR's `status.backupTargetName` and then the BackupVolume CR on
the DR cluster. The DR cluster's backup controller fills that status in a little after the Backup CR appears, and
for a brand-new volume the BackupVolume can arrive later still. Without both, creating the DR volume fails with
`backup target name and volume name cannot be empty`. Harvey waits for them first.

### Synced backups report an unknown block size

A Backup CR that a cluster learns about through the backup target sync has `spec.backupBlockSize: -1`. This was
seen both ways between Longhorn 1.12.1 and 1.13.0. Longhorn's volume mutator copies that value into a new DR
volume, and the CRD schema then rejects it (only 2 MiB and 16 MiB are valid). Harvey sets the source backup's real
block size on the synced Backup CR before it creates the DR volume, and logs that it did.

### Standalone PVCs in a recovery group need detached recurring backups

Longhorn's recurring jobs skip **detached** volumes by default. A standalone PVC in a recovery group that nothing
mounts therefore only gets new backups at protect, planned failover and reprotect, and its lag keeps growing in
`replication status`. Turn on the Longhorn setting `allow-recurring-job-while-volume-detached` (global) if those
volumes change outside of Harvey operations.

### Harvey trusts the engine, not the volume, for "restored"

A DR volume's `status.lastBackup` names the newest backup it should restore. Only the engine's
`status.lastRestoredBackup` proves that the data was restored. Harvey's final sync and `replication status`
require the engine. With the V2 engine in the lab, `lastBackup` moved on while the engine never restored
anything (see the V2 section below), and the volume shows `restore not confirmed` instead of `up to date`.

When Harvey waits for a specific backup (final sync, reprotect), it accepts that backup **or a newer one** of the
same volume, ordered by snapshot time. A DR volume always follows the newest backup on the target, so a scheduled
backup that lands during the wait is restored instead of Harvey's, and an exact-match check would wait forever.

### Why reprotect replaces the old copy

Longhorn can create a DR volume only from a backup. It can't turn an existing volume into a DR volume or apply
just the changes in reverse, so reprotect deletes the old primary's stale copy and restores the new primary's
data in full. Nothing is lost by deleting it: every backup of the old primary, up to the final failover backup,
stays on the backup target. Only the new primary's *upload* is incremental (same backup volume, changed blocks
only); the *download* onto the old primary is a full restore. Plan time and bandwidth for it.

### Storage classes

The PV is recreated with the source's `storageClassName`. Because Harvey creates a static PV, that class doesn't
have to exist on the DR cluster. Keep the same class names on both sites anyway, so later volume operations
(expansion, clones) behave the same.

## Flow

| Step | What happens |
|---|---|
| **protect** | point the source volumes at the target, schedule backups, take a first backup, wait until the DR cluster sees it, create the DR volume (standby) and wait for the first restore, copy the metadata |
| **replication status** | per volume: backup schedule on the source, `standby` on DR, the last restored backup and its snapshot time (the recovery point), lag |
| **planned failover** | stop the VM → snapshot + backup of each volume (Longhorn attaches a stopped volume for this) → wait until each DR volume has restored that exact backup → activate → create the PV and PVC → start. RPO 0 |
| **unplanned failover** | activate with whatever the DR volume restored last. The RPO is the age of that backup's snapshot, and the DR cluster knows it without the source |
| **fencing** | an activated DR volume stops restoring, so a source that comes back can't overwrite it. When the source is reachable, Harvey also stops its scheduled backups |
| **reprotect** | back up the new primary's volumes to the target, delete the old primary's PVC, PV and volume, recreate it as a DR volume of the new primary (a **full** restore), wait for the restore, and only then schedule the new primary's recurring backups. A reprotect that stopped half way reuses the DR volumes it already created |

## Measured in the lab

retail-store (Harvester 1.9.0, Longhorn 1.12.1) ↔ disaster-recovery (Harvester 1.10.0-rc1, Longhorn 1.13.0),
V1 data engine. Backup target: a local Ceph RGW (S3) that both clusters reach on the LAN, 5-minute backup
interval. The VM was Leap 16 from a CDI image, with a 5 GiB RWX block root disk and a 1 GiB data disk. In-guest
timestamps were written to both disks every 5 seconds.

| Step | Result |
|---|---|
| protect | first backups 1.47 GB (root) and 65 MB (data), DR volumes restored |
| planned failover | **RTO 93 s** (stop 9, final backup + restore 52, activate + PV/PVC 12, start 18), **RPO 0** in the guest. Final backups were incremental: 13 MB + 33 KB |
| reprotect | full restore of both disks on the old primary, replica VM recreated |
| failback (planned failover back + reprotect) | **RTO 93 s, RPO 0**. Guest history across both moves: only the two outage gaps |
| unplanned failover (source unreachable) | **31 s** to Running. Reported RPO ≤ 180 s; real loss 151–156 s. The two disks were a few seconds apart |
| reprotect after the unplanned failover | the stale primary was demoted, disks replaced, replication running again |
| recovery group: 2 VMs + 1 standalone PVC | protect, planned failover of the group (RTO 151 s, RPO 0), reprotect, failback (RTO 144 s, RPO 0) |

## V2 data engine: lab findings (alpha)

far-edge (Longhorn 1.12.1, V2 on NVMe) → harvester-ai (Longhorn 1.12.1, V2 on a 100 GiB LV, two NUMA nodes):

- **protect works**: V2 backups to S3, V2 standby DR volumes created on the other cluster.
- **Restores don't complete on harvester-ai.** A V2 restore exposes the volume through a temporary NVMe-TCP
  target and attaches it with the host's NVMe initiator. On harvester-ai the kernel's NVMe work queue hung in
  `nvme_scan_work`: controllers stayed in `deleting`, no namespace device appeared, and every restore attempt
  failed ("after restore flow failure"). So the DR volume could never be activated. Clearing it needs a reboot.
  This is a host/kernel issue to investigate (native NVMe multipath is on; NVMe-TCP over loopback to SPDK on the
  same node). It isn't a Harvey issue, but it's why V2 stays alpha.
- **Getting V2 to start at all** on that host needed two things. First, the leftover kernel arguments
  `default_hugepagesz=1G hugepages=128` had to go: DPDK failed with "EAL: Cannot init memory". Second, enough
  2 MiB hugepages per NUMA node: Harvester's `longhorn-v2-data-engine-memory-size` is split across sockets,
  while SPDK's memory size has to fit on one.

## RPO and RTO

- **Unplanned RPO** ≈ the backup interval + the time a backup takes + the time the DR cluster needs to notice
  and restore it. A 5-minute interval typically means 5–10 minutes.
- **Planned failover** is lossless, but its RTO includes one more backup and one incremental restore per disk.
  Both are proportional to how much changed since the last scheduled backup.
- **Reprotect** restores the whole volume, so plan time and bandwidth for a full copy.

## Limits

- Backup based: no continuous replication. Every recovery point is a backup.
- One VM's disks are backed up one at a time. After an unplanned failover they can come from slightly different
  points in time.
- The sharded (erasure-coded) V2 layout supports neither backups nor DR volumes.
- Backups that Harvey takes manually aren't pruned automatically.
