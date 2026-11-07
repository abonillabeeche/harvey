# Portworx async DR backend

Harvey's `portworx` backend replicates VM disks with Portworx async DR (Stork). Portworx moves the volume
data and the PV/PVC objects. Harvey moves the VM spec, Secrets, ConfigMaps and NADs, and orchestrates the
failover, the fencing and the failback.

The backend is selected automatically for VMs whose PVs use the `pxd.portworx.com` CSI driver.

## Requirements

- Portworx Enterprise with the **PX-DR** feature on both clusters (the Trial and Extended Eval licences have it).
  Both clusters run the same Portworx version and Stork 24.2 or later.
- An S3-compatible object store that **both** clusters can reach. Portworx async DR moves volume data through it.
- From each source cluster:
  - the destination's Kubernetes API (a Rancher-proxied kubeconfig works)
  - the destination's Portworx API (`portworx-api`, port 9001) through `--dest-ep host:port`
- The same StorageClass names on both clusters. Stork keeps the PVC's `storageClassName`.

## One-time setup per direction

Create the ClusterPair in the VM namespace. Name it to match `replication.portworx.pairs` in the Harvey config.

```bash
storkctl create clusterpair harvey-to-dr -n <vm-namespace> \
  --src-kube-file site-a.yaml --dest-kube-file site-b.yaml \
  --dest-ep <site-b-px-endpoint>:9001 \
  --provider s3 --s3-endpoint <host:port> --s3-access-key ... --s3-secret-key ... --s3-region us-east-1 \
  [--disable-ssl] --unidirectional
kubectl --kubeconfig site-a.yaml -n <vm-namespace> get clusterpair harvey-to-dr   # both statuses Ready
```

Do the same in the other direction (`harvey-to-<site-a>`, created on site-b) so that reprotect and failback work.
If a pair lives outside the VM namespace (`ns/name` in the config), that namespace must be Stork's admin
namespace, and both directions must use the same namespace.

## What Harvey creates

| Object | Where | Purpose |
|---|---|---|
| label `harvey.io/vm=<vm>` | the VM's PVCs and PVs (source) | Stork selector |
| `SchedulePolicy harvey-<N>min` | both clusters | migration interval (`interval_minutes`, default 15) |
| `MigrationSchedule harvey-<vm>` | source, pair namespace | volume-only migration (`includeResources: false`), `autoSuspend: true` |
| `Migration harvey-<vm>-final-*` | source | final sync of a planned failover |
| annotation `harvey.io/promoted-at` | destination PVCs | marks the failover |

Stork copies each MigrationSchedule to the destination as a suspended "static copy". On failover, Harvey sets
`status.applicationActivated=true` on that copy. This is the same flag `storkctl perform failover` sets. When the
old source comes back, `autoSuspend` makes it suspend its schedule instead of migrating over the promoted
volumes.

## Flow

- **protect**: label the volumes, create the schedule, wait for the first (baseline) migration. The replica
  PVCs now exist on the destination. Then copy the metadata and create the replica VM (Halted).
- **failover (planned)**: stop the VM, suspend the schedule, wait for a running migration to finish, run one
  more `Migration` (RPO 0), fence, start the VM on the destination.
- **failover --unplanned**: no final sync. The RPO comes from the last successful migration (source reachable)
  or from the destination PVC annotation `stork.libopenstorage.org/migrationTime` (source down). The second
  value under-estimates the loss by that migration's transfer time.
- **reprotect**: delete the old direction's schedule on both sides, create `harvey-<vm>` on the new primary with
  the reverse pair, wait for the first reverse migration. That migration overwrites the old primary's disks.
- **unprotect --delete-replica**: restores the PV reclaim policy that Stork saved, so the Portworx volume is
  deleted together with the replica PVC.

## Limits

- The recovery point is per migration (all of the VM's volumes in one Stork migration), not crash-consistent
  across VMs.
- Portworx recommends a migration interval of 15 minutes or more. A planned failover still has RPO 0 because
  of the final migration.
