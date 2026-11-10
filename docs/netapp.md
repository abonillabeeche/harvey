# NetApp backend (ONTAP SnapMirror through Trident)

All Harvey settings are listed in the [configuration reference](configuration.md).

Harvey's `netapp` backend replicates VM disks with **ONTAP SnapMirror** and drives it through Trident's
`TridentMirrorRelationship` (TMR) objects. NetApp moves the disk data. Harvey moves the VM spec, Secrets,
ConfigMaps and NADs, and orchestrates the failover, the final sync, the promote and the failback.

The backend is selected automatically for VMs whose PVs use the `csi.trident.netapp.io` CSI driver.

## What Harvey expects from the NetApp side

Harvey isn't picky about how the arrays are laid out. Aggregates, volume names, QoS, export policies, LIF layout
and protocols are whatever Trident already uses. What it needs is **SnapMirror working through Trident**, plus
three small things of its own. A storage admin can go through this as a checklist.

### NetApp's own prerequisites for Trident replication

These come from NetApp's
[Trident volume replication docs](https://docs.netapp.com/us-en/trident/trident-use/vol-volume-replicate.html).
Harvey adds nothing here:

- [ ] **Trident 22.10 or later** on both Kubernetes clusters, each with an ONTAP backend (tested: Trident 26.06.1,
  ONTAP 9.16.1).
- [ ] **SnapMirror asynchronous license** on both ONTAP clusters. It's part of ONTAP One and the Data Protection
  bundle. Only asynchronous SnapMirror is supported.
- [ ] **Cluster peering and SVM peering** between the two ONTAP clusters, with SVM peering for SnapMirror
  (`vserver peer create ... -applications snapmirror`).
- [ ] **Unique SVM names** on the two sides. NetApp lists this as a requirement.
- [ ] **On the DR cluster, Trident's backend is the peered SVM.** The SVM that receives the mirrors must be the
  one Trident manages there.
- [ ] **Supported drivers:** `ontap-nas` (NFS) and `ontap-san` (iSCSI, FC, NVMe/TCP; NVMe needs ONTAP 9.15.1 or
  later). **Not supported:** `ontap-nas-economy`, `ontap-san-economy`, `ontap-nas-flexgroup`, and ASA r2 systems.
- [ ] **A SnapMirror policy and an ONTAP job schedule** that exist on the destination cluster. The built-in
  `MirrorAllSnapshots` policy and `5min` schedule are fine (5 minutes is NetApp's supported minimum).
- [ ] *Recommended by NetApp:* the source StorageClass has the parameter `trident.netapp.io/replication: "true"`.
  Trident 26.06 replicated without it in the lab, so Harvey doesn't require it. Set it anyway to stay inside
  NetApp's documented configuration.

### What Harvey itself needs

- [ ] **The account Trident already uses is enough.** Harvey doesn't need cluster admin or an account of its own.
  It reuses the credentials of the Trident backend that owns each PVC. In the lab that was an **SVM-scoped** login
  with the `vsadmin` role, on the SVM management LIF. Harvey calls these ONTAP REST APIs on the destination SVM:

  | Call | Why |
  |---|---|
  | `GET /api/snapmirror/relationships` | state, health, lag, recovery point |
  | `POST /api/snapmirror/relationships/{uuid}/transfers` | final update before a planned failover |
  | `PATCH .../transfers/{uuid}` (`state: aborted`) | abort a stalled or conflicting transfer |
  | `PATCH /api/snapmirror/relationships/{uuid}` (`paused`, `broken_off`, `snapmirrored`) | quiesce + break when Trident left a volume DP, restart a stalled baseline |
  | `GET /api/storage/volumes` | check that a promoted volume really is read-write |
  | `GET /api/cluster/jobs/{uuid}` | wait for the job of a quiesce or break and report its error |
  | `GET /api/storage/volumes/{uuid}/snapshots` | recovery point of a volume an interrupted failover already promoted |

  The login needs the `http` application (ONTAP REST) enabled, which Trident's REST mode already requires.
- [ ] **Backends defined as `TridentBackendConfig` objects with a username/password Secret.** Harvey finds a PVC's
  ONTAP endpoint and credentials by following PV → TridentVolume → TridentBackendConfig → `managementLIF` + Secret.
  If a backend was created with `tridentctl` (no TridentBackendConfig), or uses certificate authentication or
  AWS Secrets Manager credentials, set the endpoint per cluster in Harvey's config instead:

  ```yaml
  clusters:
    dr:
      kubeconfig: ~/.kube/dr.yaml
      ontap: {host: 192.0.2.10, username: vsadmin, password_env: DR_ONTAP_PW}
  ```

- [ ] **Network access from wherever Harvey runs** to the ONTAP management LIF of **both** sites over HTTPS
  (TCP 443). The DR side matters most: status, final sync and promote all talk to the destination. Harvey accepts
  ONTAP's self-signed certificate.
- [ ] Optional: map StorageClass names that differ between sites with `replication.storage_class_map`. One VM can
  mix NFS and iSCSI disks.

### Not required

- Harvey doesn't create or change SVMs, peers, policies, schedules or export policies. Everything it does goes
  through Trident's `TridentMirrorRelationship` objects and the REST calls above.
- No ONTAP software beyond SnapMirror. No SnapCenter, no BlueXP, no Trident Protect.
- No specific aggregate or volume layout. VMs created from Harvester images work: their root disk is a FlexClone
  of the image volume, and SnapMirror sends it as a full, independent copy.

## Configuration

```yaml
replication:
  netapp:
    policy: MirrorAllSnapshots     # an async SnapMirror policy (Trident supports async only)
    schedule: 5min                 # ONTAP job schedule on the destination
  storage_class_map: {}            # source SC -> destination SC when the names differ
  max_lag_seconds: 900             # health-check threshold (schedule + transfer time)
  timeouts: {tmr: 300, transfer: 1800, stall: 120}
```

0.2 configs that keep `policy` and `schedule` directly under `replication:` still work.

## How it works

Per PVC of a protected VM:

| Where | Object | State |
|---|---|---|
| source cluster | `TridentMirrorRelationship <pvc>` | `promoted`. Exposes the ONTAP volume handle `svm:volume` |
| destination cluster | `TridentMirrorRelationship <pvc>` | `established`, `remoteVolumeHandle` = the source handle, policy + schedule |
| destination cluster | `PersistentVolumeClaim <pvc>` | annotated `trident.netapp.io/mirrorRelationship: <pvc>`. Trident provisions a DP volume on the peered SVM |

### protect

1. Creates a source TMR `promoted` per PVC and waits for its volume handle.
2. Creates a destination TMR `established` and the annotated PVC per disk, then waits for the SnapMirror baseline.
   A baseline that stops making progress (seen on memory-starved simulators) is aborted and restarted from its
   checkpoint, up to 3 times.
3. Copies Secrets, ConfigMaps and NADs and creates the replica VM `Halted`, with the same name, namespace, MAC
   addresses and disk names. PVC templates and image references are dropped on the replica, because its disks
   are the mirrored PVCs.

VMs created from Harvester images work. Their root disk is a FlexClone of the image volume, and SnapMirror sends
it as a full, independent copy (731 MB for a Leap 16 root disk in the lab).

### Planned failover

Metadata sync → stop the VM → **final SnapMirror update** per volume → promote → start.

- The final update waits for a scheduled transfer that's already running, aborts a transfer that stalls for
  `timeouts.stall` seconds, and retries up to 3 times.
- Before it promotes, Harvey checks that the replicated snapshot is newer than the time the VM stopped. A VM is
  promoted only with RPO 0.

### Promote, and the Trident quirk

With Trident 26.06.1 and ONTAP 9.16.1, a TMR can report `promoted` while the volume is still **DP**
(read-only), so the VM can't attach it. This happens in two cases:

- **A scheduled transfer is running.** Trident's break fails with "snapmirror transfer already in progress",
  is requeued, and the TMR still ends up `promoted`.
- **The relationship is already quiesced.** Trident marks it promoted without breaking it.

Harvey therefore aborts a running transfer before it promotes, lets Trident promote, checks the volume type over
the ONTAP REST API, and quiesces and breaks the relationship itself if the volume is still DP. NetApp has been told
about the behavior.

### Reprotect and failback

`harvey reprotect` sets the old source's TMR to `reestablished`, with `remoteVolumeHandle` = the new primary's
volume. That is a **reverse resync**: only changed blocks travel, so failback stays cheap even for large disks.
`harvey failback` = planned failover back + reprotect.

If a reestablish fails (Trident reports the TMR `failed`, e.g. while the peering is down), Harvey stops waiting at
once, retries that volume once, and then reports each failed disk. ONTAP's own reason is only in Trident's log
(`kubectl -n trident logs deploy/trident-controller -c trident-main | grep 'Could not reestablish'`). Once the
cause is fixed, `harvey reprotect` again retries the failed volumes. With several VMs, a failed VM doesn't stop the
others; the command exits 1 at the end.

### Replication status

`harvey replication status` reads the relationships from the destination ONTAP:

- **State:** the TMR state on each side (`promoted → established`).
- **Replication:** the SnapMirror state (`snapmirrored`) and its health.
- **Lag:** ONTAP's `lag_time`, which is the worst-case data loss for an unplanned failover.
- **Recovery point:** the timestamp of the newest exported snapshot.

## Measured in the lab

Two SUSE Virtualization clusters, each with Trident 26.06.1 against its own single-node ONTAP 9.16.1 simulator.
The test VM was Leap 16 with an image-cloned iSCSI root disk, a 1 GiB iSCSI data disk and a 1 GiB NFS data disk.
In-guest timestamps were written to all three disks every 5 seconds.

| Scenario | Result |
|---|---|
| Planned failover | **72–101 s** over 3 runs (stop 9, final sync 43, promote 33, start 15 in the slowest run), **RPO 0** verified in the guest |
| Failback (reverse resync + planned failover back) | **75–95 s** over 4 runs, **RPO 0** |
| Unplanned failover | Harvey reported ≤ 65 s. Measured loss: 7 s on the data disks, 62 s on the root disk |
| Steady-state lag, 1-minute schedule | 7–41 s (51 samples) |

## Limits

- **Asynchronous only.** Trident TMRs support async SnapMirror policies, with a 5-minute supported minimum
  schedule. The ONTAP `Sync` policy works between the clusters but has no Trident integration. NetApp's zero-RPO
  answer is MetroCluster.
- **Per-volume consistency.** Each disk is its own SnapMirror relationship. After an unplanned failover, the disks
  of one VM can come from different points in time (55 s apart in the lab). Planned failovers aren't affected,
  because the VM is stopped before the final update. ONTAP consistency groups aren't available through Trident,
  and VolumeGroupSnapshots don't work with replication yet.
- **Scale.** One relationship per disk. ONTAP allows 100 or 250 concurrent transfers per system, depending on the
  platform.
- **Simulators.** A 6 GB ONTAP simulator runs out of WAFL memory under replication load, and transfers stall
  silently. Give simulators 10 GB. Harvey's stall detection keeps a demo moving, but it's no substitute for memory.

## Harvey and NetApp Trident Protect

Trident Protect is NetApp's own application-level DR. Both were run side by side in the lab:

| | Harvey + SnapMirror | Trident Protect 26.06 |
|---|---|---|
| Recovery point | per disk, crash-consistent | one snapshot of all of a VM's disks, guest frozen via qemu-guest-agent |
| VM on the DR site | stopped replica always present | recreated at promotion from metadata in S3 |
| Planned failover | 72–101 s | 166 s (1 run) |
| Failback | reverse resync (changed blocks) | full re-copy in our run |
| Extras | multi-VM plans, priorities, hooks, health checks, split-brain checks | backups, restores, clones, transformations, NetApp support |

The two are complementary. Trident Protect gives an app-consistent recovery point and vendor support. Harvey adds
ordering, preflight checks, status, health checks and the audit trail on top of SnapMirror.
