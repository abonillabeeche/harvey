#!/usr/bin/env python3
"""
Harvey - DR Orchestration CLI for KubeVirt / SUSE Virtualization

Storage-aware disaster recovery across two sites:
  - NetApp ONTAP SnapMirror (driven through Trident TridentMirrorRelationships) replicates the VM disks
  - Harvey replicates the metadata (VM spec, cloud-init/credential Secrets, ConfigMaps, NADs)
  - Failover  : stop on source -> final SnapMirror update -> promote replica volumes -> start on DR
  - Reprotect : reverse resync so the old primary becomes the replica; failback = failover + reprotect

Also: recovery plans (priority groups, hooks), RTO/RPO tracking, JSONL audit log, health checks,
split-brain detection.
"""

import copy
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import typer
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table
from typing_extensions import Annotated

from . import __version__
from .kube import KubeClient, KubeError
from .ontap import OntapError
from .replication import (L_ROLE, A_REPROTECT, ReplicationError, Replicator, vm_status as _vm_printable)

# ── Paths ────────────────────────────────────────────────────────────────────
CONFIG_DIR  = Path(os.environ.get("HARVEY_CONFIG_DIR", Path.home() / ".config" / "harvey"))
CONFIG_FILE = CONFIG_DIR / "config.yaml"
PLANS_DIR   = CONFIG_DIR / "plans"
AUDIT_LOG   = CONFIG_DIR / "audit.log"

CONFIG_TEMPLATE = {
    # Only needed for clusters reached through the Rancher proxy (rancher_cluster_id).
    "rancher_url": "https://your-rancher-url.com",
    "api_token":   "token-xxxxx:xxxxxxxxxxxxxxxxxxxxxxxxxx",
    "clusters": {
        # Either a Rancher cluster ID ...
        "primary": {"rancher_cluster_id": "c-m-xxxxxxxx"},
        # ... or a kubeconfig (token or client-cert auth); context optional.
        "dr":      {"kubeconfig": "~/.kube/dr.yaml"},
        # ONTAP credentials are discovered from each cluster's Trident backend (managementLIF + Secret).
        # Override per cluster with:  ontap: {host: 192.0.2.10, username: vsadmin, password: ...}
    },
    "defaults": {"source": "primary", "destination": "dr"},
    "replication": {
        "policy":   "MirrorAllSnapshots",   # an async SnapMirror policy (Trident supports async only)
        "schedule": "5min",                 # an ONTAP job schedule on the destination cluster
        "storage_class_map": {},            # source SC -> destination SC, when names differ
        "max_lag_seconds": 900,             # health-check threshold
    },
}

PLAN_TEMPLATE = """\
# Harvey Recovery Plan — edit and save to ~/.config/harvey/plans/<name>.yaml
name: {name}
description: "Production failover plan"
source:      primary
destination: dr

# Protection groups control VM grouping and startup order.
# Higher priority number = started FIRST and stopped LAST (most critical infra).
protection_groups:

  - name: database-tier
    description: "Core databases — must be running before app layer starts"
    priority: 100          # stopped last, started first
    startup_delay: 60      # seconds to wait after issuing 'start' before proceeding
    vms:
      - namespace: production
        name: db-primary
      - namespace: production
        name: db-replica

  - name: app-tier
    description: "Application servers"
    priority: 50
    startup_delay: 30
    vms:
      - namespace: production
        name: app-server-1

  - name: web-tier
    description: "Web / proxy layer — starts last"
    priority: 10
    startup_delay: 0
    vms:
      - namespace: production
        name: web-proxy

# Hooks execute shell commands at key phases.
# on_failure: warn (default) | abort
# Environment variables injected: HARVEY_SOURCE, HARVEY_DESTINATION
hooks:
  pre_failover: []
  # - name: "Notify on-call"
  #   command: "curl -sf -X POST -d 'DR starting' https://hooks.slack.com/..."
  #   timeout: 30
  #   on_failure: warn

  pre_start: []
  # - name: "Update load-balancer"
  #   command: "./scripts/lb-drain.sh --site primary"

  post_failover: []
  # - name: "Update DNS to DR site"
  #   command: "./scripts/update-dns.sh --target dr"
  #   timeout: 60
"""

# ── App skeleton ──────────────────────────────────────────────────────────────
state = {"debug": False}

app          = typer.Typer(help="Harvey — DR orchestration CLI for KubeVirt/SUSE Virtualization.",
                           add_completion=False, rich_markup_mode="markdown")
config_app   = typer.Typer(help="Manage CLI configuration.")
failover_app = typer.Typer(help="Ad-hoc failover operations.")
plan_app     = typer.Typer(help="Manage and execute recovery plans.")
repl_app     = typer.Typer(help="Replication (SnapMirror) state of protected VMs.")

app.add_typer(config_app,   name="config")
app.add_typer(failover_app, name="failover")
app.add_typer(plan_app,     name="plan")
app.add_typer(repl_app,     name="replication")

console = Console()


@app.callback()
def main_callback(
    debug: Annotated[bool, typer.Option("--debug", help="Verbose API output.")] = False,
):
    """Harvey — storage-aware DR orchestration for KubeVirt/SUSE Virtualization."""
    if debug:
        state["debug"] = True
        console.print("[bold yellow]>>> DEBUG MODE ENABLED <<<[/bold yellow]")


@app.command("version")
def version():
    """Prints the Harvey version."""
    console.print(f"harvey {__version__}")


# ── Audit log ─────────────────────────────────────────────────────────────────
def _audit(event: str, details: dict):
    entry = {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(), "event": event, **details}
    try:
        AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(AUDIT_LOG, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # audit failure must never block DR operations


# ── API client ────────────────────────────────────────────────────────────────
class HarvesterClient(KubeClient):
    """KubeClient plus the forgiving helpers the inventory/plan commands use (print + return None on error)."""

    @property
    def cluster_name(self) -> str:
        return self.name

    def _safe(self, fn, *a, **kw):
        try:
            return fn(*a, **kw)
        except KubeError as e:
            console.print(f"[bold red]{e}[/bold red]")
            return None

    def get_vms(self, namespace: str = None):
        r = self._safe(self.list, "vm", namespace)
        return None if r is None else {"items": r}

    def get_vm(self, namespace: str, vm_name: str):
        return self._safe(self.get, "vm", namespace, vm_name)

    def create_vm(self, namespace: str, vm_body: dict):
        try:
            return self.create("vm", namespace, vm_body)
        except KubeError as e:
            if e.status == 409:
                return {"status": "ok", "http_status": 409}
            console.print(f"[bold red]{e}[/bold red]")
            return None

    def delete_vm(self, namespace: str, vm_name: str):
        return self._safe(self.delete, "vm", namespace, vm_name)

    def _vm_action(self, namespace: str, vm_name: str, action: str):
        """start/stop through spec.runStrategy (works without the Harvester API, e.g. with a plain kubeconfig)."""
        console.print(f"  Sending '[bold]{action}[/bold]' → [magenta]{namespace}/{vm_name}[/magenta] "
                      f"on [cyan]{self.name}[/cyan]...")
        vm = self.get_vm(namespace, vm_name)
        if vm is None:
            return None
        if action == "stop":
            patch = {"spec": {"runStrategy": "Halted"}}
        else:
            from .replication import original_run_strategy
            patch = {"spec": {"runStrategy": original_run_strategy(vm)}}
        if "running" in vm["spec"] and "runStrategy" not in vm["spec"]:
            patch = {"spec": {"running": action != "stop"}}
        return self._safe(self.merge_patch, "vm", namespace, vm_name, patch)

    def get_namespace(self, name: str):
        return self._safe(self.get, "namespace", None, name)

    def ensure_namespace(self, name: str) -> bool:
        try:
            KubeClient.ensure_namespace(self, name)
            return True
        except KubeError as e:
            console.print(f"[bold red]{e}[/bold red]")
            return False

    def delete_namespace(self, name: str):
        return self._safe(self.delete, "namespace", None, name)


# ── Config helpers ────────────────────────────────────────────────────────────
def load_config() -> dict:
    if not CONFIG_FILE.exists():
        console.print("[bold yellow]Configuration file not found.[/bold yellow]")
        console.print(f"Run [bold cyan]harvey config init[/bold cyan] to create one at {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE) as fh:
        return yaml.safe_load(fh)


def make_client(config: dict, name: str) -> HarvesterClient:
    clusters = config.get("clusters", {})
    if name not in clusters:
        console.print(f"[bold red]Cluster '{name}' not found in config.[/bold red] Available: {list(clusters)}")
        sys.exit(1)
    c = clusters[name]
    if isinstance(c, str):  # legacy: bare Rancher cluster ID
        c = {"rancher_cluster_id": c}
    if c.get("kubeconfig"):
        return HarvesterClient.from_kubeconfig(name, c["kubeconfig"], c.get("context"), debug=state["debug"])
    if c.get("rancher_cluster_id"):
        return HarvesterClient.from_rancher(name, config["rancher_url"], config["api_token"],
                                            c["rancher_cluster_id"], debug=state["debug"])
    console.print(f"[bold red]Cluster '{name}': set 'kubeconfig' or 'rancher_cluster_id'.[/bold red]")
    sys.exit(1)


def get_clients(config: dict, source: str, destination: str) -> Tuple[HarvesterClient, HarvesterClient]:
    return make_client(config, source), make_client(config, destination)


def resolve_pair(config: dict, source: Optional[str], destination: Optional[str]) -> Tuple[str, str]:
    d = config.get("defaults") or {}
    s, t = source or d.get("source"), destination or d.get("destination")
    if not s or not t:
        console.print("[bold red]Give --source/--destination or set defaults.source/destination in the config."
                      "[/bold red]")
        sys.exit(1)
    return s, t


def make_replicator(config: dict, names: List[str]) -> Replicator:
    clients = {n: make_client(config, n) for n in names}
    overrides = {}
    for n in names:
        c = config["clusters"].get(n)
        if isinstance(c, dict) and c.get("ontap"):
            o = dict(c["ontap"])
            if o.get("password_env"):
                o["password"] = os.environ.get(o["password_env"], "")
            overrides[n] = o
    return Replicator(clients, config.get("replication") or {}, log=console.print,
                      ontap_overrides=overrides, debug=state["debug"])


# ── Display helpers ───────────────────────────────────────────────────────────
def get_status_style(status: str) -> str:
    return {
        "Running":  "[bold green]",
        "Stopped":  "[bold red]",
        "Halted":   "[bold red]",
        "Starting": "[bold yellow]",
        "Stopping": "[bold yellow]",
        "Not Found":"[dim]",
    }.get(status, "")


def _display_plan_table(plan: list, source: str, destination: str, title: str = None, dry_run: bool = False):
    t = Table(
        title=title or f"Failover Plan: [cyan]{source}[/cyan] → [green]{destination}[/green]",
        box=box.ROUNDED,
    )
    t.add_column("Stop #",  style="yellow",  justify="right")
    t.add_column("Start #", style="green",   justify="right")
    t.add_column("Namespace", style="cyan")
    t.add_column("VM Name",   style="magenta")
    t.add_column("Priority",  justify="right")
    t.add_column("Startup Delay", justify="right")

    by_stop  = sorted(plan, key=lambda x: x[2])
    by_start = sorted(plan, key=lambda x: x[2], reverse=True)

    for stop_idx, (ns, name, prio, delay) in enumerate(by_stop, 1):
        start_idx = next(i for i, item in enumerate(by_start, 1) if item == (ns, name, prio, delay))
        t.add_row(str(stop_idx), str(start_idx), ns, name, str(prio),
                  f"{delay}s" if delay else "—")
    console.print(t)

    if not dry_run:
        console.rule("[bold blue]Stop Phase[/bold blue]")
        for ns, vm, prio, _ in by_stop:
            console.print(f"  [yellow]Stop[/yellow]  {ns}/{vm}  (priority {prio})")
        console.rule("[bold blue]Start Phase[/bold blue]")
        for ns, vm, prio, delay in by_start:
            suffix = f", then wait {delay}s" if delay else ""
            console.print(f"  [green]Start[/green] {ns}/{vm}  (priority {prio}){suffix}")
        console.rule()


# ── Hook runner ───────────────────────────────────────────────────────────────
def _run_hook(hook: dict, context: dict) -> bool:
    name       = hook.get("name", "unnamed")
    command    = hook.get("command", "")
    timeout    = hook.get("timeout", 60)
    on_failure = hook.get("on_failure", "warn")

    if not command:
        return True

    console.print(f"  Running hook: [bold]{name}[/bold]")
    env = {**os.environ, **{"HARVEY_" + k.upper(): str(v) for k, v in context.items()}}
    try:
        r = subprocess.run(command, shell=True, timeout=timeout, capture_output=True, text=True, env=env)
        if r.returncode == 0:
            console.print(f"  [green]✓ Hook '{name}' succeeded.[/green]")
            if r.stdout.strip():
                console.print(f"  [dim]{r.stdout.strip()}[/dim]")
            return True
        msg = f"Hook '{name}' exited {r.returncode}" + (f": {r.stderr.strip()}" if r.stderr.strip() else "")
        if on_failure == "abort":
            console.print(f"  [bold red]✗ {msg} — aborting.[/bold red]")
            return False
        console.print(f"  [yellow]⚠ {msg} — continuing (on_failure=warn).[/yellow]")
        return True
    except subprocess.TimeoutExpired:
        console.print(f"  [yellow]⚠ Hook '{name}' timed out ({timeout}s) — continuing.[/yellow]")
        return True


def _run_hooks(hooks: list, phase: str, context: dict) -> bool:
    """Runs all hooks for a phase. Returns False only if an abort-level hook fails."""
    if not hooks:
        return True
    console.rule(f"[dim]Hooks → {phase}[/dim]")
    for hook in hooks:
        if not _run_hook(hook, context):
            return False
    return True


# ── VM polling ────────────────────────────────────────────────────────────────
def _poll_until(
    client: HarvesterClient, namespace: str, vm_name: str,
    target_statuses: tuple, label: str, timeout_s: int,
) -> bool:
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  transient=True) as progress:
        task = progress.add_task(f"Waiting for '{vm_name}' ({label})...", total=None)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            details = client.get_vm(namespace, vm_name)
            if details:
                status = details.get("status", {}).get("printableStatus", "Unknown")
                if status in target_statuses:
                    return True
                progress.update(task, description=f"Waiting for '{vm_name}' ({label})... [{status}]")
            time.sleep(5)
    return False


# ── Plan builder ──────────────────────────────────────────────────────────────
def _extract_labels(vm: dict) -> Tuple[int, int]:
    labels = vm.get("metadata", {}).get("labels", {})
    try:
        priority = int(labels.get("failover-priority", "0"))
    except (ValueError, TypeError):
        priority = 0
    try:
        startup_delay = int(labels.get("failover-startup-time", "0"))
    except (ValueError, TypeError):
        startup_delay = 0
    return priority, startup_delay


def _fetch_vm_maps(client_source: HarvesterClient, client_dest: HarvesterClient):
    with console.status("[cyan]Fetching VM inventory from both clusters...[/cyan]"):
        src_data = client_source.get_vms()
        dst_data = client_dest.get_vms()
    if not src_data or not dst_data:
        console.print("[red]Failed to retrieve VM data from one or both clusters.[/red]")
        return None, None
    vms_src = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in src_data.get("items", [])}
    vms_dst = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in dst_data.get("items", [])}
    return vms_src, vms_dst


def _build_plan_from_inventory(
    vms_src: dict, vms_dst: dict,
    namespace_filter: str = None,
    vm_names_filter: List[str] = None,
) -> list:
    """[(namespace, vm_name, priority, startup_delay)] for VMs that are PRIMARY on the source and have a Harvey
    replica on the destination. Unprotected VMs are listed as skipped."""
    plan = []
    for key, vm in sorted(vms_src.items()):
        if namespace_filter and key[0] != namespace_filter:
            continue
        if vm_names_filter and key[1] not in vm_names_filter:
            continue
        role_src = (vm["metadata"].get("labels") or {}).get(L_ROLE)
        dvm = vms_dst.get(key)
        role_dst = ((dvm or {}).get("metadata", {}).get("labels") or {}).get(L_ROLE)
        if role_src != "primary" or role_dst != "replica":
            if vm_names_filter:
                console.print(f"[yellow]Skipping {key[0]}/{key[1]}: not protected in this direction "
                              f"(source role={role_src}, destination role={role_dst}). "
                              f"Run 'harvey protect' first.[/yellow]")
            continue
        if _vm_printable(vm) != "Running" and not vm_names_filter:
            continue  # namespace/all failovers move running workloads only
        priority, startup_delay = _extract_labels(vm)
        plan.append((*key, priority, startup_delay))
    return plan


# ── Core failover engine ──────────────────────────────────────────────────────
def _execute_failover_plan(
    plan: list,
    config: dict,
    source: str,
    destination: str,
    hooks:     dict = None,
    dry_run:   bool = False,
    unplanned: bool = False,
    force:     bool = False,
) -> dict:
    """
    Storage-aware failover. Planned: metadata sync -> stop on source (lowest priority first) ->
    final SnapMirror update per volume -> promote replica volumes -> start on destination
    (highest priority first). Unplanned: promote + start only, data loss up to the replication lag.
    """
    hooks = hooks or {}
    ctx = {"source": source, "destination": destination}
    empty = {"stopped": [], "started": [], "failed": [], "rto_seconds": None, "rpo": {}, "phases": {}, "synced": []}

    if dry_run:
        console.print("\n[bold yellow]DRY RUN — no changes will be made.[/bold yellow]")
        steps = (["metadata sync", "stop (lowest priority first)", "final SnapMirror update"]
                 if not unplanned else []) + ["promote replica volumes", "start (highest priority first)"]
        console.print("  Phases: " + " → ".join(steps))
        for ns, vm, prio, _ in sorted(plan, key=lambda x: x[2]):
            console.print(f"  [yellow]Would fail over[/yellow] {ns}/{vm}  (priority {prio})")
        console.rule("[bold green]Dry run complete[/bold green]")
        return empty

    rep = make_replicator(config, [source, destination])

    def hook_cb(phase: str) -> bool:
        return _run_hooks(hooks.get(phase, []), phase, ctx)

    try:
        r = rep.failover(plan, source, destination, unplanned=unplanned, force=force, hooks_cb=hook_cb)
    except (ReplicationError, KubeError, OntapError) as e:
        console.print(f"\n[bold red]Failover aborted: {e}[/bold red]")
        _audit("failover_aborted", {"source": source, "destination": destination, "error": str(e)})
        return {**empty, "failed": [("-", "preflight", str(e))]}
    return {"stopped": r.stopped, "started": r.started, "failed": r.failed, "rto_seconds": r.rto_seconds,
            "rpo": r.rpo, "phases": r.phases, "synced": r.synced, "promoted": r.promoted}


def _print_failover_summary(result: dict):
    rto = result.get("rto_seconds")
    console.rule("[bold green]Failover Complete[/bold green]" if not result["failed"]
                 else "[bold yellow]Failover finished with errors[/bold yellow]")
    t = Table(title="Failover Summary", box=box.ROUNDED)
    t.add_column("Metric", style="bold")
    t.add_column("Value", justify="right")
    t.add_row("[blue]VMs stopped on source[/blue]", str(len(result["stopped"])))
    t.add_row("[magenta]Volumes final-synced[/magenta]", str(len(result.get("synced", []))))
    t.add_row("[green]VMs started on DR[/green]", str(len(result["started"])))
    if result["failed"]:
        t.add_row("[red]Failures[/red]", str(len(result["failed"])))
    for phase, secs in (result.get("phases") or {}).items():
        t.add_row(f"[dim]  {phase}[/dim]", f"{secs}s")
    if rto is not None:
        t.add_row("[cyan]Total RTO[/cyan]", f"{rto}s  ({rto // 60}m {rto % 60}s)")
    for vm, rpo in (result.get("rpo") or {}).items():
        t.add_row(f"[cyan]RPO {vm}[/cyan]",
                  "0s (lossless)" if rpo == 0 else ("unknown" if rpo is None else f"≤ {rpo}s"))
    console.print(t)
    for vm, phase, err in result["failed"]:
        console.print(f"  [red]• {vm} [{phase}]: {err}[/red]")



def _save_plan_from_dry_run(plan: list, source: str, destination: str):
    """Prompt the user to save the dry-run VM list as a named recovery plan."""
    if not Confirm.ask("\nSave this as a recovery plan?", default=False):
        return

    name = Prompt.ask("Plan name").strip()
    if not name:
        console.print("[yellow]No name given — skipping save.[/yellow]")
        return

    plan_file = PLANS_DIR / f"{name}.yaml"
    if plan_file.exists():
        if not Confirm.ask(f"Plan '{name}' already exists. Overwrite?"):
            return

    # Group VMs by priority; each unique priority → one protection_group.
    groups: dict = {}
    for ns, vm, prio, delay in sorted(plan, key=lambda x: x[2], reverse=True):
        if prio not in groups:
            groups[prio] = {"delay": delay, "vms": []}
        groups[prio]["vms"].append({"namespace": ns, "name": vm})

    protection_groups = [
        {
            "name": f"priority-{prio}",
            "description": f"Priority {prio} VMs (auto-generated)",
            "priority": prio,
            "startup_delay": meta["delay"],
            "vms": meta["vms"],
        }
        for prio, meta in sorted(groups.items(), reverse=True)
    ]

    data = {
        "name": name,
        "description": f"Generated from dry-run failover {source} → {destination}",
        "source": source,
        "destination": destination,
        "protection_groups": protection_groups,
        "hooks": {"pre_failover": [], "pre_start": [], "post_failover": []},
    }

    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    plan_file.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    console.print(f"[green]✓ Plan saved to {plan_file}[/green]")
    console.print(f"  Review with : harvey plan show {name}")
    console.print(f"  Run with    : harvey plan run {name} --dry-run")


def _ad_hoc_failover(
    config: dict, plan: list, source: str, destination: str, yes: bool, dry_run: bool,
    unplanned: bool = False, force: bool = False, hint: bool = True,
):
    if not plan:
        console.print("[green]No protected VMs to fail over (primary on source, replica on destination).[/green]")
        return
    _display_plan_table(plan, source, destination, dry_run=dry_run)
    if unplanned:
        console.print("[bold red]UNPLANNED failover: no final sync — data written after the last SnapMirror "
                      "transfer is lost.[/bold red]")
    if not yes and not dry_run:
        if not Confirm.ask("\nProceed with this failover?"):
            console.print("Aborted.")
            return
    _audit("failover_started", {"source": source, "destination": destination, "vm_count": len(plan),
                                "dry_run": dry_run, "unplanned": unplanned})
    result = _execute_failover_plan(plan, config, source, destination, dry_run=dry_run,
                                    unplanned=unplanned, force=force)
    if dry_run:
        _save_plan_from_dry_run(plan, source, destination)
        return
    _print_failover_summary(result)
    _audit("failover_completed", {
        "source": source, "destination": destination, "unplanned": unplanned,
        "started": len(result["started"]), "failed": len(result["failed"]),
        "rto_seconds": result.get("rto_seconds"), "rpo": result.get("rpo"), "phases": result.get("phases"),
    })
    if result["started"] and hint:
        console.print(f"\nNext: once {source} is healthy, run "
                      f"[bold cyan]harvey reprotect -n <ns> <vm>[/bold cyan] to replicate back "
                      f"({destination} → {source}).")



# ── Shadow VM spec builder ────────────────────────────────────────────────────
def _build_shadow_vm_spec(source_vm: dict, target_namespace: str) -> dict:
    """
    Produces a clean VirtualMachine object for deployment in a shadow namespace.

    Strips all runtime metadata (UID, resourceVersion, generation, status,
    managedFields) and Harvester-internal annotations that would conflict with
    a fresh object.  Adds harvey-test=true label for easy identification.

    NOTE ON STORAGE: The cloned spec retains the original volume/PVC references.
    If those volumes do not exist in the shadow namespace the VM will enter an
    error state and the test will report a timeout.  This mirrors SRM's
    snapshot-clone approach — the intent is to discover such gaps during
    testing rather than at real DR time.
    """
    vm = copy.deepcopy(source_vm)
    orig_meta = vm.get("metadata", {})

    labels = dict(orig_meta.get("labels", {}))
    labels["harvey-test"]             = "true"
    labels["harvey-source-namespace"] = orig_meta.get("namespace", "")

    # Strip Harvester internal annotations that would cause admission errors
    _skip = (
        "kubectl.kubernetes.io",
        "field.cattle.io",
        "harvesterhci.io/volumeClaimTemplates",
        "kubevirt.io/latest-observed-api-version",
        "kubevirt.io/storage-observed-api-version",
    )
    annotations = {
        k: v for k, v in orig_meta.get("annotations", {}).items()
        if not any(k.startswith(p) for p in _skip)
    }

    spec = vm.get("spec", {})
    # Ensure the VM is created stopped; we start it explicitly via the action API
    spec["running"] = False

    return {
        "apiVersion": "kubevirt.io/v1",
        "kind":       "VirtualMachine",
        "metadata": {
            "name":        orig_meta["name"],
            "namespace":   target_namespace,
            "labels":      labels,
            "annotations": annotations,
        },
        "spec": spec,
    }


# ── DR Test engine ────────────────────────────────────────────────────────────
def _run_plan_test(
    plan_data: dict,
    source: str,
    destination: str,
    protection_groups: list,
):
    """
    Non-disruptive DR test that mirrors VMware SRM 'Test Recovery':

    Phase 1 — Clone: copies VM specs into shadow namespaces (*-harvey-test)
              on the DR cluster and starts them.
    Phase 2 — Verify: polls each shadow VM until it reaches Running state.
    Phase 3 — Cleanup: deletes all shadow namespaces regardless of outcome.

    Production VMs on both clusters are never touched.

    Storage caveat: shadow VMs retain original PVC/DataVolume references.
    If those volumes don't exist on the DR cluster the VM will fail to start,
    which is exactly the kind of gap this test is designed to surface.
    """
    config = load_config()
    client_source, client_dest = get_clients(config, source, destination)

    SUFFIX = "-harvey-test"

    console.print(Panel(
        f"[bold yellow]NON-DISRUPTIVE DR TEST[/bold yellow]\n\n"
        f"Plan :        [bold]{plan_data.get('name', '?')}[/bold]\n"
        f"DR cluster:   [bold]{destination}[/bold]\n\n"
        f"What happens:\n"
        f"  1. VM specs cloned into shadow namespaces ([cyan]*{SUFFIX}[/cyan]) on [bold]{destination}[/bold]\n"
        f"  2. Shadow VMs started and polled for Running state (5-min timeout)\n"
        f"  3. All shadow namespaces deleted on completion\n\n"
        f"[green]Production VMs on both clusters are NOT touched.[/green]\n"
        f"[dim]Storage note: if PVCs/DataVolumes don't exist on DR, VMs will timeout — "
        f"that gap is exactly what this test surfaces.[/dim]",
        title="Harvey DR Test Mode",
        border_style="yellow",
    ))

    if not Confirm.ask("Start the DR test?"):
        console.print("Aborted.")
        return

    shadow_namespaces: set = set()
    # [(pg_name, orig_ns, vm_name, shadow_ns, outcome)]
    results: List[Tuple] = []
    rto_start = time.time()

    try:
        # ── Phase 1: Clone VMs ────────────────────────────────────────────────
        console.rule("[bold blue]Phase 1 — Cloning VMs into Shadow Namespaces[/bold blue]")

        for pg in protection_groups:
            pg_name = pg.get("name", "unnamed-pg")
            for vm_entry in pg.get("vms", []):
                orig_ns  = vm_entry["namespace"]
                vm_name  = vm_entry["name"]
                shadow_ns = f"{orig_ns}{SUFFIX}"
                shadow_namespaces.add(shadow_ns)

                console.print(f"\n[bold]{pg_name}[/bold] → [cyan]{orig_ns}/{vm_name}[/cyan]")

                # Prefer the DR-side spec (it's the actual replica); fall back to source
                vm_data = client_dest.get_vm(orig_ns, vm_name)
                if not vm_data:
                    console.print(
                        f"  [dim]VM not found on DR cluster — trying source cluster...[/dim]"
                    )
                    vm_data = client_source.get_vm(orig_ns, vm_name)
                if not vm_data:
                    console.print(f"  [red]✗ VM not found on either cluster. Skipping.[/red]")
                    results.append((pg_name, orig_ns, vm_name, shadow_ns, "not_found"))
                    continue

                # Ensure shadow namespace exists on DR cluster
                console.print(f"  Ensuring shadow namespace [cyan]{shadow_ns}[/cyan] on {destination}...")
                if not client_dest.ensure_namespace(shadow_ns):
                    console.print(f"  [red]✗ Failed to create shadow namespace.[/red]")
                    results.append((pg_name, orig_ns, vm_name, shadow_ns, "ns_failed"))
                    continue
                time.sleep(1)  # give the namespace a moment to be fully registered

                # Build and create the shadow VM
                shadow_vm = _build_shadow_vm_spec(vm_data, shadow_ns)
                console.print(f"  Creating shadow VM [cyan]{shadow_ns}/{vm_name}[/cyan]...")
                created = client_dest.create_vm(shadow_ns, shadow_vm)
                if not created or created.get("http_status") == 409:
                    # 409 = already exists from a previous interrupted test; treat as ok
                    if created and created.get("http_status") == 409:
                        console.print(f"  [yellow]⚠ Shadow VM already exists (leftover?). Reusing.[/yellow]")
                    else:
                        console.print(f"  [red]✗ Failed to create shadow VM.[/red]")
                        results.append((pg_name, orig_ns, vm_name, shadow_ns, "clone_failed"))
                        continue

                # Start the shadow VM
                console.print(f"  Issuing start...")
                client_dest._vm_action(shadow_ns, vm_name, "start")
                results.append((pg_name, orig_ns, vm_name, shadow_ns, "starting"))

        # ── Phase 2: Verify ───────────────────────────────────────────────────
        console.rule("[bold blue]Phase 2 — Verifying Shadow VMs[/bold blue]")

        for i, (pg_name, orig_ns, vm_name, shadow_ns, outcome) in enumerate(results):
            if outcome != "starting":
                continue
            console.print(f"\n  Polling [cyan]{shadow_ns}/{vm_name}[/cyan]...")
            if _poll_until(client_dest, shadow_ns, vm_name, ("Running",), "starting", timeout_s=300):
                console.print(f"  [green]✓ {shadow_ns}/{vm_name} reached Running.[/green]")
                results[i] = (pg_name, orig_ns, vm_name, shadow_ns, "running")
                _audit("dr_test_vm_passed", {"shadow_ns": shadow_ns, "vm": vm_name, "cluster": destination})
            else:
                console.print(f"  [red]✗ {shadow_ns}/{vm_name} did not reach Running within 5 minutes.[/red]")
                results[i] = (pg_name, orig_ns, vm_name, shadow_ns, "timeout")
                _audit("dr_test_vm_timeout", {"shadow_ns": shadow_ns, "vm": vm_name, "cluster": destination})

    finally:
        # ── Phase 3: Cleanup (always runs) ────────────────────────────────────
        console.rule("[bold blue]Phase 3 — Cleanup[/bold blue]")
        for shadow_ns in sorted(shadow_namespaces):
            console.print(f"  Deleting [cyan]{shadow_ns}[/cyan] on {destination}...")
            client_dest.delete_namespace(shadow_ns)
            console.print(f"  [green]✓ Deleted.[/green]")

    # ── Results summary ───────────────────────────────────────────────────────
    rto = int(time.time() - rto_start)
    console.rule("[bold green]DR Test Complete[/bold green]")

    t = Table(title="DR Test Results", box=box.ROUNDED)
    t.add_column("Protection Group")
    t.add_column("Namespace",      style="cyan")
    t.add_column("VM Name",        style="magenta")
    t.add_column("Shadow NS",      style="dim")
    t.add_column("Result",         justify="center")

    passed = failed = 0
    outcome_labels = {
        "running":     ("[bold green]✓ PASSED[/bold green]",  "pass"),
        "timeout":     ("[bold red]✗ TIMEOUT[/bold red]",     "fail"),
        "not_found":   ("[bold red]✗ NOT FOUND[/bold red]",   "fail"),
        "ns_failed":   ("[bold red]✗ NS ERROR[/bold red]",    "fail"),
        "clone_failed":("[bold red]✗ CLONE FAILED[/bold red]","fail"),
    }
    for pg_name, orig_ns, vm_name, shadow_ns, outcome in results:
        label, verdict = outcome_labels.get(outcome, (f"[yellow]{outcome}[/yellow]", "fail"))
        if verdict == "pass":
            passed += 1
        else:
            failed += 1
        t.add_row(pg_name, orig_ns, vm_name, shadow_ns, label)

    console.print(t)
    console.print(
        f"\n  [green]Passed:[/green] {passed}  "
        f"[red]Failed:[/red] {failed}  "
        f"[cyan]Test RTO:[/cyan] {rto}s ({rto // 60}m {rto % 60}s)"
    )

    overall_style = "green" if failed == 0 else "red"
    overall_label = "PASSED ✓" if failed == 0 else "FAILED ✗"
    console.print(Panel(f"[bold {overall_style}]DR TEST {overall_label}[/bold {overall_style}]",
                        border_style=overall_style))

    _audit("dr_test_completed", {
        "plan": plan_data.get("name", ""),
        "destination": destination,
        "passed": passed, "failed": failed,
        "rto_seconds": rto,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# CLI COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

# ── config ────────────────────────────────────────────────────────────────────
@config_app.command("init")
def config_init():
    """Creates the default configuration file at ~/.config/harvey/config.yaml."""
    if CONFIG_FILE.exists():
        if not Confirm.ask(f"Config already exists at {CONFIG_FILE}. Overwrite?"):
            raise typer.Abort()
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as fh:
        yaml.dump(CONFIG_TEMPLATE, fh, default_flow_style=False, sort_keys=False)
    console.print(f"[green]✓ Config created at {CONFIG_FILE}[/green]")
    console.print("Edit it with your Rancher URL, API token, and cluster IDs.")


@config_app.command("show")
def config_show():
    """Displays current configuration (API token is masked)."""
    cfg = load_config()
    token = cfg.get("api_token", "")
    if len(token) > 12:
        cfg["api_token"] = token[:8] + "***" + token[-4:]
    for c in (cfg.get("clusters") or {}).values():
        if isinstance(c, dict) and (c.get("ontap") or {}).get("password"):
            c["ontap"]["password"] = "***"
    console.print(yaml.dump(cfg, default_flow_style=False))


# ── status ────────────────────────────────────────────────────────────────────
@app.command("status")
def vm_status(
    cluster1: Annotated[str, typer.Argument(help="First cluster name (from config).")],
    cluster2: Annotated[str, typer.Argument(help="Second cluster name (from config).")],
    namespace: Annotated[Optional[str], typer.Option("--namespace", "-n", help="Filter by namespace.")] = None,
):
    """Compares VM status across two clusters and shows DR readiness."""
    config = load_config()
    if config.get("rancher_url") == CONFIG_TEMPLATE["rancher_url"]:
        console.print("[bold red]Default config detected — edit ~/.config/harvey/config.yaml first.[/bold red]")
        sys.exit(1)

    client1, client2 = get_clients(config, cluster1, cluster2)

    with console.status("[cyan]Fetching VM inventory...[/cyan]"):
        d1 = client1.get_vms()
        d2 = client2.get_vms()

    if not d1 or not d2:
        console.print("[red]Could not retrieve VM data from one or both clusters.[/red]")
        return

    vms1 = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in d1.get("items", [])}
    vms2 = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in d2.get("items", [])}

    all_keys = sorted(vms1.keys() | vms2.keys())
    if namespace:
        all_keys = [k for k in all_keys if k[0] == namespace]
        if not all_keys:
            console.print(f"[yellow]No VMs found in namespace '{namespace}'.[/yellow]")
            return

    t = Table(title="VM DR Status", box=box.ROUNDED)
    t.add_column("Namespace",  style="cyan",    no_wrap=True)
    t.add_column("VM Name",    style="magenta")
    t.add_column(cluster1.upper(), justify="center")
    t.add_column(cluster2.upper(), justify="center")
    t.add_column("DR State",   justify="center")
    t.add_column("Priority",   justify="right", style="dim")

    split_brain_count = ready_count = 0

    for ns, name in all_keys:
        vm1 = vms1.get((ns, name))
        vm2 = vms2.get((ns, name))
        s1  = vm1.get("status", {}).get("printableStatus", "Unknown") if vm1 else "Not Found"
        s2  = vm2.get("status", {}).get("printableStatus", "Unknown") if vm2 else "Not Found"

        labels   = (vm1 or vm2).get("metadata", {}).get("labels", {})
        priority = labels.get("failover-priority", "—")

        if s1 == "Not Found" or s2 == "Not Found":
            state_text, state_style = "Standalone", "[dim]"
        elif s1 == "Running" and s2 == "Running":
            state_text, state_style = "❌ SPLIT BRAIN", "[bold white on red]"
            split_brain_count += 1
        elif s1 == s2:
            state_text, state_style = "✅ Synced", "[green]"
        else:
            state_text, state_style = "✅ Ready", "[green]"
            ready_count += 1

        t.add_row(
            ns, name,
            f"{get_status_style(s1)}{s1}[/]",
            f"{get_status_style(s2)}{s2}[/]",
            f"{state_style}{state_text}[/]",
            str(priority),
        )

    console.print(t)

    if split_brain_count:
        console.print(f"\n[bold red]⚠  {split_brain_count} split-brain condition(s) — immediate manual intervention required.[/bold red]")
    if ready_count:
        console.print(f"[green]{ready_count} VM(s) ready for failover.[/green]")


# ── health-check ──────────────────────────────────────────────────────────────
@app.command("health-check")
def health_check(
    cluster1: Annotated[str, typer.Argument(help="First cluster name.")],
    cluster2: Annotated[str, typer.Argument(help="Second cluster name.")],
):
    """
    Pre-flight DR readiness check: connectivity, VM coverage, and split-brain detection.
    Run this before every failover or as a scheduled verification.
    """
    config = load_config()
    client1, client2 = get_clients(config, cluster1, cluster2)

    console.rule("[bold cyan]Harvey DR Health Check[/bold cyan]")
    all_ok = True

    # 1. Connectivity
    for client in (client1, client2):
        with console.status(f"[cyan]Checking connectivity to '{client.cluster_name}'...[/cyan]"):
            data = client.get_vms()
        if data is not None:
            count = len(data.get("items", []))
            console.print(f"  [green]✓[/green] '{client.cluster_name}' reachable — {count} VMs found.")
        else:
            console.print(f"  [red]✗[/red] '{client.cluster_name}' unreachable.")
            all_ok = False

    if not all_ok:
        console.print(Panel("[bold red]✗ Health check FAILED — connectivity issues.[/bold red]", border_style="red"))
        return

    # 2. VM coverage
    d1 = client1.get_vms()
    d2 = client2.get_vms()
    vms1 = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in d1.get("items", [])}
    vms2 = {(v["metadata"]["namespace"], v["metadata"]["name"]): v for v in d2.get("items", [])}

    only_in_1  = vms1.keys() - vms2.keys()
    only_in_2  = vms2.keys() - vms1.keys()
    common     = vms1.keys() & vms2.keys()
    split_brain = [
        k for k in common
        if vms1[k].get("status", {}).get("printableStatus") == "Running"
        and vms2[k].get("status", {}).get("printableStatus") == "Running"
    ]

    console.print(f"\n  VMs on both clusters (failover-eligible): [bold]{len(common)}[/bold]")
    console.print(f"  Only on {cluster1}: [dim]{len(only_in_1)}[/dim]")
    console.print(f"  Only on {cluster2}: [dim]{len(only_in_2)}[/dim]")

    if split_brain:
        console.print(f"\n  [bold red]✗ SPLIT BRAIN — {len(split_brain)} VM(s) running on both clusters:[/bold red]")
        for ns, name in split_brain:
            console.print(f"    [red]{ns}/{name}[/red]")
        all_ok = False
    else:
        console.print(f"  [green]✓ No split-brain conditions detected.[/green]")

    # 3. Replication (protected VMs)
    rep = make_replicator(config, [cluster1, cluster2])
    max_lag = int((config.get("replication") or {}).get("max_lag_seconds", 900))
    protected = rep.protected_vms()
    console.print(f"\n  Protected VMs (SnapMirror): [bold]{len(protected)}[/bold]  (max lag {max_lag}s)")
    for ns, name in protected:
        p, r = rep.direction(ns, name, cluster1, cluster2)
        if not p:
            console.print(f"    [red]✗ {ns}/{name}: no single primary[/red]")
            all_ok = False
            continue
        try:
            rows = rep.volume_status(ns, name, p, r)
        except (ReplicationError, KubeError, OntapError) as e:
            console.print(f"    [red]✗ {ns}/{name}: {e}[/red]")
            all_ok = False
            continue
        bad = [v for v in rows if v.sm_state != "snapmirrored" or (v.lag_s or 0) > max_lag]
        rv = rep.k[r].get("vm", ns, name)
        if ((rv or {}).get("metadata", {}).get("annotations") or {}).get(A_REPROTECT):
            console.print(f"    [yellow]⚠ {ns}/{name}: running on {p} without protection — run harvey reprotect[/yellow]")
            all_ok = False
        elif bad:
            for v in bad:
                console.print(f"    [red]✗ {ns}/{name} {v.claim}: {v.sm_state}, lag {_fmt_lag(v.lag_s)} {v.note}[/red]")
            all_ok = False
        else:
            worst = max((v.lag_s or 0) for v in rows) if rows else 0
            console.print(f"    [green]✓[/green] {ns}/{name} {p} → {r}: {len(rows)} volume(s), worst lag {_fmt_lag(worst)}")

    # 4. Plans coverage
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    plans = list(PLANS_DIR.glob("*.yaml"))
    if plans:
        console.print(f"\n  Saved recovery plans: [bold]{len(plans)}[/bold]")
        for p in plans:
            console.print(f"    [dim]• {p.stem}[/dim]")
    else:
        console.print(f"\n  [yellow]⚠  No recovery plans found. Run 'harvey plan new <name>' to create one.[/yellow]")

    console.print()
    if all_ok:
        console.print(Panel("[bold green]✓ DR health check PASSED — environment ready for failover.[/bold green]",
                            border_style="green"))
    else:
        console.print(Panel("[bold red]✗ DR health check FAILED — resolve issues before failover.[/bold red]",
                            border_style="red"))

    _audit("health_check", {"cluster1": cluster1, "cluster2": cluster2,
                            "common_vms": len(common), "split_brain": len(split_brain), "ok": all_ok})


# ── audit ─────────────────────────────────────────────────────────────────────
@app.command("audit")
def show_audit(
    limit: Annotated[int, typer.Option("--limit", "-l", help="Number of recent entries.")] = 50,
):
    """Shows the audit log of all Harvey operations."""
    if not AUDIT_LOG.exists():
        console.print("[dim]No audit log yet.[/dim]")
        return

    lines = AUDIT_LOG.read_text().strip().splitlines()
    t = Table(title=f"Harvey Audit Log (last {min(limit, len(lines))} of {len(lines)} entries)",
              box=box.ROUNDED)
    t.add_column("Timestamp", style="dim", no_wrap=True)
    t.add_column("Event",     style="cyan")
    t.add_column("Details",   style="white")

    for line in lines[-limit:]:
        try:
            entry   = json.loads(line)
            ts      = entry.pop("timestamp", "")
            event   = entry.pop("event", "")
            details = "  ".join(f"{k}={v}" for k, v in entry.items())
            t.add_row(ts, event, details)
        except json.JSONDecodeError:
            t.add_row("", "raw", line)

    console.print(t)


# ── protection ────────────────────────────────────────────────────────────────
SourceOpt = Annotated[Optional[str], typer.Option("--source", "-s", help="Source cluster (default: config defaults).")]
DestOpt = Annotated[Optional[str], typer.Option("--destination", "-d", help="Destination cluster (default: config defaults).")]
NsOpt = Annotated[str, typer.Option("--namespace", "-n", help="Namespace of the VMs.")]


def _pair_for_vm(rep: Replicator, ns: str, vm: str, a: str, b: str) -> Tuple[str, str]:
    p, r = rep.direction(ns, vm, a, b)
    if not p:
        console.print(f"[bold red]{ns}/{vm}: no single primary found between {a} and {b} "
                      f"(not protected, or both claim primary).[/bold red]")
        raise typer.Exit(1)
    return p, r


@app.command("protect")
def protect(
    vm_names: Annotated[List[str], typer.Argument(help="VM name(s) to protect.")],
    namespace: NsOpt,
    source: SourceOpt = None,
    destination: DestOpt = None,
    wait: Annotated[bool, typer.Option("--wait", help="Wait for the SnapMirror baseline transfer.")] = False,
):
    """
    Protects VMs: replicates every PVC with SnapMirror (Trident TMRs) and copies the VM spec, Secrets,
    ConfigMaps and NADs to the destination, where the replica VM is created stopped.
    """
    config = load_config()
    source, destination = resolve_pair(config, source, destination)
    rep = make_replicator(config, [source, destination])
    rc = 0
    for vm in vm_names:
        console.rule(f"[bold]protect {namespace}/{vm}: {source} → {destination}[/bold]")
        try:
            for line in rep.protect(namespace, vm, source, destination, wait_init=wait):
                console.print(f"  {line}")
            _audit("vm_protected", {"namespace": namespace, "vm": vm, "source": source, "destination": destination})
            console.print(f"  [green]✓ {namespace}/{vm} protected[/green]")
        except (ReplicationError, KubeError, OntapError) as e:
            rc = 1
            console.print(f"  [bold red]✗ {e}[/bold red]")
    if rc:
        raise typer.Exit(rc)


@app.command("unprotect")
def unprotect(
    vm_names: Annotated[List[str], typer.Argument(help="VM name(s).")],
    namespace: NsOpt,
    source: SourceOpt = None,
    destination: DestOpt = None,
    delete_replica: Annotated[bool, typer.Option("--delete-replica", help="Also delete the replica VM, its PVCs "
                                                 "and copied Secrets on the replica side.")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
):
    """Stops replication for VMs (deletes the TMRs; the replica volumes become independent read-write copies)."""
    config = load_config()
    a, b = resolve_pair(config, source, destination)
    rep = make_replicator(config, [a, b])
    for vm in vm_names:
        p, r = _pair_for_vm(rep, namespace, vm, a, b)
        what = "and DELETE the replica VM + volumes " if delete_replica else ""
        if not yes and not Confirm.ask(f"Stop replicating {namespace}/{vm} ({p} → {r}) {what}?"):
            continue
        try:
            for line in rep.unprotect(namespace, vm, p, r, delete_replica):
                console.print(f"  {line}")
            _audit("vm_unprotected", {"namespace": namespace, "vm": vm, "primary": p, "replica": r,
                                      "deleted_replica": delete_replica})
        except (ReplicationError, KubeError) as e:
            console.print(f"  [bold red]✗ {e}[/bold red]")
            raise typer.Exit(1)


@app.command("sync")
def sync(
    vm_names: Annotated[Optional[List[str]], typer.Argument(help="VM name(s); default: all protected VMs.")] = None,
    namespace: Annotated[Optional[str], typer.Option("--namespace", "-n")] = None,
    source: SourceOpt = None,
    destination: DestOpt = None,
    watch: Annotated[int, typer.Option("--watch", help="Repeat every N seconds (0 = once).")] = 0,
):
    """
    Re-copies VM metadata (spec, Secrets, ConfigMaps, NADs) from each VM's primary to its replica.
    Disks are replicated by SnapMirror on its own schedule; this keeps the small stuff in step.
    """
    config = load_config()
    a, b = resolve_pair(config, source, destination)
    rep = make_replicator(config, [a, b])
    while True:
        targets = [(ns, v) for ns, v in rep.protected_vms(namespace) if not vm_names or v in vm_names]
        for ns, vm in targets:
            p, r = rep.direction(ns, vm, a, b)
            if not p:
                console.print(f"[yellow]{ns}/{vm}: direction unclear, skipped[/yellow]")
                continue
            try:
                acts = rep.sync_metadata(ns, vm, p, r)
                warn = [x for x in acts if x.startswith("WARN")]
                console.print(f"[green]✓[/green] {ns}/{vm} {p} → {r}: {len(acts) - len(warn)} object(s)"
                              + (f", [yellow]{len(warn)} warning(s)[/yellow]" if warn else ""))
                for w in warn:
                    console.print(f"    [yellow]{w}[/yellow]")
            except (ReplicationError, KubeError) as e:
                console.print(f"[red]✗ {ns}/{vm}: {e}[/red]")
        if not watch:
            break
        time.sleep(watch)


def _fmt_lag(s: Optional[int]) -> str:
    if s is None:
        return "-"
    return f"{s}s" if s < 120 else f"{s // 60}m{s % 60:02d}s"


@repl_app.command("status")
def replication_status(
    namespace: Annotated[Optional[str], typer.Option("--namespace", "-n")] = None,
    source: SourceOpt = None,
    destination: DestOpt = None,
):
    """Per-volume replication state of protected VMs: TMR states, SnapMirror state, health, lag, recovery point."""
    config = load_config()
    a, b = resolve_pair(config, source, destination)
    rep = make_replicator(config, [a, b])
    vms = rep.protected_vms(namespace)
    if not vms:
        console.print("[dim]No protected VMs.[/dim]")
        return
    t = Table(title="Replication status", box=box.ROUNDED)
    t.add_column("VM / direction / state", no_wrap=True)
    t.add_column("Volume", no_wrap=True)
    for col in ("TMR pri→rep", "SnapMirror", "Healthy", "Lag", "Recovery point"):
        t.add_column(col, overflow="fold")
    t.add_column("Note", overflow="fold", max_width=40)
    short = {"promoted": "prom", "established": "est", "reestablished": "re-est", "-": "-"}
    worst = 0
    for ns, vm in vms:
        p, r = rep.direction(ns, vm, a, b)
        if not p:
            t.add_row(f"{ns}/{vm}\n[red]direction unclear[/red]", "", "", "", "", "", "", "")
            continue
        pv = rep.k[p].get("vm", ns, vm)
        rv = rep.k[r].get("vm", ns, vm)
        vstate = f"{_vm_printable(pv)} / {_vm_printable(rv)}"
        if _vm_printable(pv) == "Running" and _vm_printable(rv) == "Running":
            vstate = "[bold white on red]SPLIT BRAIN[/bold white on red]"
        reprot = ((rv or {}).get("metadata", {}).get("annotations") or {}).get(A_REPROTECT)
        rows = rep.volume_status(ns, vm, p, r)
        for i, vs in enumerate(rows):
            worst = max(worst, vs.lag_s or 0)
            healthy = "-" if vs.healthy is None else ("[green]yes[/green]" if vs.healthy else "[red]no[/red]")
            note = vs.note or ("needs reprotect" if reprot else "")
            first = {0: f"[bold]{ns}/{vm}[/bold]", 1: f"{p} → {r}", 2: vstate}.get(i, "")
            t.add_row(first, vs.claim, f"{short.get(vs.src_tmr, vs.src_tmr)}→{short.get(vs.dst_tmr, vs.dst_tmr)}",
                      vs.sm_state, healthy, _fmt_lag(vs.lag_s),
                      vs.recovery_point.strftime("%H:%M:%S") if vs.recovery_point else "-", note)
        for extra in ([f"{p} → {r}", vstate][len(rows) - 1:] if len(rows) < 3 else []):
            t.add_row(extra, "", "", "", "", "", "", "")
    console.print(t)
    console.print(f"Worst lag: [bold]{_fmt_lag(worst)}[/bold]  (schedule "
                  f"'{(config.get('replication') or {}).get('schedule', '5min')}'; lag = age of the newest "
                  f"replicated snapshot = worst-case data loss if the primary site fails now)")


@app.command("reprotect")
def reprotect(
    vm_names: Annotated[List[str], typer.Argument(help="VM name(s).")],
    namespace: NsOpt,
    source: SourceOpt = None,
    destination: DestOpt = None,
    wait: Annotated[bool, typer.Option("--wait", help="Wait until the reverse resync has transferred.")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
):
    """
    After a failover: replicate back from the new primary to the old one (reverse resync).
    The old primary's copy of the disks is OVERWRITTEN with the new primary's data.
    """
    config = load_config()
    a, b = resolve_pair(config, source, destination)
    rep = make_replicator(config, [a, b])
    for vm in vm_names:
        p, r = _pair_for_vm(rep, namespace, vm, a, b)
        if not yes and not Confirm.ask(f"Reverse-resync {namespace}/{vm}: {p} → {r} (overwrites {r}'s disks)?"):
            continue
        try:
            for line in rep.reprotect(namespace, vm, p, r, wait_init=wait):
                console.print(f"  {line}")
            _audit("vm_reprotected", {"namespace": namespace, "vm": vm, "primary": p, "replica": r})
            console.print(f"  [green]✓ {namespace}/{vm} replicating {p} → {r}[/green]")
        except (ReplicationError, KubeError, OntapError) as e:
            console.print(f"  [bold red]✗ {e}[/bold red]")
            raise typer.Exit(1)


# ── failback ──────────────────────────────────────────────────────────────────
@app.command("failback")
def failback(
    vm_names: Annotated[List[str], typer.Argument(help="VM name(s) to return to their original site.")],
    namespace: NsOpt,
    source: SourceOpt = None,
    destination: DestOpt = None,
    no_reprotect: Annotated[bool, typer.Option("--no-reprotect", help="Do not re-establish replication "
                                               "afterwards.")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
):
    """
    Returns VMs to the other site: planned failover from the current primary (lossless: stop, final sync,
    promote, start), then reprotect so replication runs in the original direction again.
    The VMs must have been reprotected after the original failover (so the old site holds a current copy).
    """
    config = load_config()
    a, b = resolve_pair(config, source, destination)
    rep = make_replicator(config, [a, b])
    groups: dict = {}
    for vm in vm_names:
        p, r = _pair_for_vm(rep, namespace, vm, a, b)
        groups.setdefault((p, r), []).append(vm)
    for (p, r), vms in groups.items():
        cs, cd = make_client(config, p), make_client(config, r)
        vms_src, vms_dst = _fetch_vm_maps(cs, cd)
        if vms_src is None:
            raise typer.Exit(1)
        plan = _build_plan_from_inventory(vms_src, vms_dst, namespace_filter=namespace, vm_names_filter=vms)
        console.print(Panel(f"Failback {', '.join(vms)}: [green]{p}[/green] → [bold]{r}[/bold]"
                            + ("" if no_reprotect else f", then reprotect {r} → {p}"), border_style="yellow"))
        _ad_hoc_failover(config, plan, p, r, yes, False, hint=no_reprotect)
        if no_reprotect:
            continue
        for vm in vms:
            try:
                for line in rep.reprotect(namespace, vm, r, p):
                    console.print(f"  {line}")
                _audit("vm_reprotected", {"namespace": namespace, "vm": vm, "primary": r, "replica": p})
            except (ReplicationError, KubeError, OntapError) as e:
                console.print(f"  [bold red]✗ reprotect {vm}: {e}[/bold red]")


# ── failover subcommands ──────────────────────────────────────────────────────
UnplannedOpt = Annotated[bool, typer.Option("--unplanned", help="Source site is down: skip stop and final sync; "
                                            "promote the last replicated data (data loss up to the lag).")]
ForceOpt = Annotated[bool, typer.Option("--force", help="With --unplanned: proceed even if the VM still runs on "
                                        "the source (split-brain risk).")]


@failover_app.command("vm")
def failover_vms(
    vm_names: Annotated[List[str], typer.Argument(help="VM name(s) to fail over.")],
    namespace: NsOpt,
    source: SourceOpt = None,
    destination: DestOpt = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation.")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Simulate without executing.")] = False,
    unplanned: UnplannedOpt = False,
    force: ForceOpt = False,
):
    """Fails over specific protected VMs (stop → final sync → promote → start)."""
    config = load_config()
    source, destination = resolve_pair(config, source, destination)
    if unplanned:
        plan = [(namespace, v, 0, 0) for v in vm_names]  # source may be unreachable: trust the replica side
        cd = make_client(config, destination)
        plan = [(ns, v, *_extract_labels(cd.get_vm(ns, v) or {"metadata": {}})) for ns, v, _, _ in plan]
    else:
        cs, cd = get_clients(config, source, destination)
        vms_src, vms_dst = _fetch_vm_maps(cs, cd)
        if vms_src is None:
            raise typer.Exit(1)
        plan = _build_plan_from_inventory(vms_src, vms_dst, namespace_filter=namespace, vm_names_filter=vm_names)
    _ad_hoc_failover(config, plan, source, destination, yes, dry_run, unplanned, force)


@failover_app.command("namespace")
def failover_namespace(
    namespace: Annotated[str, typer.Argument(help="Namespace to fail over.")],
    source: SourceOpt = None,
    destination: DestOpt = None,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
):
    """Fails over all running protected VMs in a namespace (planned)."""
    config = load_config()
    source, destination = resolve_pair(config, source, destination)
    cs, cd = get_clients(config, source, destination)
    vms_src, vms_dst = _fetch_vm_maps(cs, cd)
    if vms_src is None:
        raise typer.Exit(1)
    plan = _build_plan_from_inventory(vms_src, vms_dst, namespace_filter=namespace)
    _ad_hoc_failover(config, plan, source, destination, yes, dry_run)


@failover_app.command("all")
def failover_all(
    source: SourceOpt = None,
    destination: DestOpt = None,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
):
    """Fails over all running protected VMs across all namespaces (planned)."""
    config = load_config()
    source, destination = resolve_pair(config, source, destination)
    cs, cd = get_clients(config, source, destination)
    vms_src, vms_dst = _fetch_vm_maps(cs, cd)
    if vms_src is None:
        raise typer.Exit(1)
    plan = _build_plan_from_inventory(vms_src, vms_dst)
    _ad_hoc_failover(config, plan, source, destination, yes, dry_run)



# ── plan subcommands ──────────────────────────────────────────────────────────
@plan_app.command("new")
def plan_new(
    name: Annotated[str, typer.Argument(help="Plan name (used as filename).")],
):
    """Creates a new recovery plan template file."""
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    plan_file = PLANS_DIR / f"{name}.yaml"
    if plan_file.exists():
        if not Confirm.ask(f"Plan '{name}' already exists. Overwrite?"):
            raise typer.Abort()
    plan_file.write_text(PLAN_TEMPLATE.format(name=name))
    console.print(f"[green]✓ Recovery plan template created at {plan_file}[/green]")
    console.print("Edit it with your protection groups, VMs, and hooks.")


@plan_app.command("edit")
def plan_edit(
    name: Annotated[str, typer.Argument(help="Plan name to edit.")],
):
    """Opens a recovery plan in $EDITOR."""
    plan_file = PLANS_DIR / f"{name}.yaml"
    if not plan_file.exists():
        console.print(f"[red]Plan '{name}' not found.[/red]")
        raise typer.Exit(1)
    editor = os.environ.get("EDITOR", "vi")
    subprocess.call([editor, str(plan_file)])


@plan_app.command("list")
def plan_list():
    """Lists all saved recovery plans."""
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    plans = sorted(PLANS_DIR.glob("*.yaml"))
    if not plans:
        console.print("[dim]No plans found. Create one with: harvey plan new <name>[/dim]")
        return

    t = Table(title="Saved Recovery Plans", box=box.ROUNDED)
    t.add_column("Name",                style="cyan")
    t.add_column("Source → Dest")
    t.add_column("Groups",  justify="right")
    t.add_column("VMs",     justify="right")
    t.add_column("Hooks",   justify="right")

    for p in plans:
        try:
            data = yaml.safe_load(p.read_text())
            pgs  = data.get("protection_groups", [])
            vms  = sum(len(pg.get("vms", [])) for pg in pgs)
            h    = data.get("hooks", {})
            hooks = (len(h.get("pre_failover", [])) + len(h.get("pre_start", []))
                     + len(h.get("post_failover", [])))
            t.add_row(p.stem,
                      f"{data.get('source','?')} → {data.get('destination','?')}",
                      str(len(pgs)), str(vms), str(hooks))
        except Exception:
            t.add_row(p.stem, "[red]parse error[/red]", "—", "—", "—")

    console.print(t)


@plan_app.command("show")
def plan_show(
    name: Annotated[str, typer.Argument(help="Plan name.")],
):
    """Shows full details of a recovery plan."""
    plan_file = PLANS_DIR / f"{name}.yaml"
    if not plan_file.exists():
        console.print(f"[red]Plan '{name}' not found in {PLANS_DIR}[/red]")
        raise typer.Exit(1)

    data = yaml.safe_load(plan_file.read_text())
    console.print(Panel(
        f"[bold]{data.get('name', name)}[/bold]\n{data.get('description', '')}",
        title="Recovery Plan",
        border_style="cyan",
    ))
    console.print(f"  Source      : [bold]{data.get('source')}[/bold]")
    console.print(f"  Destination : [bold]{data.get('destination')}[/bold]\n")

    for pg in data.get("protection_groups", []):
        t = Table(
            title=(f"[yellow]Protection Group: {pg.get('name')}[/yellow]  "
                   f"priority={pg.get('priority',0)}  startup_delay={pg.get('startup_delay',0)}s"),
            box=box.SIMPLE,
        )
        t.add_column("Namespace", style="cyan")
        t.add_column("VM Name",   style="magenta")
        for vm in pg.get("vms", []):
            t.add_row(vm.get("namespace", "—"), vm.get("name", "—"))
        console.print(t)

    hooks = data.get("hooks", {})
    all_hooks = (
        [("pre_failover",  h) for h in hooks.get("pre_failover",  [])] +
        [("pre_start",     h) for h in hooks.get("pre_start",     [])] +
        [("post_failover", h) for h in hooks.get("post_failover", [])]
    )
    if all_hooks:
        ht = Table(title="Hooks", box=box.SIMPLE)
        ht.add_column("Phase",   style="yellow")
        ht.add_column("Name")
        ht.add_column("Command", style="dim")
        ht.add_column("on_failure")
        for phase, h in all_hooks:
            ht.add_row(phase, h.get("name",""), h.get("command",""), h.get("on_failure","warn"))
        console.print(ht)


@plan_app.command("run")
def plan_run(
    name:    Annotated[str,  typer.Argument(help="Plan name to execute.")],
    yes:     Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation.")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show plan without executing.")] = False,
    test:    Annotated[bool, typer.Option(
        "--test",
        help="Non-disruptive DR test: clones VMs into shadow namespaces on DR cluster "
             "and verifies they start. Production VMs are never touched.",
    )] = False,
    unplanned: Annotated[bool, typer.Option("--unplanned", help="Source site down: promote + start only.")] = False,
    force: Annotated[bool, typer.Option("--force", help="With --unplanned: ignore VMs still running on source.")] = False,
    reverse: Annotated[bool, typer.Option("--reverse", help="Run the plan destination → source (failback).")] = False,
):
    """
    Executes a recovery plan.

    **Modes:**

    - (default) Planned failover — stop on source, final SnapMirror sync, promote, start on destination.
    - `--unplanned` — source is down: promote the last replicated data and start (RPO = replication lag).
    - `--reverse` — run it the other way round (failback), after `harvey reprotect`.
    - `--dry-run` — shows the ordered plan with no changes.
    - `--test`    — non-disruptive shadow DR test (mirrors SRM 'Test Recovery').
    """
    plan_file = PLANS_DIR / f"{name}.yaml"
    if not plan_file.exists():
        console.print(f"[red]Plan '{name}' not found.[/red]")
        raise typer.Exit(1)

    data              = yaml.safe_load(plan_file.read_text())
    source            = data.get("source")
    destination       = data.get("destination")
    protection_groups = data.get("protection_groups", [])
    hooks             = data.get("hooks", {})
    if reverse:
        source, destination = destination, source

    if test:
        _run_plan_test(data, source, destination, protection_groups)
        return

    # Build flat plan: one tuple per VM across all protection groups
    plan = [
        (vm["namespace"], vm["name"],
         pg.get("priority", 0), pg.get("startup_delay", 0))
        for pg in protection_groups
        for vm in pg.get("vms", [])
    ]

    config = load_config()

    console.print(Panel(
        f"[bold]{data.get('name', name)}[/bold]\n{data.get('description', '')}",
        title="Executing Recovery Plan",
        border_style="cyan",
    ))
    _display_plan_table(plan, source, destination, dry_run=dry_run)

    if not yes and not dry_run:
        if not Confirm.ask("\nExecute this recovery plan?"):
            console.print("Aborted.")
            return

    _audit("plan_run_started", {"plan": name, "source": source, "destination": destination,
                                "dry_run": dry_run})
    result = _execute_failover_plan(plan, config, source, destination, hooks=hooks, dry_run=dry_run,
                                    unplanned=unplanned, force=force)

    if not dry_run:
        _print_failover_summary(result)
        _audit("plan_run_completed", {
            "plan": name, "source": source, "destination": destination, "unplanned": unplanned,
            "started": len(result["started"]), "failed": len(result["failed"]),
            "rto_seconds": result.get("rto_seconds"), "rpo": result.get("rpo"),
        })


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app()
