#!/usr/bin/env python3
"""
Harvey v1.0 - DR Orchestration CLI for KubeVirt / SUSE Virtualization

Competing with VMware SRM for automated VM disaster recovery across two sites.

New in 1.0 vs 0.4:
  - Recovery Plans  : YAML runbooks with protection groups, hooks, and ordering
  - DR Test Mode    : Non-disruptive shadow-namespace clone test (mirrors SRM test recovery)
  - Pre/Post Hooks  : Shell commands at key failover phases
  - RTO Tracking    : Measures and reports actual recovery time
  - Audit Log       : Structured JSONL record of all operations
  - Health Check    : Pre-flight connectivity, coverage, and split-brain detection
  - Reprotect       : Guided reverse-failover workflow after DR activation
  - config show     : Displays masked config
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

import requests
import typer
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table
from typing_extensions import Annotated

# ── Paths ────────────────────────────────────────────────────────────────────
CONFIG_DIR  = Path.home() / ".config" / "harvey"
CONFIG_FILE = CONFIG_DIR / "config.yaml"
PLANS_DIR   = CONFIG_DIR / "plans"
AUDIT_LOG   = CONFIG_DIR / "audit.log"

CONFIG_TEMPLATE = {
    "rancher_url": "https://your-rancher-url.com",
    "api_token":   "token-xxxxx:xxxxxxxxxxxxxxxxxxxxxxxxxx",
    "clusters": {
        "primary": "c-m-xxxxxxxx",
        "dr":      "c-m-yyyyyyyy",
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

app.add_typer(config_app,   name="config")
app.add_typer(failover_app, name="failover")
app.add_typer(plan_app,     name="plan")

console = Console()


@app.callback()
def main_callback(
    debug: Annotated[bool, typer.Option("--debug", help="Verbose API output.")] = False,
):
    """Harvey v1.0 — DR orchestration for KubeVirt/SUSE Virtualization."""
    if debug:
        state["debug"] = True
        console.print("[bold yellow]>>> DEBUG MODE ENABLED <<<[/bold yellow]")


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
class HarvesterClient:
    """Communicates with KubeVirt clusters via the Rancher API proxy."""

    def __init__(self, rancher_url: str, token: str, cluster_id: str, cluster_name: str):
        self.cluster_proxy_url = f"{rancher_url}/k8s/clusters/{cluster_id}"
        self.cluster_name      = cluster_name
        self.session           = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json",
            "Content-Type":  "application/json",
        })
        self.session.verify = False
        requests.packages.urllib3.disable_warnings()

    def _request(self, method: str, path_segment: str, ok_codes: tuple = (), **kwargs):
        """Generic request handler. ok_codes lets callers treat extra HTTP codes as success."""
        url = f"{self.cluster_proxy_url}/{path_segment}"
        if state["debug"]:
            console.print(f"\n[cyan]DEBUG: {method} {url}[/cyan]")
        try:
            resp = self.session.request(method, url, **kwargs)
            if resp.status_code in ok_codes:
                return {"status": "ok", "http_status": resp.status_code}
            resp.raise_for_status()
            if not resp.text:
                return {"status": "success"}
            data = resp.json()
            if state["debug"]:
                console.print(f"[cyan]DEBUG response:[/cyan]\n{json.dumps(data, indent=2)}")
            return data
        except requests.exceptions.HTTPError as e:
            console.print(
                f"[bold red]HTTP {e.response.status_code} from '{self.cluster_name}': "
                f"{e.response.reason}[/bold red]"
            )
            if e.response.text:
                console.print(f"[red]{e.response.text[:400]}[/red]")
            return None
        except requests.exceptions.RequestException as e:
            console.print(f"[bold red]Network error → {self.cluster_proxy_url}: {e}[/bold red]")
            return None
        except json.JSONDecodeError:
            console.print("[bold red]Failed to decode JSON response.[/bold red]")
            return None

    # ── VM operations ─────────────────────────────────────────────────────────
    def get_vms(self, namespace: str = None):
        if namespace:
            return self._request("GET", f"apis/kubevirt.io/v1/namespaces/{namespace}/virtualmachines")
        return self._request("GET", "apis/kubevirt.io/v1/virtualmachines")

    def get_vm(self, namespace: str, vm_name: str):
        return self._request("GET", f"apis/kubevirt.io/v1/namespaces/{namespace}/virtualmachines/{vm_name}")

    def create_vm(self, namespace: str, vm_body: dict):
        return self._request(
            "POST", f"apis/kubevirt.io/v1/namespaces/{namespace}/virtualmachines",
            ok_codes=(409,), json=vm_body,
        )

    def delete_vm(self, namespace: str, vm_name: str):
        return self._request("DELETE", f"apis/kubevirt.io/v1/namespaces/{namespace}/virtualmachines/{vm_name}")

    def _vm_action(self, namespace: str, vm_name: str, action: str):
        console.print(
            f"  Sending '[bold]{action}[/bold]' → "
            f"[magenta]{namespace}/{vm_name}[/magenta] on [cyan]{self.cluster_name}[/cyan]..."
        )
        path = f"v1/harvester/kubevirt.io.virtualmachines/{namespace}/{vm_name}?action={action}"
        return self._request("POST", path)

    # ── Namespace operations ──────────────────────────────────────────────────
    def get_namespace(self, name: str):
        return self._request("GET", f"api/v1/namespaces/{name}")

    def ensure_namespace(self, name: str) -> bool:
        """Creates namespace if absent; returns True when namespace is ready."""
        existing = self.get_namespace(name)
        if existing is not None:
            return True
        body = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}
        result = self._request("POST", "api/v1/namespaces", ok_codes=(409,), json=body)
        return result is not None

    def delete_namespace(self, name: str):
        return self._request("DELETE", f"api/v1/namespaces/{name}")


# ── Config helpers ────────────────────────────────────────────────────────────
def load_config() -> dict:
    if not CONFIG_FILE.exists():
        console.print("[bold yellow]Configuration file not found.[/bold yellow]")
        console.print(f"Run [bold cyan]harvey config init[/bold cyan] to create one at {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE) as fh:
        return yaml.safe_load(fh)


def get_clients(config: dict, source: str, destination: str) -> Tuple[HarvesterClient, HarvesterClient]:
    for name in (source, destination):
        if name not in config.get("clusters", {}):
            console.print(f"[bold red]Cluster '{name}' not found in config.[/bold red]")
            console.print(f"Available: {list(config['clusters'].keys())}")
            sys.exit(1)
    cs = HarvesterClient(config["rancher_url"], config["api_token"], config["clusters"][source],      source)
    cd = HarvesterClient(config["rancher_url"], config["api_token"], config["clusters"][destination], destination)
    return cs, cd


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
            console.print(f"  [yellow]Would stop[/yellow]  {ns}/{vm}  (priority {prio})")
        console.rule("[bold blue]Start Phase[/bold blue]")
        for ns, vm, prio, delay in by_start:
            suffix = f", then wait {delay}s" if delay else ""
            console.print(f"  [green]Would start[/green] {ns}/{vm}  (priority {prio}){suffix}")
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
    vms_src = {
        (v["metadata"]["namespace"], v["metadata"]["name"]): v
        for v in src_data.get("items", [])
    }
    vms_dst_keys = {
        (v["metadata"]["namespace"], v["metadata"]["name"])
        for v in dst_data.get("items", [])
    }
    return vms_src, vms_dst_keys


def _build_plan_from_inventory(
    vms_src: dict, vms_dst_keys: set,
    namespace_filter: str = None,
    vm_names_filter: List[str] = None,
) -> list:
    """Returns [(namespace, vm_name, priority, startup_delay), ...] for running VMs on both clusters."""
    if namespace_filter:
        vms_src      = {k: v for k, v in vms_src.items() if k[0] == namespace_filter}
        vms_dst_keys = {k for k in vms_dst_keys if k[0] == namespace_filter}
    if vm_names_filter:
        vms_src      = {k: v for k, v in vms_src.items() if k[1] in vm_names_filter}

    plan = []
    for key in vms_src.keys() & vms_dst_keys:
        vm     = vms_src[key]
        status = vm.get("status", {}).get("printableStatus", "Unknown")
        if status != "Running":
            continue
        priority, startup_delay = _extract_labels(vm)
        plan.append((*key, priority, startup_delay))
    return plan


# ── Core failover engine ──────────────────────────────────────────────────────
def _execute_failover_plan(
    plan: list,
    client_source: HarvesterClient,
    client_dest:   HarvesterClient,
    hooks:   dict = None,
    dry_run: bool = False,
) -> dict:
    """
    Stops VMs on source (lowest priority first: web→app→db), then starts them on
    destination (highest priority first: db→app→web).  Returns a result dict with
    outcome details and RTO.
    """
    hooks  = hooks or {}
    ctx    = {"source": client_source.cluster_name, "destination": client_dest.cluster_name}
    result = {"stopped": [], "started": [], "failed_stop": [], "failed_start": [], "rto_seconds": None}

    if dry_run:
        console.print("\n[bold yellow]DRY RUN — no changes will be made.[/bold yellow]")
        console.rule("[bold blue]Stop Phase (Dry Run)[/bold blue]")
        for ns, vm, prio, _ in sorted(plan, key=lambda x: x[2]):
            console.print(f"  [yellow]Would stop[/yellow]  {ns}/{vm}  (priority {prio})")
        console.rule("[bold blue]Start Phase (Dry Run)[/bold blue]")
        for ns, vm, prio, delay in sorted(plan, key=lambda x: x[2], reverse=True):
            suffix = f", then wait {delay}s" if delay else ""
            console.print(f"  [yellow]Would start[/yellow] {ns}/{vm}  (priority {prio}){suffix}")
        console.rule("[bold green]Dry run complete[/bold green]")
        return result

    rto_start = time.time()

    # ── pre_failover hooks ────────────────────────────────────────────────────
    if not _run_hooks(hooks.get("pre_failover", []), "pre_failover", ctx):
        console.print("[bold red]Pre-failover hook aborted the operation.[/bold red]")
        return result

    # ── Stop phase ────────────────────────────────────────────────────────────
    console.rule("[bold blue]Stop Phase[/bold blue]")
    for ns, vm, prio, delay in sorted(plan, key=lambda x: x[2]):
        console.print(f"\n[bold]Stopping {ns}/{vm}[/bold]  (priority {prio})")
        client_source._vm_action(ns, vm, "stop")
        if _poll_until(client_source, ns, vm, ("Stopped", "Halted"), "stopping", timeout_s=120):
            console.print(f"  [green]✓ Confirmed stopped.[/green]")
            result["stopped"].append((ns, vm, prio, delay))
            _audit("vm_stopped", {"namespace": ns, "vm": vm, "cluster": client_source.cluster_name})
        else:
            console.print(f"  [red]✗ Timed out waiting for {ns}/{vm} to stop. Skipping start.[/red]")
            result["failed_stop"].append((ns, vm))
            _audit("vm_stop_timeout", {"namespace": ns, "vm": vm, "cluster": client_source.cluster_name})

    if not result["stopped"]:
        console.print("\n[bold yellow]No VMs stopped successfully — aborting start phase.[/bold yellow]")
        return result

    # ── pre_start hooks ───────────────────────────────────────────────────────
    _run_hooks(hooks.get("pre_start", []), "pre_start", ctx)

    # ── Start phase ───────────────────────────────────────────────────────────
    console.rule("[bold blue]Start Phase[/bold blue]")
    for ns, vm, prio, delay in sorted(result["stopped"], key=lambda x: x[2], reverse=True):
        console.print(f"\n[bold]Starting {ns}/{vm}[/bold]  (priority {prio})")
        client_dest._vm_action(ns, vm, "start")
        console.print(f"  [green]✓ Start command sent.[/green]")
        result["started"].append((ns, vm))
        _audit("vm_started", {"namespace": ns, "vm": vm, "cluster": client_dest.cluster_name})
        if delay > 0:
            console.print(f"  Pausing [cyan]{delay}s[/cyan] for service initialization...")
            time.sleep(delay)

    # ── post_failover hooks ───────────────────────────────────────────────────
    _run_hooks(hooks.get("post_failover", []), "post_failover", ctx)

    result["rto_seconds"] = int(time.time() - rto_start)
    return result


def _print_failover_summary(result: dict):
    rto = result.get("rto_seconds")
    console.rule("[bold green]Failover Complete[/bold green]")
    t = Table(title="Failover Summary", box=box.ROUNDED)
    t.add_column("Metric", style="bold")
    t.add_column("Value",  justify="right")
    t.add_row("[green]VMs started on DR[/green]",     str(len(result["started"])))
    t.add_row("[blue]VMs stopped on source[/blue]",   str(len(result["stopped"])))
    if result["failed_stop"]:
        t.add_row("[red]Stop failures[/red]",         str(len(result["failed_stop"])))
    if result["failed_start"]:
        t.add_row("[red]Start failures[/red]",        str(len(result["failed_start"])))
    if rto is not None:
        t.add_row("[cyan]Total RTO[/cyan]",           f"{rto}s  ({rto // 60}m {rto % 60}s)")
    console.print(t)

    if result["failed_stop"]:
        console.print("\n[red]VMs that failed to stop:[/red]")
        for ns, vm in result["failed_stop"]:
            console.print(f"  [red]• {ns}/{vm}[/red]")


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
    client_source: HarvesterClient, client_dest: HarvesterClient,
    plan: list, source: str, destination: str, yes: bool, dry_run: bool,
):
    if not plan:
        console.print("[green]No eligible running VMs found on source that also exist on destination.[/green]")
        return
    _display_plan_table(plan, source, destination, dry_run=dry_run)
    if not yes and not dry_run:
        if not Confirm.ask("\nProceed with this failover?"):
            console.print("Aborted.")
            return
    _audit("failover_started", {"source": source, "destination": destination,
                                "vm_count": len(plan), "dry_run": dry_run})
    result = _execute_failover_plan(plan, client_source, client_dest, dry_run=dry_run)
    if dry_run:
        _save_plan_from_dry_run(plan, source, destination)
        return
    _print_failover_summary(result)
    _audit("failover_completed", {
            "source": source, "destination": destination,
            "started": len(result["started"]),
            "failed":  len(result["failed_stop"]) + len(result["failed_start"]),
            "rto_seconds": result.get("rto_seconds"),
        })


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

    # 3. Plans coverage
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


# ── failback ──────────────────────────────────────────────────────────────────
@app.command("failback")
def failback(
    source:      Annotated[str, typer.Option(help="Original source cluster (now offline).")],
    destination: Annotated[str, typer.Option(help="Original destination cluster (now active).")],
    namespace:   Annotated[Optional[str], typer.Option("--namespace", "-n", help="Limit scope to a single namespace.")] = None,
):
    """
    Advisory failback guide — shows the path to restore the original primary site.

    After a successful failover the DR cluster is running production workloads.
    This command shows what is running there and prints the exact command to
    return the original primary cluster to its primary role.

    It does NOT start/stop any VMs — it is an advisory and documentation step.
    """
    config = load_config()

    console.print(Panel(
        f"[bold yellow]FAILBACK — Restore Primary Site[/bold yellow]\n\n"
        f"  Current state (post-failover):\n"
        f"    Active site    → [green]{destination}[/green]  (running production workloads)\n"
        f"    Offline site   → [red]{source}[/red]  (needs recovery)\n\n"
        f"  Goal after failback:\n"
        f"    Primary site   → [bold green]{source}[/bold green]  (restored to original role)\n"
        f"    DR site        → [bold cyan]{destination}[/bold cyan]  (standing by)",
        title="Failback",
        border_style="yellow",
    ))

    # Show what is now running on the active (destination) cluster
    _, client_active = get_clients(config, source, destination)
    with console.status(f"[cyan]Fetching VMs from active cluster '{destination}'...[/cyan]"):
        data = client_active.get_vms()

    if not data:
        console.print("[red]Could not retrieve VM data from active cluster.[/red]")
        return

    running = [
        (v["metadata"]["namespace"], v["metadata"]["name"])
        for v in data.get("items", [])
        if v.get("status", {}).get("printableStatus") == "Running"
    ]

    if namespace:
        running = [(ns, name) for ns, name in running if ns == namespace]

    scope_label = f"namespace '{namespace}'" if namespace else "all namespaces"
    t = Table(title=f"Running VMs on '{destination}' ({scope_label})", box=box.ROUNDED)
    t.add_column("Namespace", style="cyan")
    t.add_column("VM Name",   style="magenta")
    for ns, name in sorted(running):
        t.add_row(ns, name)
    console.print(t)

    if namespace:
        failback_cmd = f"harvey failover namespace {namespace} --source {destination} --destination {source}"
    else:
        failback_cmd = f"harvey failover all --source {destination} --destination {source}"

    console.print(f"\n[dim]This command is advisory only — no VMs have been started or stopped.[/dim]")
    console.print(f"\n[bold]Next steps to restore '{source}' as primary:[/bold]")
    console.print(f"  1. Recover the original '{source}' cluster infrastructure.")
    console.print(f"  2. Ensure VMs on '{source}' are in Stopped state.")
    console.print(f"  3. Run the failback — this returns '{source}' to its primary role:")
    console.print(f"\n     [bold cyan]{failback_cmd}[/bold cyan]\n")
    console.print(f"  4. Optionally update your recovery plan source/destination fields.")

    _audit("failback_guidance", {"original_source": source, "active_cluster": destination,
                                   "namespace": namespace, "running_vms": len(running)})


# ── failover subcommands ──────────────────────────────────────────────────────
@failover_app.command("vm")
def failover_vms(
    vm_names:    Annotated[List[str], typer.Argument(help="VM name(s) to fail over.")],
    namespace:   Annotated[str,  typer.Option("--namespace", "-n", help="Namespace of the VMs.")],
    source:      Annotated[str,  typer.Option(help="Source cluster name.")],
    destination: Annotated[str,  typer.Option(help="Destination cluster name.")],
    yes:         Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation.")] = False,
    dry_run:     Annotated[bool, typer.Option("--dry-run", help="Simulate without executing.")] = False,
):
    """Fails over one or more specific VMs by name."""
    config = load_config()
    cs, cd = get_clients(config, source, destination)
    vms_src, vms_dst_keys = _fetch_vm_maps(cs, cd)
    if vms_src is None:
        return
    plan = _build_plan_from_inventory(vms_src, vms_dst_keys,
                                      namespace_filter=namespace,
                                      vm_names_filter=vm_names)
    _ad_hoc_failover(cs, cd, plan, source, destination, yes, dry_run)


@failover_app.command("namespace")
def failover_namespace(
    namespace:   Annotated[str,  typer.Argument(help="Namespace to fail over.")],
    source:      Annotated[str,  typer.Option(help="Source cluster name.")],
    destination: Annotated[str,  typer.Option(help="Destination cluster name.")],
    yes:         Annotated[bool, typer.Option("--yes", "-y")] = False,
    dry_run:     Annotated[bool, typer.Option("--dry-run")] = False,
):
    """Fails over all running VMs in a namespace."""
    config = load_config()
    cs, cd = get_clients(config, source, destination)
    vms_src, vms_dst_keys = _fetch_vm_maps(cs, cd)
    if vms_src is None:
        return
    plan = _build_plan_from_inventory(vms_src, vms_dst_keys, namespace_filter=namespace)
    _ad_hoc_failover(cs, cd, plan, source, destination, yes, dry_run)


@failover_app.command("all")
def failover_all(
    source:      Annotated[str,  typer.Option(help="Source cluster name.")],
    destination: Annotated[str,  typer.Option(help="Destination cluster name.")],
    yes:         Annotated[bool, typer.Option("--yes", "-y")] = False,
    dry_run:     Annotated[bool, typer.Option("--dry-run")] = False,
):
    """Fails over all running VMs across all namespaces."""
    config = load_config()
    cs, cd = get_clients(config, source, destination)
    vms_src, vms_dst_keys = _fetch_vm_maps(cs, cd)
    if vms_src is None:
        return
    plan = _build_plan_from_inventory(vms_src, vms_dst_keys)
    _ad_hoc_failover(cs, cd, plan, source, destination, yes, dry_run)


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
):
    """
    Executes a recovery plan.

    **Modes:**

    - (default) Live failover — stops VMs on source, starts them on destination.
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
    cs, cd = get_clients(config, source, destination)

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
    result = _execute_failover_plan(plan, cs, cd, hooks=hooks, dry_run=dry_run)

    if not dry_run:
        _print_failover_summary(result)
        _audit("plan_run_completed", {
            "plan": name,
            "started": len(result["started"]),
            "failed":  len(result["failed_stop"]) + len(result["failed_start"]),
            "rto_seconds": result.get("rto_seconds"),
        })


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app()
