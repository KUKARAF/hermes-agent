"""CLI handlers for ``hermes secrets online_kv ...``.

Subcommands:
    setup    — enable online_kv and explain how session tokens work
    status   — show current config + session token state
    sync     — run a fetch right now and show what would be applied (dry-run friendly)
    disable  — flip ``secrets.online_kv.enabled`` to False
"""

from __future__ import annotations

import argparse
import os
import tomllib
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from agent.secret_sources.online_kv import _DEFAULT_KEYS, apply_online_kv_secrets
from hermes_cli.config import load_config, save_config


# ---------------------------------------------------------------------------
# Argparse wiring — called from hermes_cli.main
# ---------------------------------------------------------------------------


def register_cli(parent_parser: argparse.ArgumentParser) -> None:
    """Attach the ``online_kv`` subcommand tree to a parent parser."""
    sub = parent_parser.add_subparsers(dest="secrets_okv_command")

    setup = sub.add_parser("setup", help="Enable online_kv secret fetching")
    setup.set_defaults(func=cmd_setup)

    status = sub.add_parser("status", help="Show config + session token state")
    status.set_defaults(func=cmd_status)

    sync = sub.add_parser("sync", help="Fetch secrets now and report what changed")
    sync.add_argument(
        "--apply",
        action="store_true",
        help="Actually export secrets into the current process env (default: dry-run)",
    )
    sync.set_defaults(func=cmd_sync)

    disable = sub.add_parser("disable", help="Turn off the online_kv integration")
    disable.set_defaults(func=cmd_disable)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def cmd_setup(args: argparse.Namespace) -> int:
    console = Console()
    console.print(
        Panel.fit(
            "[bold]online_kv store setup[/bold]\n\n"
            "Secrets are fetched from kv.osmosis.page on startup.\n"
            "The session token is managed automatically and stored in\n"
            "[cyan]~/.config/kv/config.toml[/cyan].\n\n"
            "On first run (no saved token), an approval link is sent via Zulip\n"
            "(requires [cyan]ZULIP_API_KEY[/cyan] in [cyan]~/.hermes/.env[/cyan]).",
            border_style="cyan",
        )
    )

    cfg = load_config()
    secrets_cfg = cfg.setdefault("secrets", {}).setdefault("online_kv", {})
    secrets_cfg["enabled"] = True
    save_config(cfg)

    console.print("\n[green]✓ online_kv enabled.[/green]")
    console.print(
        "  Status:  [cyan]hermes secrets online_kv status[/cyan]\n"
        "  Refresh: [cyan]hermes secrets online_kv sync[/cyan]\n"
        "  Disable: [cyan]hermes secrets online_kv disable[/cyan]"
    )
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    console = Console()
    cfg = load_config()
    okv_cfg = (cfg.get("secrets") or {}).get("online_kv") or {}

    enabled = bool(okv_cfg.get("enabled"))

    token_path = Path("~/.config/kv/config.toml").expanduser()
    session_token_saved = False
    if token_path.exists():
        try:
            with open(token_path, "rb") as f:
                kv_cfg = tomllib.load(f)
            session_token_saved = bool(kv_cfg.get("session_token"))
        except (OSError, ValueError):
            pass

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("", style="bold")
    table.add_column("")
    table.add_row("Enabled", _yn(enabled))
    table.add_row("Session token saved", _yn(session_token_saved))
    table.add_row("Token path", str(token_path))
    table.add_row("Override existing", _yn(bool(okv_cfg.get("override_existing", False))))
    table.add_row("Cache TTL (s)", str(okv_cfg.get("cache_ttl_seconds", 300)))
    notify = okv_cfg.get("notify_chat_id") or os.getenv("KV_SESSION_NOTIFY_CHAT") or os.getenv("ZULIP_HOME_CHANNEL") or "(not set)"
    table.add_row("Notify chat ID", notify)

    console.print(Panel(table, title="online_kv store", border_style="cyan"))

    if not enabled:
        console.print("\n  Run [cyan]hermes secrets online_kv setup[/cyan] to enable.")
    elif not session_token_saved:
        console.print(
            "\n  [yellow]No saved session token — will be requested on next startup.[/yellow]"
        )
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    console = Console()
    cfg = load_config()
    okv_cfg = (cfg.get("secrets") or {}).get("online_kv") or {}
    if not okv_cfg.get("enabled"):
        console.print(
            "[yellow]online_kv integration is disabled.  Run "
            "`hermes secrets online_kv setup` first.[/yellow]"
        )
        return 1

    console.print("[bold]Fetching secrets from online_kv…[/bold]")
    result = apply_online_kv_secrets(
        enabled=True,
        notify_chat_id=okv_cfg.get("notify_chat_id"),
        keys=okv_cfg.get("keys"),
        override_existing=True,
        cache_ttl_seconds=0,
    )

    if result.error:
        console.print(f"[red]Fetch failed: {result.error}[/red]")
        return 1

    secret_keys = okv_cfg.get("keys") or _DEFAULT_KEYS
    override = bool(okv_cfg.get("override_existing", False)) or args.apply
    table = Table(show_header=True, header_style="bold")
    table.add_column("Name", style="cyan")
    table.add_column("Action")
    applied = 0
    for key in sorted(secret_keys):
        value = result.secrets.get(key, "")
        if not value:
            table.add_row(key, "[dim]skip (empty / not in store)[/dim]")
            continue
        already = bool(os.environ.get(key))
        if already and not override:
            table.add_row(key, "[dim]skip (already set)[/dim]")
            continue
        if args.apply:
            os.environ[key] = value
            applied += 1
            table.add_row(key, "[green]exported[/green]" + (" (overrode)" if already else ""))
        else:
            table.add_row(key, "[green]would export[/green]" + (" (overrides)" if already else ""))

    console.print(table)
    for w in result.warnings:
        console.print(f"[yellow]warning:[/yellow] {w}")

    if not args.apply:
        console.print(
            "\n  Dry-run — secrets are applied automatically on the next "
            "[cyan]hermes[/cyan] invocation.  Re-run with [cyan]--apply[/cyan] "
            "to export into the current shell instead."
        )
    else:
        console.print(f"\n  [green]Exported {applied} secret(s) into current process.[/green]")
    return 0


def cmd_disable(args: argparse.Namespace) -> int:
    console = Console()
    cfg = load_config()
    okv_cfg = cfg.setdefault("secrets", {}).setdefault("online_kv", {})
    okv_cfg["enabled"] = False
    save_config(cfg)
    console.print(
        "[green]Disabled.[/green]  online_kv secrets will NOT be pulled on the next "
        "Hermes invocation."
    )
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _yn(b: bool) -> str:
    return "[green]yes[/green]" if b else "[dim]no[/dim]"
