"""CLI handlers for ``hermes secrets online_kv ...``.

Subcommands:
    setup    — interactive wizard: prompt for token, test fetch
    status   — show current config + last fetch outcome
    sync     — run a fetch right now and show what would be applied (dry-run friendly)
    disable  — flip ``secrets.online_kv.enabled`` to False
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from agent.secret_sources import online_kv as okv
from hermes_cli.config import (
    get_env_path,
    load_config,
    save_config,
    save_env_value,
)


# ---------------------------------------------------------------------------
# Argparse wiring — called from hermes_cli.main
# ---------------------------------------------------------------------------


def register_cli(parent_parser: argparse.ArgumentParser) -> None:
    """Attach the ``online_kv`` subcommand tree to a parent parser.

    Called from ``hermes_cli.main`` as part of building the top-level
    ``hermes secrets`` parser.
    """
    sub = parent_parser.add_subparsers(dest="secrets_okv_command")

    setup = sub.add_parser(
        "setup",
        help="Interactive wizard: store token, test fetch",
    )
    setup.add_argument(
        "--token",
        help="Provide the KV token non-interactively (will be stored in .env)",
    )
    setup.set_defaults(func=cmd_setup)

    status = sub.add_parser("status", help="Show config + last fetch")
    status.set_defaults(func=cmd_status)

    sync = sub.add_parser("sync", help="Fetch secrets now and report what changed")
    sync.add_argument(
        "--apply",
        action="store_true",
        help="Actually export the secrets into the current shell's env (default: dry-run)",
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
            "online_kv is a simple key-value store at kv.osmosis.page.\n"
            "Set the KV_TOKEN in your .env file to get started.",
            border_style="cyan",
        )
    )

    # ------------------------------------------------------------------- token
    console.print()
    console.print("[bold]Step 1[/bold]  Provide your KV token")
    cfg = load_config()
    secrets_cfg = (cfg.setdefault("secrets", {})
                     .setdefault("online_kv", {}))
    token_env = secrets_cfg.get("token_env", "KV_TOKEN")

    token = (args.token or "").strip()
    if not token:
        token = getpass.getpass(f"  Paste KV token ({token_env}): ").strip()
    if not token:
        console.print("  [red]Empty token, aborting.[/red]")
        return 1

    save_env_value(token_env, token)
    os.environ[token_env] = token  # so the test fetch below sees it
    console.print(f"  [green]✓[/green] stored in {get_env_path()} as {token_env}")

    # ------------------------------------------------------------------- test
    console.print()
    console.print("[bold]Step 2[/bold]  Test fetch")
    try:
        secrets, warnings = okv.fetch_kv_secrets(
            keys=["KV_TOKEN"],  # just test that we can reach the store
            token=token,
            use_cache=False,
        )
    except Exception as exc:  # noqa: BLE001
        console.print(f"  [red]✗ Fetch failed: {exc}[/red]")
        return 1

    console.print("  [green]✓[/green] Connection successful")

    # ------------------------------------------------------------------- save
    secrets_cfg["enabled"] = True
    secrets_cfg.setdefault("token_env", token_env)
    secrets_cfg.setdefault("cache_ttl_seconds", 300)
    secrets_cfg.setdefault("override_existing", False)
    save_config(cfg)

    console.print()
    console.print(
        "[green]✓ online_kv is enabled.[/green]  "
        "Secrets will be pulled at the start of every Hermes process."
    )
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
    token_env = okv_cfg.get("token_env", "KV_TOKEN")
    token_set = bool(os.environ.get(token_env))

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("", style="bold")
    table.add_column("")
    table.add_row("Enabled",         _yn(enabled))
    table.add_row("Token env var",   token_env)
    table.add_row("Token in env",    _yn(token_set))
    table.add_row("Override existing", _yn(bool(okv_cfg.get("override_existing", False))))
    table.add_row("Cache TTL (s)",   str(okv_cfg.get("cache_ttl_seconds", 300)))

    console.print(Panel(table, title="online_kv store", border_style="cyan"))

    if not enabled:
        console.print("\n  Run [cyan]hermes secrets online_kv setup[/cyan] to enable.")
        return 0
    if not token_set:
        console.print(
            f"\n  [yellow]Enabled but {token_env} is not set — Hermes will skip "
            "and warn on next startup.[/yellow]"
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

    token_env = okv_cfg.get("token_env", "KV_TOKEN")
    token = os.environ.get(token_env, "").strip()
    if not token:
        console.print(f"[red]{token_env} is not set.[/red]")
        return 1

    # Get the full list of keys that online_kv would try to fetch
    secret_keys = okv_cfg.get("keys") or [
        "HASS_TOKEN",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_BOT_TOKEN",
        "SLACK_BOT_TOKEN",
        "WHATSAPP_BOT_TOKEN",
        "MATTERMOST_TOKEN",
        "MATRIX_ACCESS_TOKEN",
        "SIGNAL_TOKEN",
        "ZULIP_API_KEY",
        "WECOM_WEBHOOK_SECRET",
        "DINGTALK_APP_SECRET",
        "FEISHU_APP_SECRET",
        "QQBOT_SECRET",
        "WEIXIN_TOKEN",
        "BLUEBUBBLES_PASSWORD",
        "GITHUB_TOKEN",
        "GITLAB_TOKEN",
        "BITBUCKET_APP_PASSWORD",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "ELEVENLABS_API_KEY",
        "MINIMAX_API_KEY",
        "MISTRAL_API_KEY",
        "FIRECRAWL_API_KEY",
        "FIRECRAWL_API_URL",
        "EXA_API_KEY",
        "TAVILY_API_KEY",
        "BROWSERBASE_API_KEY",
        "BROWSERBASE_PROJECT_ID",
        "BROWSER_USE_API_KEY",
        "BWS_ACCESS_TOKEN",
    ]

    try:
        secrets, warnings = okv.fetch_kv_secrets(
            keys=secret_keys,
            token=token,
            use_cache=False,
        )
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Fetch failed: {exc}[/red]")
        return 1

    if not secrets:
        console.print("[yellow]No secrets fetched.[/yellow]")
        return 0

    override = bool(okv_cfg.get("override_existing", False)) or args.apply
    table = Table(show_header=True, header_style="bold")
    table.add_column("Name", style="cyan")
    table.add_column("Action")
    applied = 0
    for key in sorted(secrets):
        if key == token_env:
            table.add_row(key, "[dim]skip (bootstrap token)[/dim]")
            continue
        if not secrets[key]:
            table.add_row(key, "[dim]skip (empty)[/dim]")
            continue
        already = bool(os.environ.get(key))
        if already and not override:
            table.add_row(key, "[dim]skip (already set)[/dim]")
            continue
        if args.apply:
            os.environ[key] = secrets[key]
            applied += 1
            table.add_row(key, "[green]exported[/green]" + (" (overrode)" if already else ""))
        else:
            table.add_row(key, "[green]would export[/green]" + (" (overrides)" if already else ""))

    console.print(table)
    for w in warnings:
        console.print(f"[yellow]warning:[/yellow] {w}")

    if not args.apply:
        console.print(
            "\n  This was a dry-run — secrets are picked up automatically on the "
            "next [cyan]hermes[/cyan] invocation.  Re-run with [cyan]--apply[/cyan] "
            "to export into the current shell instead."
        )
    else:
        console.print(f"\n  [green]Exported {applied} secret(s) into current process.[/green]")
    return 0


def cmd_disable(args: argparse.Namespace) -> int:
    console = Console()
    cfg = load_config()
    okv_cfg = (cfg.setdefault("secrets", {})
                .setdefault("online_kv", {}))
    okv_cfg["enabled"] = False
    save_config(cfg)
    console.print(
        "[green]Disabled.[/green]  online_kv secrets will NOT be pulled on the next "
        "Hermes invocation.\n"
        "  Your token is left in .env — remove it manually if you also want "
        "to revoke the credential."
    )
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _yn(b: bool) -> str:
    return "[green]yes[/green]" if b else "[dim]no[/dim]"