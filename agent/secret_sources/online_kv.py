"""online_kv store (kv.osmosis.page) integration.

Auth model
---------
ZULIP_API_KEY   Required.  Pre-provisioned in ~/.hermes/.env.  Used by the
                session-approval handler to send the approval link via Zulip.

KV_API_KEY      Optional.  Scoped API key with limited access.  If present in
                ~/.config/kv/config.toml it is used for direct API access.

KV_SESSION_TOKEN / session_token in config.toml
                Session token obtained via the Zulip approval link
                (tailscale-style confirmation).  Obtained automatically on
                first run if no saved token exists; a link is sent to the
                configured Zulip chat.

Startup flow
-----------
1. Check ZULIP_API_KEY is present — if not, fail fast with a clear error.
2. Load config.toml for any saved session_token and KV_API_KEY.
3. If no session_token:
   - If on_auth_error handler is provided (Zulip flow), call it to get a token.
   - Otherwise raise AuthError immediately.
4. KVClient uses the session token for all reads.
5. Per-key failures (404, network) are collected as warnings and do not block
   startup.  Only a missing session token with no recovery path is fatal.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

from online_pykv import KVClient, KVError  # noqa: F401 (re-exported for callers)

logger = logging.getLogger(__name__)

try:
    from plugins.platforms.zulip.kv_session import make_kv_auth_error_handler
except ImportError:
    make_kv_auth_error_handler = None  # type: ignore[assignment, misc]

# In-memory cache of fetched secrets, keyed by name.
_CACHE: Dict[str, "_CachedFetch"] = {}


# ----------------------------------------------------------------------------
# Data types
# ----------------------------------------------------------------------------


@dataclass
class _CachedFetch:
    value: str
    fetched_at: float

    def is_fresh(self, ttl_seconds: float) -> bool:
        if ttl_seconds <= 0:
            return False
        return (time.time() - self.fetched_at) < ttl_seconds


@dataclass
class FetchResult:
    """Result of a secret-fetch pass."""

    secrets: Dict[str, str] = field(default_factory=dict)
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


# ----------------------------------------------------------------------------
# Auth error handler (Zulip approval link flow)
# ----------------------------------------------------------------------------


def _build_auth_error_handler(
    notify_chat_id: Optional[str],
    label: str,
) -> Optional[Callable[[], str]]:
    """Return a callable that requests a new session token via Zulip approval.

    Requires ZULIP_API_KEY to be present in the environment — this is the
    only credential that MUST be pre-provisioned.  If the import fails or the
    env var is missing, returns None so the caller can decide how to handle
    the error (fail fast vs fall back to terminal flow).
    """
    if make_kv_auth_error_handler is None:
        logger.debug("kv_session unavailable — Zulip auth handler not loaded")
        return None

    zulip_api_key = os.getenv("ZULIP_API_KEY", "").strip()
    if not zulip_api_key:
        # ZULIP_API_KEY is not set — the caller should treat this as fatal.
        return None

    return make_kv_auth_error_handler(
        notify_chat_id=notify_chat_id,
        label=label,
        zulip_api_key=zulip_api_key,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def apply_online_kv_secrets(
    *,
    enabled: bool,
    notify_chat_id: Optional[str] = None,
    keys: Optional[List[str]] = None,
    override_existing: bool = False,
    cache_ttl_seconds: float = 300,
) -> FetchResult:
    """Fetch secrets from kv.osmosis.page into os.environ.

    Parameters
    ----------
    enabled:
        If False, this function is a no-op and returns an empty result.
    notify_chat_id:
        Zulip destination for the session-approval link when no saved token
        exists (e.g. ``"dm_12345"`` or ``"hermes:kv-session"``).
    keys:
        List of secret names to fetch.  Defaults to all known keys.
    override_existing:
        If True, overwrite env vars that are already set.  If False (default),
        existing env vars are left alone and KV values are skipped.
    cache_ttl_seconds:
        How long a cached value is considered fresh before re-fetching.
        Defaults to 300 s.  Pass 0 to always re-fetch.
    """
    result = FetchResult()
    if not enabled:
        return result

    secret_keys = keys or _DEFAULT_KEYS

    # Build the auth error handler for the Zulip approval-link flow.  It only
    # works when ZULIP_API_KEY is already in the environment (needed to send
    # the link via Zulip); otherwise this returns None and online_pykv falls
    # back to printing the approval URL to stderr / the dashboard.
    #
    # We deliberately do NOT hard-require ZULIP_API_KEY here.  A valid saved
    # session token (~/.config/kv/config.toml) authenticates KV without any
    # approval step, so requiring the key up front would break the common
    # "all secrets in KV, empty .env" deployment (the key itself lives in KV).
    # If there is no token AND no handler, KVClient() raises AuthError — a
    # KVError subclass caught below — which degrades to a warning rather than
    # blocking startup.
    on_auth_error: Optional[Callable[[], str]] = _build_auth_error_handler(
        notify_chat_id=notify_chat_id,
        label="hermes-agent",
    )

    # OSError covers raw network failures (urllib.error.URLError et al.) that
    # online_pykv does not wrap in KVError; ValueError covers a malformed
    # ~/.config/kv/config.toml (TOMLDecodeError).  Secret sources must never
    # block startup — an unreachable KV host degrades to warnings.
    try:
        client = KVClient(
            on_auth_error=on_auth_error,
            request_label="hermes-agent",
            request_show_qr=False,  # URL goes to Zulip, not terminal
        )
    except (KVError, OSError, ValueError) as exc:
        result.error = str(exc)
        return result

    # Fetch each key.
    for key in secret_keys:
        cached = _CACHE.get(key)
        if cached and cached.is_fresh(cache_ttl_seconds):
            result.secrets[key] = cached.value
            continue
        try:
            value = client.get_or_default(key, default="")
        except (KVError, OSError, ValueError) as exc:
            result.warnings.append(f"Error fetching {key!r}: {exc}")
            continue
        result.secrets[key] = value
        _CACHE[key] = _CachedFetch(value=value, fetched_at=time.time())

    # Apply fetched values into os.environ.
    for key, value in result.secrets.items():
        if not value:
            continue
        if not override_existing and os.environ.get(key):
            result.skipped.append(key)
            continue
        os.environ[key] = value
        result.applied.append(key)

    return result


# ----------------------------------------------------------------------------
# Default key list
# ----------------------------------------------------------------------------

_DEFAULT_KEYS: List[str] = [
    # Messaging / communication platforms
    "HASS_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_BOT_TOKEN",
    "SLACK_BOT_TOKEN",
    "SLACK_APP_TOKEN",
    "WHATSAPP_BOT_TOKEN",
    "MATTERMOST_TOKEN",
    "MATRIX_ACCESS_TOKEN",
    "SIGNAL_TOKEN",
    "WECOM_WEBHOOK_SECRET",
    "DINGTALK_APP_SECRET",
    "FEISHU_APP_SECRET",
    "QQBOT_SECRET",
    "WEIXIN_TOKEN",
    "BLUEBUBBLES_PASSWORD",
    # Dev / VCS
    "GITHUB_TOKEN",
    "GITLAB_TOKEN",
    "BITBUCKET_APP_PASSWORD",
    # LLM providers
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY",
    "ELEVENLABS_API_KEY",
    "MINIMAX_API_KEY",
    "MISTRAL_API_KEY",
    "XAI_API_KEY",
    "NOVITA_API_KEY",
    "LM_API_KEY",
    "AI_GATEWAY_API_KEY",
    "AZURE_ANTHROPIC_KEY",
    "AZURE_FOUNDRY_API_KEY",
    "OPENROUTER_API_KEY",
    "CUSTOM_API_KEY",
    # Search / browser tools
    "FIRECRAWL_API_KEY",
    "EXA_API_KEY",
    "TAVILY_API_KEY",
    "BROWSERBASE_API_KEY",
    "BROWSERBASE_PROJECT_ID",
    "BROWSER_USE_API_KEY",
    "DAYTONA_API_KEY",
    # Cloud / infra
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AZURE_CLIENT_SECRET",
]