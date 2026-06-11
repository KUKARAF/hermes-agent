"""online_kv store (kv.osmosis.page) integration via KVClient."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from online_pykv import KVClient, KVError, NotFoundError  # noqa: F401 (re-exported)

try:
    from online_pykv.kv_session import make_kv_auth_error_handler
except ImportError:
    make_kv_auth_error_handler = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)

_CACHE: Dict[str, "_CachedFetch"] = {}


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
    secrets: Dict[str, str] = field(default_factory=dict)
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


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
    "ZULIP_API_KEY",
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


def _make_auth_error_handler(notify_chat_id: Optional[str]) -> Optional[Callable[[], str]]:
    """Return a Zulip-based auth error handler, or None to use the terminal QR flow."""
    if make_kv_auth_error_handler is None:
        return None
    return make_kv_auth_error_handler(
        notify_chat_id=notify_chat_id,
        label="hermes-agent",
    )


def apply_online_kv_secrets(
    *,
    enabled: bool,
    notify_chat_id: Optional[str] = None,
    keys: Optional[List[str]] = None,
    override_existing: bool = False,
    cache_ttl_seconds: float = 300,
) -> FetchResult:
    result = FetchResult()
    if not enabled:
        return result

    secret_keys = keys or _DEFAULT_KEYS

    try:
        on_auth_error = _make_auth_error_handler(notify_chat_id)
        client = KVClient(
            on_auth_error=on_auth_error,
            request_label="hermes-agent",
            request_show_qr=True,
        )
    except KVError as exc:
        result.error = str(exc)
        return result

    for key in secret_keys:
        cached = _CACHE.get(key)
        if cached and cached.is_fresh(cache_ttl_seconds):
            result.secrets[key] = cached.value
            continue
        try:
            value = client.get_or_default(key, default="")
        except KVError as exc:
            result.warnings.append(f"Error fetching {key!r}: {exc}")
            continue
        result.secrets[key] = value
        _CACHE[key] = _CachedFetch(value=value, fetched_at=time.time())

    for key, value in result.secrets.items():
        if not value:
            continue
        if not override_existing and os.environ.get(key):
            result.skipped.append(key)
            continue
        os.environ[key] = value
        result.applied.append(key)

    return result
