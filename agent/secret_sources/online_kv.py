"""online_kv store (kv.osmosis.page) integration."""

from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_KV_STORE_BASE = "https://kv.osmosis.page"
_KV_HTTP_TIMEOUT = 15

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


def _http_get(url: str, token: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "hermes-agent",
        },
    )
    with urllib.request.urlopen(req, timeout=_KV_HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_kv_secrets(
    *,
    keys: List[str],
    token: str,
    cache_ttl_seconds: float = 300,
    use_cache: bool = True,
) -> Tuple[Dict[str, str], List[str]]:
    if not token:
        raise RuntimeError("KV_TOKEN is empty")
    if not keys:
        return {}, []

    secrets: Dict[str, str] = {}
    warnings: List[str] = []

    for key in keys:
        cache_key = key
        if use_cache:
            cached = _CACHE.get(cache_key)
            if cached and cached.is_fresh(cache_ttl_seconds):
                secrets[key] = cached.value
                continue

        url = f"{_KV_STORE_BASE}/{key.lstrip('/')}"
        try:
            value = _http_get(url, token)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                warnings.append(f"Key {key!r} not found in KV store")
                _CACHE[cache_key] = _CachedFetch(value="", fetched_at=time.time())
                continue
            raise RuntimeError(
                f"HTTP {exc.code} fetching {key!r} from KV store: {exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"Network error fetching {key!r} from KV store: {exc.reason}"
            ) from exc

        secrets[key] = value
        _CACHE[cache_key] = _CachedFetch(value=value, fetched_at=time.time())

    return secrets, warnings


def apply_online_kv_secrets(
    *,
    enabled: bool,
    token_env: str = "KV_TOKEN",
    keys: Optional[List[str]] = None,
    override_existing: bool = False,
    cache_ttl_seconds: float = 300,
) -> FetchResult:
    result = FetchResult()
    if not enabled:
        return result

    token = os.environ.get(token_env, "").strip()
    if not token:
        result.error = (
            f"secrets.online_kv.enabled is true but {token_env} is "
            "not set.  Set KV_TOKEN in your .env file."
        )
        return result

    secret_keys = keys or [
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
        secrets, warnings = fetch_kv_secrets(
            keys=secret_keys,
            token=token,
            cache_ttl_seconds=cache_ttl_seconds,
        )
    except RuntimeError as exc:
        result.error = str(exc)
        return result

    result.secrets = secrets
    result.warnings.extend(warnings)

    for key, value in secrets.items():
        if not value:
            continue
        if not override_existing and os.environ.get(key):
            result.skipped.append(key)
            continue
        os.environ[key] = value
        result.applied.append(key)

    return result