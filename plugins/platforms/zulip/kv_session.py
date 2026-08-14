"""
KV Manager session request via Zulip notification.

When hermes-agent needs a KV session token, this module creates an approval
request, sends the clickable URL to the admin via Zulip, and polls until the
request is approved (or times out).

Usage::

    from plugins.platforms.zulip.kv_session import request_kv_session_via_zulip

    token = await request_kv_session_via_zulip(notify_chat_id="dm_12345")

Required Zulip env vars (already expected by the Zulip adapter):
    ZULIP_SERVER_URL, ZULIP_BOT_EMAIL, ZULIP_API_KEY

Optional env var:
    KV_SESSION_NOTIFY_CHAT  — where to deliver the approval link.
                              Format: ``dm_<user_id>``  or  ``stream_name:topic``
                              Falls back to ZULIP_HOME_CHANNEL if not set.
"""

import asyncio
import concurrent.futures
import logging
import os
from typing import Optional

import httpx
from online_pykv import initiate_session_request, await_session_approval

logger = logging.getLogger(__name__)


async def request_kv_session_via_zulip(
    *,
    notify_chat_id: Optional[str] = None,
    label: str = "hermes-agent",
    kv_base_url: Optional[str] = None,
    zulip_server_url: Optional[str] = None,
    zulip_bot_email: Optional[str] = None,
    zulip_api_key: Optional[str] = None,
    poll_interval: float = 5.0,
    timeout: float = 900.0,
    save_to_config: bool = True,
) -> str:
    """Request a KV Manager session token, notifying the admin via Zulip.

    Creates the approval request, sends a Zulip message with the clickable
    approval URL, then polls until approved, rejected, or timed out.

    Returns the session token string on success.
    Raises ``online_pykv.KVError`` if rejected or timed out.

    Parameters
    ----------
    notify_chat_id:
        Zulip destination for the approval link.  Overrides
        ``KV_SESSION_NOTIFY_CHAT`` / ``ZULIP_HOME_CHANNEL`` env vars.
        Format: ``"dm_<user_id>"`` or ``"stream_name:topic"``.
    label:
        Human-readable label shown on the approval page (default: "hermes-agent").
    kv_base_url:
        KV Manager base URL.  Defaults to ``~/.config/kv/config.toml`` value.
    zulip_server_url / zulip_bot_email / zulip_api_key:
        Override the matching ZULIP_* env vars.
    poll_interval:
        Seconds between status polls (default: 5).
    timeout:
        Total seconds to wait for approval (default: 900 = 15 min).
    save_to_config:
        Write the token to ``~/.config/kv/config.toml`` on success (default: True).
    """
    # ── Resolve Zulip credentials ────────────────────────────────────────────
    # Prefer ZULIP_SITE_URL (the key online_kv provides and the adapter uses);
    # fall back to ZULIP_SERVER_URL for older setups. Using only the latter meant
    # an all-KV deployment (ZULIP_SITE_URL) could not deliver the approval link,
    # so session renewals stalled silently.
    server_url = (
        zulip_server_url
        or os.getenv("ZULIP_SITE_URL", "")
        or os.getenv("ZULIP_SERVER_URL", "")
    ).rstrip("/")
    bot_email = zulip_bot_email or os.getenv("ZULIP_BOT_EMAIL", "")
    api_key = zulip_api_key or os.getenv("ZULIP_API_KEY", "")

    # ── Resolve notification destination ────────────────────────────────────
    chat_id = (
        notify_chat_id
        or os.getenv("KV_SESSION_NOTIFY_CHAT", "")
        or os.getenv("ZULIP_HOME_CHANNEL", "")
    ).strip()

    # ── Create the session request (public endpoint, no KV auth needed) ─────
    result = initiate_session_request(label=label, base_url=kv_base_url, show_qr=False)
    request_id = result["id"]
    approval_url = result["url"]
    expires_at = result["expires_at"]

    # ── Notify via Zulip ─────────────────────────────────────────────────────
    if server_url and bot_email and api_key and chat_id:
        message = (
            f"**KV session request** from `{label}`\n\n"
            f"Approve or reject: {approval_url}\n\n"
            f"Expires: `{expires_at}`"
        )
        await _zulip_send(server_url, bot_email, api_key, chat_id, message)
        logger.info("KV session approval link sent to Zulip chat %s", chat_id)
    else:
        missing = []
        if not server_url:
            missing.append("ZULIP_SITE_URL")
        if not bot_email:
            missing.append("ZULIP_BOT_EMAIL")
        if not api_key:
            missing.append("ZULIP_API_KEY")
        if not chat_id:
            missing.append("KV_SESSION_NOTIFY_CHAT")
        logger.warning(
            "Cannot send KV session request via Zulip (missing: %s). "
            "Approve manually at: %s",
            ", ".join(missing),
            approval_url,
        )

    # ── Poll for approval (blocking, run in executor to stay async-friendly) ─
    loop = asyncio.get_event_loop()
    token = await loop.run_in_executor(
        None,
        lambda: await_session_approval(
            request_id,
            base_url=kv_base_url,
            poll_interval=poll_interval,
            timeout=timeout,
            save_to_config=save_to_config,
            poll_secret=result.get("poll_secret"),
        ),
    )
    return token


def make_kv_auth_error_handler(
    *,
    notify_chat_id: Optional[str] = None,
    label: str = "hermes-agent",
    kv_base_url: Optional[str] = None,
    zulip_server_url: Optional[str] = None,
    zulip_bot_email: Optional[str] = None,
    zulip_api_key: Optional[str] = None,
    poll_interval: float = 5.0,
    timeout: float = 900.0,
):
    """Return a sync callable suitable for ``KVClient(on_auth_error=...)``.

    The returned function runs ``request_kv_session_via_zulip`` in a fresh
    event loop (via ``asyncio.run``), which works whether or not the caller
    is already inside a running loop — it always uses a dedicated thread so
    it never blocks the caller's loop.

    Example::

        from plugins.platforms.zulip.kv_session import make_kv_auth_error_handler
        from online_pykv import KVClient

        client = KVClient(
            on_auth_error=make_kv_auth_error_handler(label="hermes-agent"),
            request_label="hermes-agent",
        )
        value = client.get("myapp/config")  # auto-requests session if needed
    """
    kwargs = dict(
        notify_chat_id=notify_chat_id,
        label=label,
        kv_base_url=kv_base_url,
        zulip_server_url=zulip_server_url,
        zulip_bot_email=zulip_bot_email,
        zulip_api_key=zulip_api_key,
        poll_interval=poll_interval,
        timeout=timeout,
    )

    def _refresh() -> str:
        # Always run in a dedicated thread with its own event loop so this
        # callable is safe whether invoked from sync or async context.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, request_kv_session_via_zulip(**kwargs))
            return future.result()

    return _refresh


async def _zulip_send(
    server_url: str,
    bot_email: str,
    api_key: str,
    chat_id: str,
    message: str,
) -> None:
    """Send a single Zulip message without a running adapter."""
    if chat_id.startswith("dm_"):
        user_id = chat_id[3:]
        data: dict = {"type": "direct", "to": f"[{user_id}]", "content": message}
    elif ":" in chat_id:
        stream, topic = chat_id.split(":", 1)
        data = {"type": "stream", "to": stream, "topic": topic, "content": message}
    else:
        data = {"type": "stream", "to": chat_id, "topic": "kv-session", "content": message}

    try:
        async with httpx.AsyncClient(auth=(bot_email, api_key), timeout=15.0) as client:
            resp = await client.post(f"{server_url}/api/v1/messages", data=data)
            resp.raise_for_status()
    except (httpx.HTTPError, httpx.RequestError, OSError) as exc:
        logger.warning("Failed to send KV session approval link via Zulip: %s", exc)
