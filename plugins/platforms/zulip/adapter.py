"""
Zulip Platform Adapter for Hermes Agent.

Connects to a Zulip server via the long-poll event queue API and relays
stream/topic and DM messages to the Hermes agent.

Configuration in config.yaml::

    gateway:
      platforms:
        zulip:
          enabled: true
          extra:
            server_url: https://osmosis.zulipchat.com
            bot_email: bot@osmosis.zulipchat.com
            api_key: ZULIP_API_KEY   # KV key — resolved at config load time
            require_mention: true    # only respond when @mentioned in streams

Or via environment variables (overrides config.yaml):
    ZULIP_SERVER_URL, ZULIP_BOT_EMAIL, ZULIP_API_KEY,
    ZULIP_REQUIRE_MENTION, ZULIP_ALLOWED_USERS, ZULIP_ALLOW_ALL_USERS
"""

import asyncio
import datetime
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform
from gateway.session import SessionSource  # noqa: F401 (used via build_source)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class ZulipAdapter(BasePlatformAdapter):
    """Async Zulip adapter implementing the BasePlatformAdapter interface."""

    def __init__(self, config, **kwargs):
        platform = Platform("zulip")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        self.server_url = (
            os.getenv("ZULIP_SERVER_URL") or extra.get("server_url", "")
        ).rstrip("/")
        self.bot_email = os.getenv("ZULIP_BOT_EMAIL") or extra.get("bot_email", "")
        self.api_key = os.getenv("ZULIP_API_KEY") or extra.get("api_key", "")

        _req_mention_env = os.getenv("ZULIP_REQUIRE_MENTION", "")
        if _req_mention_env:
            self.require_mention = _req_mention_env.lower() not in {"0", "false", "no"}
        else:
            self.require_mention = extra.get("require_mention", True)

        allowed_env = os.getenv("ZULIP_ALLOWED_USERS", "")
        allowed_extra = extra.get("allowed_users", "")
        raw_allowed = allowed_env or (
            ",".join(allowed_extra) if isinstance(allowed_extra, list) else allowed_extra
        )
        self._allowed_users: set = {
            u.strip().lower() for u in raw_allowed.split(",") if u.strip()
        }

        # Runtime state
        self._queue_id: Optional[str] = None
        self._last_event_id: int = -1
        self._poll_task: Optional[asyncio.Task] = None
        self._client = None  # httpx.AsyncClient

        # Per-session context caches (populated on first message, used when sending)
        self._stream_names: Dict[str, str] = {}   # stream_id str → stream name
        self._chat_topics: Dict[str, str] = {}    # chat_id → last seen topic

    @property
    def name(self) -> str:
        return "Zulip"

    def _api(self, path: str) -> str:
        return f"{self.server_url}/api/v1/{path.lstrip('/')}"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        """Register a Zulip event queue and start the long-poll loop."""
        if not self.server_url or not self.bot_email or not self.api_key:
            self._set_fatal_error(
                "config_missing",
                "ZULIP_SERVER_URL, ZULIP_BOT_EMAIL, and ZULIP_API_KEY must be set",
                retryable=False,
            )
            return False

        try:
            import httpx
        except ImportError:
            self._set_fatal_error(
                "missing_dep",
                "httpx is required: pip install httpx",
                retryable=False,
            )
            return False

        self._client = httpx.AsyncClient(
            auth=(self.bot_email, self.api_key),
            timeout=120.0,
        )

        try:
            resp = await self._client.post(
                self._api("register"),
                data={"event_types": '["message"]'},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Zulip: failed to register event queue: %s", e)
            await self._client.aclose()
            self._client = None
            self._set_fatal_error("register_failed", str(e), retryable=True)
            return False

        self._queue_id = data["queue_id"]
        self._last_event_id = data.get("last_event_id", -1)

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        logger.info("Zulip: connected to %s as %s", self.server_url, self.bot_email)
        return True

    async def disconnect(self) -> None:
        """Cancel the poll loop and deregister the event queue."""
        self._mark_disconnected()

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._client and self._queue_id:
            try:
                await self._client.delete(
                    self._api("events"),
                    params={"queue_id": self._queue_id},
                    timeout=10.0,
                )
            except Exception:
                pass

        if self._client:
            await self._client.aclose()
            self._client = None

        self._queue_id = None
        self._last_event_id = -1

    # ── Poll loop ─────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Long-poll the Zulip event queue and dispatch incoming messages."""
        while True:
            try:
                resp = await self._client.get(
                    self._api("events"),
                    params={
                        "queue_id": self._queue_id,
                        "last_event_id": self._last_event_id,
                        "dont_block": "false",
                    },
                    timeout=120.0,
                )

                if resp.status_code == 200:
                    for event in resp.json().get("events", []):
                        self._last_event_id = max(self._last_event_id, event["id"])
                        if event.get("type") == "message":
                            try:
                                await self._handle_event(event)
                            except Exception as e:
                                logger.warning("Zulip: error handling event %s: %s", event.get("id"), e)

                elif resp.status_code == 400:
                    body = resp.json()
                    if body.get("code") == "BAD_EVENT_QUEUE_ID":
                        logger.warning("Zulip: event queue expired, re-registering")
                        await self._reregister_queue()
                    else:
                        logger.warning("Zulip: 400 from events endpoint: %s", body)
                        await asyncio.sleep(5)

                else:
                    logger.warning("Zulip: unexpected status %s from events endpoint", resp.status_code)
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self.is_connected:
                    break
                logger.error("Zulip: poll error: %s", e)
                await asyncio.sleep(5)

    async def _reregister_queue(self) -> None:
        """Re-register the event queue after expiry."""
        backoff = 5
        while True:
            try:
                resp = await self._client.post(
                    self._api("register"),
                    data={"event_types": '["message"]'},
                )
                resp.raise_for_status()
                data = resp.json()
                self._queue_id = data["queue_id"]
                self._last_event_id = data.get("last_event_id", -1)
                logger.info("Zulip: event queue re-registered")
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Zulip: re-register failed: %s — retrying in %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ── Event handling ────────────────────────────────────────────────────

    async def _handle_event(self, event: Dict) -> None:
        """Build a MessageEvent from a Zulip message event and dispatch it."""
        msg = event["message"]
        sender_email = msg.get("sender_email", "")

        if sender_email == self.bot_email:
            return

        is_dm = msg["type"] == "private"

        # Slash commands are self-identifying via "/" — never filter them out
        # for lack of an @mention, even when require_mention is True.
        text_content = msg.get("content", "")
        is_slash_command = text_content.strip().startswith("/")

        if not is_dm and self.require_mention and not is_slash_command:
            flags = event.get("flags", [])
            if "mentioned" not in flags and "wildcard_mentioned" not in flags:
                return

        if self._allowed_users and sender_email.lower() not in self._allowed_users:
            logger.debug("Zulip: ignoring message from unauthorized user %s", sender_email)
            return

        if is_dm:
            chat_id = f"dm_{msg['sender_id']}"
            chat_type = "dm"
            # display_recipient for DMs is a list of recipient dicts
            recipients = msg.get("display_recipient", [])
            chat_name = ", ".join(
                r.get("email", "") for r in recipients if r.get("email") != self.bot_email
            ) if isinstance(recipients, list) else sender_email
        else:
            chat_id = str(msg.get("stream_id", ""))
            chat_type = "group"
            chat_name = msg.get("display_recipient", chat_id)
            self._stream_names[chat_id] = chat_name

        topic = msg.get("subject", "")
        if topic:
            self._chat_topics[chat_id] = topic

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=str(msg.get("sender_id", "")),
            user_name=sender_email,
            user_id_alt=sender_email,  # email for allowlist matching in gateway
            thread_id=topic or None,
        )

        message_event = MessageEvent(
            text=msg["content"],
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(msg["id"]),
            raw_message=event,
            timestamp=datetime.datetime.fromtimestamp(msg.get("timestamp", time.time())),
        )

        await self.handle_message(message_event)

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")

        metadata = metadata or {}

        try:
            if chat_id.startswith("dm_"):
                user_id = chat_id[3:]
                data = {
                    "type": "direct",
                    "to": f"[{user_id}]",
                    "content": content,
                }
            else:
                stream_name = self._stream_names.get(chat_id, chat_id)
                topic = (
                    metadata.get("thread_id")
                    or self._chat_topics.get(chat_id, "general")
                )
                data = {
                    "type": "stream",
                    "to": stream_name,
                    "topic": topic,
                    "content": content,
                }

            resp = await self._client.post(self._api("messages"), data=data)
            resp.raise_for_status()
            return SendResult(success=True, message_id=str(resp.json().get("id", "")))
        except Exception as e:
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a Zulip typing notification."""
        if not self._client:
            return
        try:
            if chat_id.startswith("dm_"):
                user_id = chat_id[3:]
                await self._client.post(
                    self._api("typing"),
                    data={"op": "start", "type": "direct", "to": f"[{user_id}]"},
                    timeout=5.0,
                )
            else:
                stream_name = self._stream_names.get(chat_id, chat_id)
                topic = self._chat_topics.get(chat_id, "general")
                await self._client.post(
                    self._api("typing"),
                    data={
                        "op": "start",
                        "type": "stream",
                        "to": stream_name,
                        "topic": topic,
                    },
                    timeout=5.0,
                )
        except Exception:
            pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        if chat_id.startswith("dm_"):
            return {"name": chat_id, "type": "dm"}
        name = self._stream_names.get(chat_id, chat_id)
        return {"name": name, "type": "group"}


# ---------------------------------------------------------------------------
# Plugin registration helpers
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    return bool(
        os.getenv("ZULIP_SERVER_URL")
        and os.getenv("ZULIP_BOT_EMAIL")
        and os.getenv("ZULIP_API_KEY")
    )


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    server_url = os.getenv("ZULIP_SERVER_URL") or extra.get("server_url", "")
    bot_email = os.getenv("ZULIP_BOT_EMAIL") or extra.get("bot_email", "")
    api_key = os.getenv("ZULIP_API_KEY") or extra.get("api_key", "")
    return bool(server_url and bot_email and api_key)


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env vars during gateway config load."""
    server_url = os.getenv("ZULIP_SERVER_URL", "").strip()
    bot_email = os.getenv("ZULIP_BOT_EMAIL", "").strip()
    api_key = os.getenv("ZULIP_API_KEY", "").strip()
    if not (server_url and bot_email and api_key):
        return None

    seed: dict = {
        "server_url": server_url,
        "bot_email": bot_email,
        "api_key": api_key,
    }

    req_mention = os.getenv("ZULIP_REQUIRE_MENTION", "").strip().lower()
    if req_mention:
        seed["require_mention"] = req_mention not in {"0", "false", "no"}

    allowed = os.getenv("ZULIP_ALLOWED_USERS", "").strip()
    if allowed:
        seed["allowed_users"] = allowed

    home = os.getenv("ZULIP_HOME_CHANNEL", "").strip()
    if home:
        if ":" in home:
            stream, topic = home.split(":", 1)
        else:
            stream, topic = home, "general"
        seed["home_channel"] = {"chat_id": stream, "name": stream, "topic": topic}

    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron delivery (no running adapter required)."""
    import httpx

    extra = getattr(pconfig, "extra", {}) or {}
    server_url = (os.getenv("ZULIP_SERVER_URL") or extra.get("server_url", "")).rstrip("/")
    bot_email = os.getenv("ZULIP_BOT_EMAIL") or extra.get("bot_email", "")
    api_key = os.getenv("ZULIP_API_KEY") or extra.get("api_key", "")

    if not (server_url and bot_email and api_key):
        return {"error": "ZULIP_SERVER_URL, ZULIP_BOT_EMAIL, ZULIP_API_KEY must be set"}

    try:
        if chat_id.startswith("dm_"):
            user_id = chat_id[3:]
            data = {
                "type": "direct",
                "to": f"[{user_id}]",
                "content": message,
            }
        else:
            # chat_id is expected as "stream_name" or "stream_name:topic"
            if ":" in chat_id:
                stream, topic = chat_id.split(":", 1)
            else:
                stream = chat_id
                topic = thread_id or "general"
            data = {
                "type": "stream",
                "to": stream,
                "topic": topic,
                "content": message,
            }

        async with httpx.AsyncClient(
            auth=(bot_email, api_key),
            timeout=30.0,
        ) as client:
            resp = await client.post(f"{server_url}/api/v1/messages", data=data)
            resp.raise_for_status()
            return {"success": True, "message_id": str(resp.json().get("id", ""))}
    except Exception as e:
        return {"error": f"Zulip standalone send failed: {e}"}


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system at startup."""
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=lambda cfg: ZulipAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        env_enablement_fn=_env_enablement,
        required_env=["ZULIP_SERVER_URL", "ZULIP_BOT_EMAIL", "ZULIP_API_KEY"],
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=10000,
        emoji="💬",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Zulip. Zulip supports Markdown formatting. "
            "Messages are organized into streams (channels) with topics (threads). "
            "Keep responses clear and structured."
        ),
    )
