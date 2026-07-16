"""WeClawBot-Bridge WebSocket Channel Adapter for Hermes Gateway.

This adapter makes WeClawBot-Bridge a first-class Hermes message channel.  It
speaks the Bridge's existing WS Remote Agent protocol directly, so no Node SDK
or HTTP API relay is needed:

    WeChat -> Bridge -> this adapter -> Hermes Gateway -> this adapter -> Bridge

The Bridge owns authentication and WeChat delivery. Hermes owns session state,
tools, memory and the agent run.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Optional

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)

DEFAULT_BRIDGE_URL = "wss://railway.122048.xyz/ws/agent"
DEFAULT_AGENT_ID = "h"
RECONNECT_INITIAL_SECONDS = 3.0
RECONNECT_MAX_SECONDS = 60.0
RECV_TIMEOUT_SECONDS = 60.0  # 死连接检测：60s 无数据就发 ping


# ------------------------------------------------------------------
# Bridge Protocol Message Builder
# ------------------------------------------------------------------
#
# Inspired by AgentScope Runtime's ResponseBuilder → MessageBuilder →
# ContentBuilder pattern.  Encapsulates Bridge ws-remote wire format
# so adapter logic never touches raw dicts directly.
#

class BridgeMessageBuilder:
    """Fluent builder for Bridge ws-remote protocol messages."""

    @staticmethod
    def auth(
        token: str,
        agent_id: str,
        name: str = "",
        command: str = "",
        description: str = "",
    ) -> dict[str, Any]:
        return {
            "type": "auth",
            "token": token,
            "agentId": agent_id,
            "name": name,
            "command": command,
            "description": description,
        }

    @staticmethod
    def chat_reply(request_id: str, text: str) -> dict[str, Any]:
        return {"type": "chat", "id": request_id, "text": text}

    @staticmethod
    def error(request_id: str, reason: str) -> dict[str, Any]:
        return {"type": "error", "id": request_id, "reason": reason}

    @staticmethod
    def pong() -> dict[str, Any]:
        return {"type": "pong"}


class WeClawBotAdapter(BasePlatformAdapter):
    """Persistent WS client that bridges WeClawBot messages into Hermes."""

    def __init__(self, config, **_kwargs: Any) -> None:
        super().__init__(config=config, platform=Platform("weclawbot"))
        extra = getattr(config, "extra", {}) or {}
        self.bridge_url = str(os.getenv("WECLAWBOT_BRIDGE_URL") or extra.get("bridge_url") or DEFAULT_BRIDGE_URL)
        self.agent_id = str(os.getenv("WECLAWBOT_AGENT_ID") or extra.get("agent_id") or DEFAULT_AGENT_ID)
        self.token = str(os.getenv("WECLAWBOT_TOKEN") or extra.get("token") or "")
        self.agent_name = str(extra.get("agent_name") or "Hermes Channel Adapter")
        self.command = str(extra.get("command") or "hermes")
        self.description = str(extra.get("description") or "Hermes Gateway Channel Adapter")
        self._ws: Any = None
        self._listener_task: Optional[asyncio.Task] = None
        self._outbound_lock = asyncio.Lock()
        # A Bridge request id is the reply address. The normal Hermes send()
        # callback only receives chat_id, so retain one id per active chat.
        self._request_ids: dict[str, str] = {}

    @property
    def name(self) -> str:
        return "WeClawBot"

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.token:
            self._set_fatal_error("config_missing", "WECLAWBOT_TOKEN is required", retryable=False)
            return False
        if self._listener_task and not self._listener_task.done():
            return True
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_loop())
        # The listener owns reconnects. Wait briefly for its first authenticated
        # connection so gateway startup reflects configuration/auth failures.
        for _ in range(100):
            if self._ws is not None:
                return True
            if self._listener_task.done():
                return False
            await asyncio.sleep(0.05)
        return True

    async def disconnect(self) -> None:
        self._running = False
        task = self._listener_task
        self._listener_task = None
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close(code=1000, reason="Hermes gateway shutdown")
            except Exception:
                pass
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._mark_disconnected()

    async def _listen_loop(self) -> None:
        import websockets
        from websockets.exceptions import ConnectionClosed

        backoff = RECONNECT_INITIAL_SECONDS
        while self._running:
            try:
                async with websockets.connect(
                    self.bridge_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=256 * 1024,
                ) as ws:
                    self._ws = ws
                    await self._send_raw(BridgeMessageBuilder.auth(
                        token=self.token,
                        agent_id=self.agent_id,
                        name=self.agent_name,
                        command=self.command,
                        description=self.description,
                    ))
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    auth = self._decode(raw)
                    if auth.get("type") != "auth_ok":
                        reason = str(auth.get("reason") or "Bridge authentication failed")
                        logger.warning(
                            "WeClawBot: Bridge authentication rejected: %s; retrying in %.0fs",
                            reason, backoff,
                        )
                        # Don't treat auth failure as fatal — Bridge may be in a
                        # transient state (stale connection not yet cleaned up
                        # after restart). Retry with backoff so the adapter
                        # recovers without manual intervention.
                        raise ConnectionError(f"Bridge auth rejected: {reason}")
                    self._mark_connected()
                    backoff = RECONNECT_INITIAL_SECONDS
                    logger.info("WeClawBot: authenticated to Bridge as agent %s", self.agent_id)
                    # 使用显式 recv + timeout 替代 async for，防止 TCP RST
                    # 死连接（Docker 容器强杀）导致 recv 永久阻塞。
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT_SECONDS)
                        except asyncio.TimeoutError:
                            # 60s 无数据 → 发 ping 探测连接是否存活
                            try:
                                pong_waiter = await ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                            except Exception:
                                logger.warning("WeClawBot: ping timeout — connection dead")
                                raise
                            continue
                        await self._handle_inbound(self._decode(raw))
            except asyncio.CancelledError:
                break
            except ConnectionClosed as exc:
                if self._running:
                    logger.warning("WeClawBot: WS closed (%s); reconnecting in %.0fs", exc, backoff)
            except Exception as exc:
                if self._running:
                    logger.warning("WeClawBot: WS connection error (%s); reconnecting in %.0fs", exc, backoff)
            finally:
                self._ws = None
                self._mark_disconnected()
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SECONDS)

    @staticmethod
    def _decode(raw: Any) -> dict[str, Any]:
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    async def _handle_inbound(self, message: dict[str, Any]) -> None:
        kind = message.get("type")
        if kind == "ping":
            await self._send_raw(BridgeMessageBuilder.pong())
            return
        if kind != "chat":
            if kind == "error":
                logger.warning("WeClawBot: Bridge error: %s", message.get("reason", "unknown"))
            return
        request_id = message.get("id")
        payload = message.get("payload")
        if not isinstance(request_id, str) or not isinstance(payload, dict):
            return
        body = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        text = body.get("text")
        if not isinstance(text, str) or not text.strip():
            await self._reply_error(request_id, "Only non-empty text messages are supported")
            return
        # Bridge business semantics are single-user. Do not use untrusted
        # upstream user IDs for Hermes session partitioning.
        chat_id = f"default:{self.agent_id}"
        self._request_ids[chat_id] = request_id
        source = self.build_source(
            chat_id=chat_id,
            chat_name="WeClawBot",
            chat_type="dm",
            user_id="default",
            user_name="default",
            message_id=request_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message={"bridge_request_id": request_id, "payload": payload},
            message_id=request_id,
        )
        await self.handle_message(event)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict] = None) -> SendResult:
        request_id = reply_to or self._request_ids.pop(str(chat_id), None)
        if not request_id:
            return SendResult(success=False, error="No Bridge request id for reply")
        try:
            await self._send_raw(BridgeMessageBuilder.chat_reply(
                request_id=request_id,
                text=content,
            ))
            return SendResult(success=True, message_id=request_id)
        except Exception as exc:
            return SendResult(success=False, error=f"Bridge send failed: {exc}")

    async def send_typing(self, chat_id: str, metadata: Optional[dict] = None) -> None:
        # Bridge protocol intentionally has no typing frame.
        return None

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": "WeClawBot", "type": "dm"}

    async def _reply_error(self, request_id: str, reason: str) -> None:
        try:
            await self._send_raw(BridgeMessageBuilder.error(
                request_id=request_id,
                reason=reason,
            ))
        except Exception:
            logger.debug("WeClawBot: failed to return error to Bridge", exc_info=True)

    async def _send_raw(self, message: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            raise RuntimeError("Bridge WebSocket is not connected")
        async with self._outbound_lock:
            await ws.send(json.dumps(message, ensure_ascii=False))


def check_requirements() -> bool:
    try:
        import websockets  # noqa: F401
    except ImportError:
        return False
    return bool(os.getenv("WECLAWBOT_TOKEN", "").strip())


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(os.getenv("WECLAWBOT_TOKEN", "").strip() or extra.get("token"))


def _env_enablement() -> dict[str, Any] | None:
    token = os.getenv("WECLAWBOT_TOKEN", "").strip()
    if not token:
        return None
    return {
        "token": token,
        "bridge_url": os.getenv("WECLAWBOT_BRIDGE_URL", DEFAULT_BRIDGE_URL).strip(),
        "agent_id": os.getenv("WECLAWBOT_AGENT_ID", DEFAULT_AGENT_ID).strip(),
    }


def register(ctx: Any) -> None:
    ctx.register_platform(
        name="weclawbot",
        label="WeClawBot Bridge",
        adapter_factory=lambda cfg: WeClawBotAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        required_env=["WECLAWBOT_TOKEN"],
        emoji="💬",
        max_message_length=0,
        pii_safe=True,
        platform_hint="You are replying through the WeClawBot WeChat bridge. Use concise Chinese by default.",
    )
