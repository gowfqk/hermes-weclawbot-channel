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
from contextvars import ContextVar
from urllib.parse import urlparse
from typing import Any, Optional

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)

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
    def chat_reply(request_id: str, text: str, final: bool = True) -> dict[str, Any]:
        return {"type": "chat", "id": request_id, "text": text, "final": final}

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
        self.bridge_url = str(os.getenv("WECLAWBOT_BRIDGE_URL") or extra.get("bridge_url") or "").strip()
        self.agent_id = str(os.getenv("WECLAWBOT_AGENT_ID") or extra.get("agent_id") or DEFAULT_AGENT_ID)
        self.token = str(os.getenv("WECLAWBOT_TOKEN") or extra.get("token") or "")
        self.agent_name = str(extra.get("agent_name") or "Hermes Channel Adapter")
        self.command = str(extra.get("command") or "hermes")
        self.description = str(extra.get("description") or "Hermes Gateway Channel Adapter")
        self._ws: Any = None
        self._listener_task: Optional[asyncio.Task] = None
        self._outbound_lock = asyncio.Lock()
        self._authenticated = asyncio.Event()
        self._auth_succeeded: Optional[bool] = None
        self._inbound_tasks: set[asyncio.Task] = set()
        self._request_sockets: dict[str, Any] = {}
        # Each request gets its own task and context. ContextVars propagate to
        # child tasks created by Hermes' streaming pipeline, unlike one mutable
        # chat_id -> request_id entry which would be overwritten by concurrency.
        self._request_id_var: ContextVar[Optional[str]] = ContextVar(
            "weclawbot_request_id", default=None
        )

    @property
    def name(self) -> str:
        return "WeClawBot"

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.token:
            self._set_fatal_error("config_missing", "WECLAWBOT_TOKEN is required", retryable=False)
            return False
        if not _is_valid_bridge_url(self.bridge_url):
            self._set_fatal_error(
                "config_invalid",
                "WECLAWBOT_BRIDGE_URL must be an explicit ws:// or wss:// URL",
                retryable=False,
            )
            return False
        if self._listener_task and not self._listener_task.done():
            return self._authenticated.is_set()

        self._authenticated.clear()
        self._auth_succeeded = None
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_loop())
        # Do not report success just because TCP connected: credentials are sent
        # only after this call and the gateway must wait for an auth result.
        try:
            await asyncio.wait_for(self._authenticated.wait(), timeout=15)
            return self._auth_succeeded is True
        except asyncio.TimeoutError:
            return False

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
        active_tasks = list(self._inbound_tasks)
        for inbound_task in active_tasks:
            inbound_task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        self._inbound_tasks.clear()
        self._authenticated.clear()
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
                    open_timeout=15,   # WebSocket 握手超时，防止 Bridge 半死状态
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
                    if auth.get("type") == "auth_fail":
                        reason = str(auth.get("reason") or "Bridge authentication rejected")
                        self._auth_succeeded = False
                        self._authenticated.set()
                        self._set_fatal_error("auth_failed", reason, retryable=False)
                        logger.error("WeClawBot: Bridge authentication rejected: %s", reason)
                        return
                    if auth.get("type") != "auth_ok":
                        raise ConnectionError("Bridge returned an invalid authentication response")
                    self._mark_connected()
                    self._auth_succeeded = True
                    self._authenticated.set()
                    backoff = RECONNECT_INITIAL_SECONDS
                    logger.info("WeClawBot: authenticated to Bridge as agent %s", self.agent_id)
                    # Keep receiving protocol frames while a Hermes request runs.
                    # Bridge application pings must receive timely pongs even for
                    # long tool calls with no visible intermediate output.
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
                        await self._dispatch_received(self._decode(raw), ws=ws)
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
                # 不在重试循环内调用 _mark_disconnected() — 它会设 _running=False
                # 从而杀死重试。只在真正退出循环时才报告断连。
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SECONDS)
        # 循环退出 = 不再重试
        self._mark_disconnected()

    @staticmethod
    def _decode(raw: Any) -> dict[str, Any]:
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    async def _dispatch_received(self, message: dict[str, Any], *, ws: Any) -> None:
        """Keep frame reception independent from potentially long agent runs."""
        if message.get("type") != "chat":
            await self._handle_inbound(message, ws=ws)
            return
        task = asyncio.create_task(self._handle_inbound(message, ws=ws))
        self._inbound_tasks.add(task)
        task.add_done_callback(self._on_inbound_task_done)

    def _on_inbound_task_done(self, task: asyncio.Task) -> None:
        self._inbound_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("WeClawBot: inbound request processing failed")

    async def _handle_inbound(self, message: dict[str, Any], *, ws: Any = None) -> None:
        kind = message.get("type")
        if kind == "ping":
            await self._send_raw(BridgeMessageBuilder.pong(), ws=ws)
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
            await self._reply_error(
                request_id,
                "Only non-empty text messages are supported",
                ws=ws,
            )
            return
        # Bridge currently normalizes WeChat users to one business identity, but
        # requests can still overlap (for example a long tool run plus a new
        # message). Keep the reply route in task-local context, not a mutable
        # chat_id map that one request can overwrite for another.
        chat_id = f"default:{self.agent_id}"
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
        token = self._request_id_var.set(request_id)
        self._request_sockets[request_id] = ws or self._ws
        try:
            await self.handle_message(event)
        except Exception:
            # This request runs in a background task so failures must be
            # correlated back to Bridge here; otherwise the user only sees a
            # timeout and the listener has no way to identify the request.
            await self._reply_error(request_id, "Hermes processing failed", ws=ws)
            raise
        finally:
            self._request_sockets.pop(request_id, None)
            self._request_id_var.reset(token)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict] = None) -> SendResult:
        # The Bridge keeps the request open for ``final: false`` replies, letting
        # Hermes show tool commentary and then return its terminal answer.
        request_id = reply_to or self._request_id_var.get()
        if not request_id:
            return SendResult(success=False, error="No Bridge request id for reply")
        # Hermes marks terminal stream deliveries with ``notify``.  ``final``
        # is retained for explicit callers; every other send is progress.
        send_metadata = metadata or {}
        is_final = bool(send_metadata.get("final") or send_metadata.get("notify"))
        try:
            await self._send_raw(
                BridgeMessageBuilder.chat_reply(
                    request_id=request_id,
                    text=content,
                    final=is_final,
                ),
                ws=self._request_sockets.get(request_id),
            )
            return SendResult(success=True, message_id=request_id)
        except Exception as exc:
            return SendResult(success=False, error=f"Bridge send failed: {exc}")

    async def send_typing(self, chat_id: str, metadata: Optional[dict] = None) -> None:
        # Bridge protocol intentionally has no typing frame.
        return None

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": "WeClawBot", "type": "dm"}

    async def _reply_error(self, request_id: str, reason: str, *, ws: Any = None) -> None:
        try:
            await self._send_raw(
                BridgeMessageBuilder.error(request_id=request_id, reason=reason),
                ws=ws,
            )
        except Exception:
            logger.debug("WeClawBot: failed to return error to Bridge", exc_info=True)

    async def _send_raw(self, message: dict[str, Any], *, ws: Any = None) -> None:
        target_ws = ws or self._ws
        if target_ws is None:
            raise RuntimeError("Bridge WebSocket is not connected")
        async with self._outbound_lock:
            await target_ws.send(json.dumps(message, ensure_ascii=False))


def _is_valid_bridge_url(value: str) -> bool:
    """Require an explicit WebSocket endpoint before a token is transmitted."""
    normalized = value.strip()
    parsed = urlparse(normalized)
    return (
        parsed.scheme in {"ws", "wss"}
        and bool(parsed.hostname)
        and not parsed.netloc.startswith(("http:", "https:"))
    )


def check_requirements() -> bool:
    """Only dependency discovery belongs here; credentials may live in config.yaml."""
    try:
        import websockets  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    token = str(os.getenv("WECLAWBOT_TOKEN") or extra.get("token") or "").strip()
    bridge_url = str(os.getenv("WECLAWBOT_BRIDGE_URL") or extra.get("bridge_url") or "").strip()
    return bool(token) and _is_valid_bridge_url(bridge_url)


def _env_enablement() -> dict[str, Any] | None:
    token = os.getenv("WECLAWBOT_TOKEN", "").strip()
    bridge_url = os.getenv("WECLAWBOT_BRIDGE_URL", "").strip()
    if not token or not _is_valid_bridge_url(bridge_url):
        return None
    return {
        "token": token,
        "bridge_url": bridge_url,
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
