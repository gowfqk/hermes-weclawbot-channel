import asyncio
from contextvars import ContextVar

from hermes_weclawbot_channel.adapter import WeClawBotAdapter, _is_valid_bridge_url, validate_config


class Config:
    extra = {"token": "token", "bridge_url": "wss://bridge.example/ws/agent"}


def make_adapter() -> WeClawBotAdapter:
    """Construct only state used by transport-level tests."""
    adapter = object.__new__(WeClawBotAdapter)
    adapter.agent_id = "h"
    adapter._request_id_var = ContextVar("test_request_id", default=None)
    adapter._request_sockets = {}
    adapter._outbound_lock = asyncio.Lock()
    return adapter


async def assert_progress_and_final_replies_share_the_bridge_request_id():
    adapter = make_adapter()
    sent = []

    async def capture(message, *, ws=None):
        sent.append((message, ws))

    socket = object()
    adapter._send_raw = capture
    adapter._request_sockets["req-1"] = socket
    context = adapter._request_id_var.set("req-1")
    try:
        progress = await adapter.send("default:h", "正在调用工具…", metadata={"final": False})
        # Hermes' stream consumer marks a terminal send with notify=True.
        final = await adapter.send("default:h", "最终答案", metadata={"notify": True})
    finally:
        adapter._request_id_var.reset(context)

    assert progress.success and final.success
    assert sent == [
        ({"type": "chat", "id": "req-1", "text": "正在调用工具…", "final": False}, socket),
        ({"type": "chat", "id": "req-1", "text": "最终答案", "final": True}, socket),
    ]


async def assert_concurrent_requests_keep_reply_routes_isolated():
    adapter = make_adapter()
    sent = []

    async def capture(message, *, ws=None):
        await asyncio.sleep(0)
        sent.append((message, ws))

    async def reply(request_id, socket):
        adapter._request_sockets[request_id] = socket
        context = adapter._request_id_var.set(request_id)
        try:
            result = await adapter.send("default:h", request_id, metadata={"notify": True})
            assert result.success
        finally:
            adapter._request_id_var.reset(context)
            adapter._request_sockets.pop(request_id, None)

    adapter._send_raw = capture
    socket_a, socket_b = object(), object()
    await asyncio.gather(reply("req-a", socket_a), reply("req-b", socket_b))

    assert {message["id"]: socket for message, socket in sent} == {
        "req-a": socket_a,
        "req-b": socket_b,
    }


def test_progress_and_final_replies_share_the_bridge_request_id():
    asyncio.run(assert_progress_and_final_replies_share_the_bridge_request_id())


def test_concurrent_requests_keep_reply_routes_isolated():
    asyncio.run(assert_concurrent_requests_keep_reply_routes_isolated())


def test_bridge_url_must_be_explicit_websocket_url(monkeypatch):
    assert _is_valid_bridge_url("wss://bridge.example/ws/agent")
    assert _is_valid_bridge_url("ws://127.0.0.1:3000/ws/agent")
    assert not _is_valid_bridge_url("")
    assert not _is_valid_bridge_url("https://bridge.example/ws/agent")
    assert not _is_valid_bridge_url("wss://https://bridge.example/ws/agent")

    monkeypatch.delenv("WECLAWBOT_TOKEN", raising=False)
    monkeypatch.delenv("WECLAWBOT_BRIDGE_URL", raising=False)
    assert validate_config(Config())
    Config.extra = {"token": "token"}
    assert not validate_config(Config())
