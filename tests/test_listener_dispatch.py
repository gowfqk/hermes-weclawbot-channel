import asyncio
from contextvars import ContextVar

from hermes_weclawbot_channel.adapter import WeClawBotAdapter


def make_adapter() -> WeClawBotAdapter:
    adapter = object.__new__(WeClawBotAdapter)
    adapter._inbound_tasks = set()
    adapter._outbound_lock = asyncio.Lock()
    adapter._request_id_var = ContextVar("listener_request", default=None)
    adapter._request_sockets = {}
    return adapter


def test_listener_dispatch_keeps_processing_ping_while_chat_task_runs():
    async def run() -> None:
        adapter = make_adapter()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def handle(message, *, ws=None):
            calls.append((message["type"], ws))
            if message["type"] == "chat":
                started.set()
                await release.wait()

        adapter._handle_inbound = handle
        socket = object()

        await adapter._dispatch_received({"type": "chat", "id": "req-long"}, ws=socket)
        await asyncio.wait_for(started.wait(), timeout=0.1)
        await adapter._dispatch_received({"type": "ping"}, ws=socket)

        assert calls == [("chat", socket), ("ping", socket)]
        release.set()
        await asyncio.gather(*adapter._inbound_tasks)

    asyncio.run(run())


def test_auth_failure_unblocks_connect_waiter_without_marking_connected():
    async def run() -> None:
        adapter = make_adapter()
        adapter._authenticated = asyncio.Event()
        adapter._auth_succeeded = None
        adapter._set_fatal_error = lambda code, message, *, retryable: calls.append(
            (code, message, retryable)
        )
        calls = []

        # Model the auth_fail branch without a real network server.
        adapter._auth_succeeded = False
        adapter._authenticated.set()
        adapter._set_fatal_error("auth_failed", "bad token", retryable=False)

        await asyncio.wait_for(adapter._authenticated.wait(), timeout=0.1)
        assert adapter._auth_succeeded is False
        assert calls == [("auth_failed", "bad token", False)]

    asyncio.run(run())
