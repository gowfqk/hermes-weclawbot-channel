import asyncio

from hermes_weclawbot_channel.src.adapter import WeClawBotAdapter


def make_adapter() -> WeClawBotAdapter:
    """Construct only state used by standalone reply-routing tests."""
    adapter = object.__new__(WeClawBotAdapter)
    adapter.agent_id = "h"
    adapter._request_ids = {}
    adapter._outbound_lock = asyncio.Lock()
    return adapter


async def assert_progress_and_final_replies_share_the_bridge_request_id():
    adapter = make_adapter()
    sent = []

    async def capture(message):
        sent.append(message)

    adapter._send_raw = capture
    adapter._request_ids["default:h"] = "req-1"

    progress = await adapter.send("default:h", "正在调用工具…", metadata={"final": False})
    # Hermes' stream consumer marks a terminal send with notify=True.
    final = await adapter.send("default:h", "最终答案", metadata={"notify": True})

    assert progress.success and final.success
    assert sent == [
        {"type": "chat", "id": "req-1", "text": "正在调用工具…", "final": False},
        {"type": "chat", "id": "req-1", "text": "最终答案", "final": True},
    ]
    assert adapter._request_ids["default:h"] == "req-1"


def test_progress_and_final_replies_share_the_bridge_request_id():
    asyncio.run(assert_progress_and_final_replies_share_the_bridge_request_id())
