import asyncio

from hermes_weclawbot_channel.src.adapter import WeClawBotAdapter


def make_adapter() -> WeClawBotAdapter:
    """Construct only state used by standalone reply-routing tests."""
    adapter = object.__new__(WeClawBotAdapter)
    adapter.agent_id = "h"
    adapter._request_ids = {}
    adapter._outbound_lock = asyncio.Lock()
    return adapter


def test_tool_and_stream_events_are_suppressed_for_one_shot_bridge_replies():
    adapter = make_adapter()
    assert adapter.render_message_event(object(), object()) is None
    assert adapter.format_tool_event(object()) is None


async def assert_final_send_uses_the_original_bridge_request_id():
    adapter = make_adapter()
    sent = []

    async def capture(message):
        sent.append(message)

    adapter._send_raw = capture
    adapter._request_ids["default:h"] = "req-1"
    result = await adapter.send("default:h", "最终答案")

    assert result.success
    assert sent == [{"type": "chat", "id": "req-1", "text": "最终答案"}]
    assert adapter._request_ids["default:h"] == "req-1"


def test_final_send_uses_the_original_bridge_request_id():
    asyncio.run(assert_final_send_uses_the_original_bridge_request_id())
