"""Minimal Hermes SDK doubles for standalone adapter transport tests.

The distributable is a Hermes plugin, so production imports come from the host
Gateway. These tests deliberately exercise adapter-owned protocol behavior and
can run from a clean package install without requiring a full Hermes checkout.
"""

import sys
import types
from dataclasses import dataclass


gateway = types.ModuleType("gateway")
config = types.ModuleType("gateway.config")
platforms = types.ModuleType("gateway.platforms")
base = types.ModuleType("gateway.platforms.base")


class Platform:
    def __init__(self, value):
        self.value = value


class BasePlatformAdapter:
    def __init__(self, config, platform):
        self.config = config
        self.platform = platform
        self._running = False


@dataclass
class SendResult:
    success: bool
    message_id: str | None = None
    error: str | None = None


class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class MessageType:
    TEXT = "text"


config.Platform = Platform
base.BasePlatformAdapter = BasePlatformAdapter
base.MessageEvent = MessageEvent
base.MessageType = MessageType
base.SendResult = SendResult

sys.modules.setdefault("gateway", gateway)
sys.modules.setdefault("gateway.config", config)
sys.modules.setdefault("gateway.platforms", platforms)
sys.modules.setdefault("gateway.platforms.base", base)
