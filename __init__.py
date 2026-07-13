# Hermes WeClawBot Bridge Channel Adapter
#
# Install:
#   cp plugin.yaml src/adapter.py ~/.hermes/plugins/weclawbot/
#   hermes plugins enable weclawbot
#
from .src.adapter import WeClawBotAdapter, check_requirements, validate_config, register

__all__ = ["WeClawBotAdapter", "check_requirements", "validate_config", "register"]
