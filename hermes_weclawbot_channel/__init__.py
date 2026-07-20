"""Installable package for the Hermes WeClawBot channel adapter."""

from .adapter import WeClawBotAdapter, check_requirements, register, validate_config

__all__ = ["WeClawBotAdapter", "check_requirements", "validate_config", "register"]
