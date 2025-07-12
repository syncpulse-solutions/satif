"""Define the configurable parameters for the xlsx_to_sdif."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Optional

from googleapiclient.discovery import Resource
from langchain_core.runnables import RunnableConfig


@dataclass(kw_only=True)
class Configuration:
    """The configuration for the xlsx_to_sdif."""

    # Changeme: Add configurable values here!
    # these values can be pre-set when you
    # create assistants (https://langchain-ai.github.io/langgraph/cloud/how-tos/configuration_cloud/)
    # and when you invoke the graph
    google_sheets_service: Resource

    @classmethod
    def from_runnable_config(
        cls, config: Optional[RunnableConfig] = None
    ) -> Configuration:
        """Create a Configuration instance from a RunnableConfig object."""
        configurable = (config.get("configurable") or {}) if config else {}
        _fields = {f.name for f in fields(cls) if f.init}
        return cls(**{k: v for k, v in configurable.items() if k in _fields})
