import importlib.metadata
import logging
from typing import Dict, Optional, Type

# Configure logging
logger = logging.getLogger(__name__)

# Entry point group name defined implicitly by satif-ai's registration
ADAPTER_ENTRY_POINT_GROUP = "satif_sdk.adapters"

# Internal registry to cache loaded adapters
_adapter_registry: Optional[Dict[str, Type]] = None


def _discover_adapters() -> Dict[str, Type]:
    """Discovers adapters registered via entry points."""
    global _adapter_registry
    if _adapter_registry is not None:
        return _adapter_registry

    logger.debug(
        f"Discovering adapters in entry point group '{ADAPTER_ENTRY_POINT_GROUP}'"
    )
    adapters: Dict[str, Type] = {}
    try:
        entry_points = importlib.metadata.entry_points(group=ADAPTER_ENTRY_POINT_GROUP)
    except Exception as e:  # Catch potential issues during discovery
        logger.error(
            f"Error accessing entry points for group '{ADAPTER_ENTRY_POINT_GROUP}': {e}",
            exc_info=True,
        )
        entry_points = []  # Ensure it's iterable

    for entry_point in entry_points:
        name = entry_point.name
        try:
            logger.debug(
                f"Attempting to load adapter '{name}' from {entry_point.value}"
            )
            adapter_class = entry_point.load()
            # Optional: Add validation here to ensure adapter_class is a valid adapter type
            # if not issubclass(adapter_class, BaseAdapter): # Example validation
            #     logger.warning(f"Skipping '{name}': Loaded object {adapter_class} is not a valid adapter type.")
            #     continue
            adapters[name] = adapter_class
            logger.debug(f"Successfully loaded adapter '{name}': {adapter_class}")
        except Exception as e:
            logger.error(
                f"Failed to load adapter '{name}' from entry point {entry_point.value}: {e}",
                exc_info=True,
            )

    _adapter_registry = adapters
    logger.info(
        f"Discovered {len(_adapter_registry)} adapters: {list(_adapter_registry.keys())}"
    )
    return _adapter_registry


def load_adapter(name: str, **kwargs) -> object:
    """
    Loads and instantiates an adapter by its registered name.

    Args:
        name: The unique name the adapter was registered with (e.g., 'tidy_ai').
        **kwargs: Keyword arguments to pass to the adapter's constructor.

    Returns:
        An instance of the requested adapter.

    Raises:
        KeyError: If no adapter with the given name is found.
        Exception: If the adapter class fails to instantiate.
    """
    registry = _discover_adapters()
    if name not in registry:
        raise KeyError(
            f"No adapter registered with name '{name}'. Available adapters: {list(registry.keys())}"
        )

    adapter_class = registry[name]
    logger.debug(
        f"Instantiating adapter '{name}' ({adapter_class}) with kwargs: {kwargs}"
    )
    try:
        return adapter_class(**kwargs)
    except Exception as e:
        logger.error(
            f"Failed to instantiate adapter '{name}' ({adapter_class}): {e}",
            exc_info=True,
        )
        raise  # Re-raise the instantiation error


def get_available_adapters() -> list[str]:
    """Returns a list of names of discovered adapters."""
    return list(_discover_adapters().keys())


# Optional: You might want to expose the registry directly if needed,
# but the load_adapter function provides a cleaner interface.
# Example: Expose the registry after first discovery
# if _adapter_registry is None:
#     _discover_adapters()
# available_adapters = _adapter_registry

# You might also want to explicitly export the public functions
__all__ = ["load_adapter", "get_available_adapters"]

# Consider adding your existing 'code.py' adapters to this registry as well,
# perhaps by default or also via entry points if you want consistency.
# from .code import CodeAdapter # Example
# if _adapter_registry is not None and 'code' not in _adapter_registry:
#    _adapter_registry['code'] = CodeAdapter
