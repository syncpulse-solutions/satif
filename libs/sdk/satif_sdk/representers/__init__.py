import logging
from pathlib import Path
from typing import Optional, Union

from satif_core.representers.base import Representer

from .csv import CSVRepresenter
from .xlsx import XlsxRepresenter

log = logging.getLogger(__name__)

__all__ = ["Representer", "CSVRepresenter", "XlsxRepresenter", "get_representer"]

# Mapping from lowercase extension to the corresponding class
REPRESENTER_MAP = {
    ".csv": CSVRepresenter,
    ".xlsx": XlsxRepresenter,
}


def get_representer(file_path: Union[str, Path]) -> Optional[Representer]:
    """
    Factory function to get the appropriate file representer based on extension.

    Args:
        file_path: Path to the file.

    Returns:
        An instance of a BaseRepresenter subclass, or None if the file type
        is unsupported or the file doesn't exist.
    """
    try:
        p = Path(file_path)

        if not p.is_file():
            log.error(f"File not found for representation: {p}")
            # Option 1: Return None
            return None
            # Option 2: Raise FileNotFoundError(f"File not found: {p}")

        suffix = p.suffix.lower()
        representer_class = REPRESENTER_MAP.get(suffix)

        if representer_class:
            log.debug(f"Found representer {representer_class.__name__} for {p.name}")
            # Instantiate and return
            # Check for dependencies here if needed (e.g., pandas for xlsx)
            if suffix == ".xlsx":
                try:
                    import pandas  # noqa F401 Check if pandas can be imported
                except ImportError:
                    log.error(
                        "Pandas library is required for XLSX representation but not installed."
                    )
                    # Option 1: Return None
                    return None
                    # Option 2: Raise ImportError(...)
            return representer_class()
        else:
            log.warning(
                f"No representer found for file extension '{suffix}' in {p.name}"
            )
            return None  # Or raise ValueError

    except Exception as e:
        log.exception(f"Error creating representer for {file_path}: {e}")
        return None  # General error during factory process
