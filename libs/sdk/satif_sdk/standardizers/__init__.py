from pathlib import Path
from typing import Dict, Optional, Type

from satif_core import Standardizer
from satif_core.types import Datasource

from .csv import CSVStandardizer
from .xlsx import XLSXStandardizer

# Import other standardizers here as they are created
# from .pdf import PDFStandardizer

# Map file extensions (lowercase) to standardizer classes
_STANDARDIZER_MAP: Dict[str, Type[Standardizer]] = {
    ".csv": CSVStandardizer,
    # ".pdf": PDFStandardizer,
    ".xlsx": XLSXStandardizer,
}


def get_standardizer(datasource: Datasource) -> Optional[Type[Standardizer]]:
    """
    Selects the appropriate standardizer based on the datasource file type(s).

    Args:
        datasource: A single file path or a list of file paths.

    Returns:
        The Standardizer class type if a suitable one is found, otherwise None.
    """
    if isinstance(datasource, (str, Path)):
        # Single file case
        try:
            file_path = Path(datasource)
            extension = file_path.suffix.lower()
            return _STANDARDIZER_MAP.get(extension)
        except Exception:  # Handle potential path errors
            return None
    elif isinstance(datasource, list):
        # List of files case
        if not datasource:
            return None  # Empty list

        try:
            file_paths = [Path(p) for p in datasource]
            first_extension = file_paths[0].suffix.lower()

            # Check if all files have the same extension
            if all(p.suffix.lower() == first_extension for p in file_paths):
                # Currently, only multi-file CSV is supported
                if first_extension == ".csv":
                    return CSVStandardizer
                elif first_extension == ".xlsx":
                    return XLSXStandardizer
                else:
                    # Add support for other multi-file standardizers here later
                    return None
            else:
                # Heterogeneous file types
                return None
        except Exception:  # Handle potential path errors in the list
            return None
    else:
        # Invalid datasource type
        return None
