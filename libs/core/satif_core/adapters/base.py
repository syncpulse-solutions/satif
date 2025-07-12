from abc import ABC, abstractmethod
from pathlib import Path

from satif_core.types import SDIFPath


class Adapter(ABC):
    """
    Abstract Base Class for SDIF adapters.

    An Adapter defines a standardized interface for components that transform
    an input SDIF (Standardized Data Interchange Format) database into an output
    SDIF database. Transformations can include schema modifications, data
    reshaping, content enrichment, or other alterations.

    The core principle is to take one SDIF representation (specified by its file path)
    and produce another, typically aiming to preserve or appropriately modify
    the informational content while potentially changing its structure to suit
    different analytical or processing needs.

    Adapters are expected to operate asynchronously.
    """

    @abstractmethod
    def adapt(self, sdif: SDIFPath) -> Path:
        """
        Adapts an SDIF database file.

        Implementations of this method are responsible for:
        1.  Accepting the file path to an input SDIF database.
        2.  Performing a transformation on the data. This typically involves
            reading from the input, processing, and writing to a new SDIF file.
        3.  Returning the file path to the newly created, adapted SDIF database.

        The input SDIF file should generally be treated as read-only by the
        adapter's direct logic; modifications should be made to a copy or
        a new file to ensure the original input is preserved.

        Args:
           sdif: The SDIF data source(s) to adapt. This can be:
                - A single SDIF file path (str or Path).
                - An `SDIFDatabase` instance.

        Returns:
            A `pathlib.Path` object pointing to the newly created and
            adapted SDIF database file.

        Raises:
            FileNotFoundError: If the `sdif_input_path` does not exist, is not a file,
                               or is otherwise inaccessible.
            AdapterError: (Or a subclass thereof) For errors specific to the
                          adaptation process itself. Concrete implementations may define
                          and raise more specific error types.
            Exception: For other unexpected errors encountered during adaptation.
        """
        pass
