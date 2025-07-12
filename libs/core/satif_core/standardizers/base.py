from abc import ABC, abstractmethod
from typing import Any

from satif_core.types import Datasource, SDIFPath, StandardizationResult


class Standardizer(ABC):
    """
    Abstract Base Class for data standardization.

    This class defines the interface for all synchronous standardizer implementations.
    Standardizers are responsible for taking raw files from various formats
    (e.g., CSV, XLSX, PDF, etc.) and transforming it into a single
    SDIF (Standardized Data Interchange Format) file.

    Concrete implementations of this class must provide logic for the `standardize` method.
    """

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> StandardizationResult:
        """
        Standardizes input data from the specified datasource into an SDIF file.

        This method should be implemented by subclasses to define the core
        standardization logic. It reads data from the `datasource`, processes it
        according to the standardizer's specific rules and configurations,
        and writes the resulting standardized data to an SDIF database file
        at `output_path`.

        Args:
            datasource: The source of the data to be standardized. This can be a
                        file path (str or Path), a list of file paths, or another
                        type specific to the standardizer (e.g., a database connection string).
            output_path: The path (str or Path) where the output SDIF database
                         file will be created. Note: The `StandardizationResult.output_path`
                         will be the definitive path returned, which should be an absolute Path object.
            overwrite: If True, an existing SDIF file at `output_path` will be
                       overwritten. Defaults to False.
            **kwargs: Arbitrary keyword arguments that can be used for future
                      extensions or specific standardizer needs.

        Returns:
            A `StandardizationResult` object containing:
            - `output_path`: The absolute path to the created or updated SDIF database file.
            - `file_configs`: An optional dictionary where keys are input file paths (str)
                               and values are `FileConfig` (Dict[str, Any]) detailing the
                               configuration used for that specific file. None if not applicable.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
            FileNotFoundError: If the `datasource` (when it's a path) does not exist.
            ValueError: If input arguments are invalid (e.g., unsupported datasource type,
                        invalid configuration).
            IOError: If there are issues reading from the datasource or writing to
                     the `output_path` (e.g., permissions, disk space).
            Exception: Subclasses may raise specific exceptions related to data parsing,
                       validation, or transformation errors during the standardization process.
        """
        pass


class AsyncStandardizer(ABC):
    """
    Abstract Base Class for asynchronous data standardization.

    This class extends the `Standardizer` interface for implementations
    that perform standardization operations asynchronously. This is typically
    useful for I/O-bound operations, such as fetching data from remote APIs
    or handling large files without blocking the main execution thread.

    Concrete implementations must provide an asynchronous `standardize` method.
    """

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    async def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> StandardizationResult:
        """
        Asynchronously standardizes input data from the specified datasource into an SDIF file.

        This method should be implemented by subclasses to define the core
        asynchronous standardization logic. It reads data from the `datasource`,
        processes it, and writes the resulting standardized data to an SDIF
        database file at `output_path` using awaitable operations where appropriate.

        Args:
            datasource: The source of the data to be standardized. See the synchronous
                        `Standardizer.standardize` method for details.
            output_path: The path where the output SDIF database file will be created.
                         Note: The `StandardizationResult.output_path` will be the
                         definitive path returned.
            overwrite: If True, an existing SDIF file at `output_path` will be
                       overwritten. Defaults to False.
            **kwargs: Arbitrary keyword arguments for future extensions or specific needs.
                      See the synchronous `Standardizer.standardize` method for details.

        Returns:
            A `StandardizationResult` object containing:
            - `output_path`: The absolute path to the created or updated SDIF database file.
            - `file_configs`: An optional dictionary where keys are input file paths (str)
                               and values are `FileConfig` (Dict[str, Any]) detailing the
                               configuration used for that specific file. None if not applicable.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
            FileNotFoundError: If the `datasource` (when it's a path) does not exist.
            ValueError: If input arguments are invalid.
            IOError: If there are issues reading from the datasource or writing to `output_path`.
            Exception: Subclasses may raise specific exceptions related to the asynchronous
                       standardization process.
        """
        pass
