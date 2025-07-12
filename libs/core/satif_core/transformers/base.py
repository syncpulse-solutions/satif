from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Union

from satif_core.sdif_db import SDIFDatabase
from satif_core.types import SDIFPath


class Transformer(ABC):
    """
    Abstract Base Class for data transformation.

    This class defines the interface for all transformer implementations.
    Transformers are responsible for taking SDIF (Standardized Data Interchange Format)
    data as input, performing some transformation logic, and producing output data,
    which can then be exported to various file formats.

    Concrete implementations of this class should provide logic for the `transform`
    and `_export_data` methods.
    """

    @abstractmethod
    def transform(
        self, sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]]
    ) -> Dict[str, Any]:
        """
        Transforms input SDIF data into an in-memory representation.

        This method should be implemented by subclasses to define the core
        transformation logic. It takes one or more SDIF sources, processes them,
        and returns a dictionary where keys are intended output filenames and
        values are the data to be written to these files (e.g., pandas DataFrames,
        dictionaries, lists, strings, or bytes).

        Args:
            sdif: The SDIF data source(s) to transform. This can be:
                - A single SDIF file path (str or Path).
                - A list of SDIF file paths.
                - An `SDIFDatabase` instance.
                - A dictionary mapping custom schema names (str) to SDIF file paths.

        Returns:
            A dictionary where keys are relative output filenames (e.g., "data.csv")
            and values are the corresponding transformed data objects.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
            FileNotFoundError: If any input SDIF file path does not exist.
            ValueError: If input arguments are invalid or incompatible.
            Exception: Subclasses may raise specific exceptions related to
                       transformation errors (e.g., database errors, data processing issues).
        """
        pass

    @abstractmethod
    def _export_data(
        self,
        data: Dict[str, Any],
        output_path: Union[str, Path] = Path("."),
        zip_archive: bool = False,
    ) -> Path:
        """
        Exports the transformed data to files or a ZIP archive.

        This method is responsible for taking the in-memory data produced by
        the `transform` method and writing it to the filesystem. It handles
        the creation of directories and files, and optionally archives them.

        This is intended as a protected method, to be called by the public `export`
        method or potentially by subclasses if they need more control over the
        export process after an external transformation.

        Args:
            data: A dictionary of data to export, where keys are relative
                  filenames and values are the data content (e.g., pandas DataFrame,
                  dict, list, str, bytes).
            output_path: The base path for output.
                         - If `zip_archive` is True, this should be the path to the
                           target ZIP file.
                         - If `zip_archive` is False and `data` contains a single item,
                           this can be the full path to the output file.
                         - If `zip_archive` is False and `data` contains multiple items,
                           this should be the path to the output directory where files
                           will be created based on their keys in the `data` dict.
                         Defaults to the current working directory (`.`).
            zip_archive: If True, all output files are packaged into a single ZIP
                         archive specified by `output_path`. If False, files are
                         written directly to the filesystem. Defaults to False.

        Returns:
            The absolute path to the created output file or directory.
            If `zip_archive` is True, this is the path to the ZIP file.
            If `zip_archive` is False and a single file was written, this is its path.
            If `zip_archive` is False and multiple files were written, this is the path
            to the output directory.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
            IOError: If there are issues writing files to disk (e.g., permissions,
                     disk space).
            Exception: Subclasses may raise specific exceptions related to
                       export errors (e.g., unsupported file formats, data serialization issues).
        """
        pass

    def export(
        self,
        sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]],
        output_path: Union[str, Path] = Path("."),
        zip_archive: bool = False,
    ) -> Path:
        """
        Transforms SDIF data and exports the results to files.

        This is a convenience method that orchestrates the transformation and
        export process. It first calls the `transform` method to get the
        in-memory transformed data, and then calls the `_export_data` method
        to write this data to the specified output path.

        Args:
            sdif: The SDIF data source(s) to transform. Passed directly to the
                  `transform` method. See `transform` method docstring for details.
            output_path: The base path for output. Passed directly to the
                         `_export_data` method. See `_export_data` method
                         docstring for details. Defaults to the current directory.
            zip_archive: If True, package all output files into a single ZIP archive.
                         Passed directly to the `_export_data` method.
                         Defaults to False.

        Returns:
            The absolute path to the created output file or directory.
            See `_export_data` method return value for more details.

        Raises:
            This method can raise any exceptions thrown by `transform` or
            `_export_data` methods (e.g., FileNotFoundError, ValueError, IOError).
        """
        transformed_data = self.transform(sdif)
        return self._export_data(
            data=transformed_data, output_path=output_path, zip_archive=zip_archive
        )
