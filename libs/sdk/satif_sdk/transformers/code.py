import inspect
import json
import logging
import sqlite3
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from satif_core import CodeExecutor, Transformer
from satif_core.exceptions import CodeExecutionError
from satif_core.types import FilePath, SDIFPath
from sdif_db import SDIFDatabase, cleanup_db_connection, create_db_connection

from satif_sdk.code_executors import LocalCodeExecutor

logger = logging.getLogger(__name__)


class ExportError(Exception):
    """Custom exception for export errors."""

    pass


# Global registry for decorated transformation functions
_TRANSFORMATION_REGISTRY = {}


class CodeTransformer(Transformer):
    """
    Executes custom Python code to transform data from an SDIF database into desired output files.

    Responsibilities:
    - Initializes with transformation logic (callable, code string, or file path) and a CodeExecutor.
    - When transforming with a direct callable: Manages SQLite connection setup (attaching SDIFs)
      and executes the callable in the current environment.
    - When transforming with a code string/file: Prepares SDIF source information and delegates
      to the configured `CodeExecutor`, which then handles DB setup and code execution
      in its own environment (e.g., locally or sandboxed).
    - Exports the results returned by the transformation logic to files or a zip archive.

    Args:
        function: The transformation logic (Python callable, code string, or Path to script).
        function_name: Name of the function to call if `function` is a code string/file.
                       Defaults to "transform". Ignored if `function` is a callable.
        code_executor: An instance of a `CodeExecutor` subclass. If None and transformation logic
                       is a code string or file path, a `LocalCodeExecutor` is instantiated.
        extra_context: Dictionary of extra objects to pass to the transformation function's context
                       or make available in the executor's global scope.
        db_schema_prefix: Prefix for auto-generated schema names when a list of SDIFs is given.
                          Defaults to "db".
    Transformation Function Signature:
        The transform function should accept these parameters:
        - `conn` (sqlite3.Connection): A connection to an in-memory SQLite
          database with all input SDIF files attached as schemas.
        - `context` (Dict[str, Any], optional): Extra context values if needed.

        The function MUST return a dictionary (`Dict[str, Any]`) where:
        - Keys (str): Relative output filenames (e.g., "orders_extract.csv", "summary/report.json").
        - Values (Any): Data to write (e.g., `pandas.DataFrame`, `dict`, `list`, `str`, `bytes`).
          The file extension in the key typically determines the output format.

    Example:
        ```python
        from satif.transformers.code import transformation, CodeTransformer

        # Define a transformation with the decorator
        @transformation
        def process_orders(conn):
            df = pd.read_sql_query("SELECT * FROM db1.orders", conn)
            return {"processed_orders.csv": df}

        # Use the transformation
        transformer = CodeTransformer(function=process_orders)
        # For in-memory transformation:
        transformed_data = transformer.transform(sdif="orders.sdif")

        # Or export to files:
        result_path = transformer.export(
            sdif="orders.sdif",
            output_path="output/processed_orders.csv"
        )
        ```
    """

    def __init__(
        self,
        function: Union[Callable, str, Path],
        function_name: str = "transform",
        code_executor: Optional[CodeExecutor] = None,
        extra_context: Optional[Dict[str, Any]] = None,
        db_schema_prefix: str = "db",
    ):
        self.transform_function_obj: Optional[Callable] = None
        self.transform_code: Optional[str] = None
        self.function_name = (
            function_name  # Will be overridden if function is callable and has a name
        )
        self.extra_context = extra_context or {}
        self.db_schema_prefix = db_schema_prefix
        self._original_function_input = function  # Store for _init_transform_logic
        self.code_executor = code_executor

        self._current_output_path: Optional[Path] = None
        self._init_transform_logic()

    def _init_transform_logic(self):
        """Initializes `transform_function_obj` or `transform_code` based on input."""
        function_input = self._original_function_input

        if callable(function_input):
            self.transform_function_obj = function_input
            # If function is a decorated one with _transform_name, or its actual name
            self.function_name = getattr(
                function_input, "_transform_name", function_input.__name__
            )
            logger.debug(f"Initialized with direct callable: {self.function_name}")
        elif isinstance(function_input, str):
            if (
                function_input in _TRANSFORMATION_REGISTRY
            ):  # Is it a name of a registered function?
                self.transform_function_obj = _TRANSFORMATION_REGISTRY[function_input]
                self.function_name = function_input  # The key is the name
                logger.debug(
                    f"Initialized with registered callable: {self.function_name}"
                )
            else:  # Assumed to be a code string
                self.transform_code = function_input
                # self.function_name was already set in __init__ (or default 'transform')
                logger.debug(
                    f"Initialized with code string for executor. Target function: '{self.function_name}'"
                )
                if self.code_executor is None:
                    logger.info(
                        "No code_executor provided for code string; defaulting to LocalCodeExecutor."
                    )
                    self.code_executor = LocalCodeExecutor()
        elif isinstance(function_input, Path):
            try:
                with open(function_input, encoding="utf-8") as f:
                    self.transform_code = f.read()
                # self.function_name was already set in __init__ (or default 'transform')
                logger.debug(
                    f"Initialized with code from file '{function_input}' for executor. Target function: '{self.function_name}'"
                )
                if self.code_executor is None:
                    logger.info(
                        f"No code_executor provided for code file '{function_input}'; defaulting to LocalCodeExecutor."
                    )
                    self.code_executor = LocalCodeExecutor()
            except Exception as e:
                raise ValueError(
                    f"Failed to read transformation logic from file '{function_input}': {e}"
                ) from e
        else:
            raise TypeError(
                f"Unsupported type for 'function' argument: {type(function_input)}. "
                "Expected callable, string (code or registered name), or Path."
            )

        if not self.transform_function_obj and not self.transform_code:
            # This state should ideally be prevented by the logic above
            raise ValueError(
                "Could not initialize transformation logic: No callable or code identified."
            )

        # If code is to be executed, a code_executor must exist by now (either provided or defaulted).
        if self.transform_code and not self.code_executor:
            # This is a safeguard, should have been handled when self.transform_code was set.
            logger.error(
                "CodeTransformer has code to execute, but no code_executor is available."
            )
            raise ValueError(
                "A code_executor is required when transformation logic is a code string or file path."
            )

    def transform(
        self,
        sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]],
    ) -> Dict[str, Any]:
        """
        Transforms data from SDIF input(s) using the configured logic.

        - If a direct Python callable was provided to `__init__`, this method sets up
          the SQLite connection, ATTACHes databases, executes the callable directly,
          and then cleans up the connection.
        - If a code string or file path was provided, this method prepares a map of
          SDIF sources and delegates to `_execute_transformation`, which in turn uses
          the configured `CodeExecutor`. The `CodeExecutor` is then responsible for
          database setup and code execution within its own environment.
        - Handles `SDIFDatabase` instances by using their pre-existing connection if
          a direct callable is used.
        """
        # Case 1: Input is an SDIFDatabase instance
        if isinstance(sdif, SDIFDatabase):
            if not self.transform_function_obj:
                raise ExportError(
                    "Using an SDIFDatabase instance as input is primarily for direct Python callables. "
                    "For code strings/files, please provide SDIF file paths/map."
                )
            try:
                return self._execute_transformation(conn=sdif.conn)
            except Exception as e:
                # Handle potential errors during the direct call using the SDIFDatabase connection.
                if isinstance(
                    e,
                    (
                        ExportError,
                        FileNotFoundError,
                        ValueError,
                        TypeError,
                        CodeExecutionError,
                    ),
                ):
                    raise
                raise ExportError(
                    f"Error during transformation with provided SDIFDatabase instance: {e}"
                ) from e

        # Case 2: Input is SDIFPath, List[SDIFPath], or Dict[str, SDIFPath]
        # Prepare sdif_sources_map for both direct callable and executor paths.
        sdif_sources_map: Dict[str, Path] = {}
        raw_paths: List[Path] = []  # For validation

        if isinstance(sdif, (str, Path)):
            p = Path(sdif).resolve()
            raw_paths.append(p)
            sdif_sources_map = {f"{self.db_schema_prefix}1": p}
        elif isinstance(sdif, list):
            for i, item in enumerate(sdif):
                p = Path(item).resolve()
                raw_paths.append(p)
                sdif_sources_map[f"{self.db_schema_prefix}{i + 1}"] = p
        elif isinstance(sdif, dict):
            for schema_name, path_item in sdif.items():
                p = Path(path_item).resolve()
                raw_paths.append(p)
                sdif_sources_map[schema_name] = p
        else:
            raise TypeError(
                f"Unsupported type for 'sdif' argument: {type(sdif)}. "
                "Expected str, Path, list, dict, or SDIFDatabase."
            )

        if not sdif_sources_map:
            raise ValueError("No input SDIF sources were resolved.")

        # Validate all source files exist before proceeding
        for path_to_check in raw_paths:
            if not path_to_check.exists() or not path_to_check.is_file():
                raise FileNotFoundError(f"Input SDIF file not found: {path_to_check}")

        # --- Execution path decision based on initialized transform logic ---
        if self.transform_function_obj:  # Path 1: Direct Python callable
            logger.debug(
                "Executing direct callable: CodeTransformer will manage DB connection via db_utils."
            )
            db_conn: Optional[sqlite3.Connection] = None
            attached_schemas: Dict[str, Path] = {}
            try:
                # Use db_utils to setup connection and attach sources
                db_conn, attached_schemas = create_db_connection(sdif_sources_map)

                return self._execute_transformation(conn=db_conn)
            except (
                CodeExecutionError
            ) as e:  # Catch from db_utils or other CodeExecutionErrors
                raise ExportError(
                    f"Database setup or execution failed for direct callable: {e}"
                ) from e
            except (
                sqlite3.Error
            ) as e:  # Should ideally be caught by db_utils as CodeExecutionError
                raise ExportError(
                    f"Database error during direct callable execution: {e}"
                ) from e
            finally:
                if db_conn:  # Only cleanup if CodeTransformer created the connection
                    cleanup_db_connection(db_conn, attached_schemas, should_close=True)

        elif self.transform_code:  # Path 2: Code string/file to be run by an executor
            logger.debug(
                "Executing via code_executor: Executor will manage DB connection from sources."
            )
            if not self.code_executor:
                # This should be caught by __init__, but as a defense.
                raise ExportError(
                    "No code_executor configured for transformation from code string/file."
                )

            # Pass the map of SDIF sources to the executor
            return self._execute_transformation(
                sdif_sources_for_executor=sdif_sources_map
            )
        else:
            # This case should be prevented by __init__ logic
            raise ExportError(
                "Transformation logic (callable or code) is not properly initialized."
            )

    def _execute_transformation(
        self,
        conn: Optional[sqlite3.Connection] = None,
        sdif_sources_for_executor: Optional[Dict[str, Path]] = None,
        # removed attached_schemas_for_direct_call parameter
    ) -> Dict[str, Any]:
        """
        Internal method to dispatch execution to either a direct callable or a code executor.

        - If `self.transform_function_obj` is set, it executes this callable directly,
          using `conn`.
        - If `self.transform_code` is set, it invokes `self.code_executor.execute()`,
          passing `sdif_sources_for_executor` for the executor to set up its own DB environment.
        """
        result: Dict[str, Any]
        logger.debug(f"_execute_transformation for function: '{self.function_name}'")

        try:
            if self.transform_function_obj:  # Direct callable path
                if conn is None:
                    # This indicates a programming error in the calling `transform` method
                    raise ExportError(
                        "Internal error: Connection not provided for direct callable."
                    )

                logger.debug(
                    f"Executing direct callable '{self.function_name}' with provided connection."
                )
                sig = inspect.signature(self.transform_function_obj)
                param_count = len(sig.parameters)

                if param_count == 1:  # Expects only conn
                    result = self.transform_function_obj(conn=conn)
                elif (
                    param_count >= 2
                ):  # Expects conn and context (and potentially others)
                    result = self.transform_function_obj(
                        conn=conn, context=self.extra_context
                    )
                else:  # Should have at least 'conn'
                    raise ExportError(
                        f"Directly provided transformation function '{self.function_name}' "
                        "must accept at least one parameter (conn)."
                    )
            elif self.transform_code:  # Code string/file path for executor
                if self.code_executor is None:
                    # Should be caught by __init__ or calling `transform` method
                    raise ExportError(
                        "Internal error: No code_executor available for code-based transformation."
                    )
                if sdif_sources_for_executor is None:
                    # Indicates a programming error in the calling `transform` method
                    raise ExportError(
                        "Internal error: SDIF sources not provided for executor-based transformation."
                    )

                logger.debug(
                    f"Delegating to code_executor for '{self.function_name}'. Sources: {sdif_sources_for_executor}"
                )
                result = self.code_executor.execute(
                    code=self.transform_code,
                    function_name=self.function_name,
                    sdif_sources=sdif_sources_for_executor,
                    extra_context=self.extra_context,
                )
            else:
                # This state should not be reached if __init__ is correct
                raise ExportError(
                    "No transformation logic (callable or code) is available to execute."
                )

            # Validate the result from either path
            if not isinstance(result, dict):
                raise ExportError(
                    f"Transformation function '{self.function_name}' (whether direct or via executor) "
                    f"must return a dictionary, but got {type(result)}."
                )

            return {str(k): v for k, v in result.items()}

        except (
            CodeExecutionError
        ) as e:  # Catch errors specifically from CodeExecutor.execute
            logger.error(
                f"CodeExecutionError from executor for '{self.function_name}': {e}"
            )
            # Re-raise as ExportError for consistent error type from CodeTransformer's public API
            raise ExportError(
                f"Error during code execution via executor for '{self.function_name}': {e}"
            ) from e
        except (
            Exception
        ) as e:  # Catch other errors (e.g., direct call issues, unexpected issues)
            logger.error(
                f"Error during execution of transformation function '{self.function_name}': {e}"
            )
            if isinstance(
                e, ExportError
            ):  # Re-raise if already our specific error type
                raise
            # Wrap other exceptions in ExportError
            raise ExportError(
                f"Error during transformation '{self.function_name}': {e}"
            ) from e

    def export(
        self,
        sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]],
        output_path: FilePath = Path("."),
        zip_archive: bool = False,
    ) -> Path:
        """
        Transforms data from SDIF input(s) and exports results to files.
        This is a convenience method that combines transform() and export().

        Args:
            sdif: Input SDIF data source. Can be:
                  - A single path (str/Path)
                  - A list of paths
                  - An SDIFDatabase instance
                  - A dictionary mapping schema names to paths (e.g., {"customers": "customers.sdif"})
            output_path: Path to the output file (if zip_archive=True or single output)
                         or directory (if multiple outputs). Defaults to current directory.
            zip_archive: If True, package all output files into a single ZIP archive
                         at the specified output_path.

        Returns:
            Path to the created output file or directory.

        Raises:
            ExportError: If any error occurs during transformation or writing.
            ValueError: If input arguments are invalid.
            FileNotFoundError: If an input SDIF file does not exist.
            TypeError: If the 'sdif' argument is of an unsupported type.
        """
        transformed_data = self.transform(sdif=sdif)
        return self._export_data(
            data=transformed_data, output_path=output_path, zip_archive=zip_archive
        )

    def _export_data(
        self,
        data: Dict[str, Any],
        output_path: FilePath = Path("."),
        zip_archive: bool = False,
    ) -> Path:
        """
        Exports the transformed data to files or a zip archive.

        Args:
            data: Dictionary of data to export where:
                 - Keys (str): Relative output filenames (e.g., "orders_extract.csv", "summary/report.json").
                 - Values (Any): Data to write (e.g., `pandas.DataFrame`, `dict`, `list`, `str`, `bytes`).
            output_path: Path to the output file (if zip_archive=True or single output)
                         or directory (if multiple outputs). Defaults to current directory.
            zip_archive: If True, package all output files into a single ZIP archive
                         at the specified output_path.

        Returns:
            Path to the created output file or directory.

        Raises:
            ExportError: If any error occurs during exporting or writing.
        """
        if not data:
            logger.warning("No data to export.")
            return Path(output_path)
        resolved_output_path = Path(output_path).resolve()
        self._current_output_path = resolved_output_path
        try:
            logger.debug(
                f"Exporting {len(data)} items to write to {resolved_output_path}."
            )
            if zip_archive:
                self._write_zip(data)
            else:
                self._write_files(data)
            return self._current_output_path
        except Exception as e:
            logger.error(f"Export process failed: {e}")
            if isinstance(e, ExportError):
                raise
            raise ExportError(f"Unexpected error during export: {e}") from e
        finally:
            self._current_output_path = None

    def _write_files(self, data_to_write: Dict[str, Any]) -> None:
        if self._current_output_path is None:
            raise ExportError(
                "Internal error: Output path not set before writing files."
            )
        output_dir_or_file = self._current_output_path
        target_dir: Path
        is_single_file_output = False

        if output_dir_or_file.is_dir():
            target_dir = output_dir_or_file
            logger.debug(
                f"Output path '{target_dir}' is a directory. Writing files inside."
            )
        elif len(data_to_write) == 1 and (
            not output_dir_or_file.exists() or output_dir_or_file.is_file()
        ):
            # User provided a file path (or non-existent path that implies a file) for a single output
            target_dir = output_dir_or_file.parent
            is_single_file_output = True
            logger.debug(
                f"Output path '{output_dir_or_file}' treated as single file path. Parent: '{target_dir}'."
            )
        elif len(data_to_write) > 1 and (not output_dir_or_file.exists()):
            # User provided a non-existent path for multiple files, treat as directory to be created
            target_dir = output_dir_or_file
            logger.debug(
                f"Output path '{target_dir}' does not exist. Creating as directory for multiple files."
            )
        else:
            # Ambiguous or problematic cases:
            # - Path exists, is a file, but multiple outputs requested.
            # - Path is a directory, but maybe user intended a file name for zip archive (handled by _write_zip checks)
            raise ExportError(
                f"Output path '{output_dir_or_file}' is problematic. If multiple files are generated, "
                f"it must be a directory path or a non-existent path that can be created as a directory. "
                f"If a single file is generated, it can be a file path."
            )

        target_dir.mkdir(parents=True, exist_ok=True)

        for filename_key, data_content in data_to_write.items():
            output_filepath: Path
            if is_single_file_output:
                # For a single file output, the user-provided path is the final one.
                # The filename_key from results is not used for path construction here, output_dir_or_file is the direct target.
                output_filepath = output_dir_or_file
                # It's good practice to ensure the output_dir_or_file itself (which is the target path)
                # is within the intended parent (target_dir) if we were to be extremely rigorous,
                # but _sanitize_output_filename is primarily for keys from transformation results.
                # The creation of output_filepath from output_dir_or_file already implies user intent for this exact path.
            else:
                # For multiple files, use the sanitized key relative to the target_dir
                safe_path = self._sanitize_output_filename(
                    filename_key, target_dir_for_file=target_dir
                )
                if safe_path is None:
                    logger.warning(
                        f"Skipping unsafe filename key from results: {filename_key}"
                    )
                    continue  # Skip unsafe filenames
                output_filepath = safe_path

            output_filepath.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._write_single_file(output_filepath, data_content)
                logger.info(f"Successfully wrote output file: {output_filepath}")
            except Exception as e:
                if isinstance(e, ExportError):
                    raise
                raise ExportError(f"Error writing file {output_filepath}: {e}") from e

    def _write_zip(self, data_to_write: Dict[str, Any]) -> None:
        if self._current_output_path is None:
            raise ExportError("Internal error: Output path not set before writing zip.")
        output_zip_path = self._current_output_path

        if output_zip_path.is_dir():
            raise ExportError(
                f"Output path '{output_zip_path}' must be a file path for zip_archive=True, not a directory."
            )

        output_zip_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for filename_key, data_item in data_to_write.items():
                    # Sanitize the filename_key for use as the path within the zip archive
                    path_in_zip_obj = self._sanitize_output_filename(
                        filename_key, target_dir_for_file=None
                    )
                    if path_in_zip_obj is None:
                        logger.warning(
                            f"Skipping unsafe filename key for zip entry: {filename_key}"
                        )
                        continue  # Skip unsafe filenames

                    archive_name = path_in_zip_obj.as_posix()
                    original_ext = (
                        Path(filename_key).suffix.lower()
                    )  # Use original key for extension for data conversion logic

                    content_bytes: Optional[bytes] = None
                    if isinstance(data_item, pd.DataFrame):
                        current_archive_name_for_df = (
                            archive_name  # Store name before potential modification
                        )
                        try:
                            if original_ext == ".csv":
                                content_bytes = data_item.to_csv(index=False).encode(
                                    "utf-8"
                                )
                            elif original_ext == ".json":
                                content_bytes = data_item.to_json(
                                    orient="records", indent=2
                                ).encode("utf-8")
                            else:
                                logger.warning(
                                    f"Unsupported DataFrame extension '{original_ext}' for '{current_archive_name_for_df}' in zip. Writing as CSV."
                                )
                                # Update archive_name to reflect the .csv extension change for this entry
                                archive_name = path_in_zip_obj.with_suffix(
                                    ".csv"
                                ).as_posix()
                                content_bytes = data_item.to_csv(index=False).encode(
                                    "utf-8"
                                )
                        except Exception as df_ex:
                            logger.error(
                                f"Error converting DataFrame for '{filename_key}' (to be '{current_archive_name_for_df}') for zip: {df_ex}"
                            )
                            continue
                    elif isinstance(data_item, (dict, list)):
                        try:
                            content_bytes = json.dumps(data_item, indent=2).encode(
                                "utf-8"
                            )
                        except Exception as json_ex:
                            logger.error(
                                f"Error serializing '{filename_key}' (to be '{archive_name}') to JSON for zip: {json_ex}"
                            )
                            continue
                    elif isinstance(data_item, str):
                        content_bytes = data_item.encode("utf-8")
                    elif isinstance(data_item, bytes):
                        content_bytes = data_item
                    else:
                        logger.warning(
                            f"Unsupported data type {type(data_item)} for file '{archive_name}' (from key '{filename_key}') in zip. Skipping."
                        )
                        continue

                    if content_bytes is not None:
                        try:
                            zipf.writestr(
                                archive_name, content_bytes
                            )  # Use potentially modified archive_name here
                        except Exception as zip_write_ex:
                            logger.error(
                                f"Error writing file '{archive_name}' (from key '{filename_key}') to zip: {zip_write_ex}"
                            )
            logger.info(f"Successfully created ZIP archive: {output_zip_path}")
        except Exception as e:
            raise ExportError(f"Error creating ZIP file {output_zip_path}: {e}") from e

    def _write_single_file(self, filepath: Path, data: Any) -> None:
        try:
            if isinstance(data, pd.DataFrame):
                ext = filepath.suffix.lower()
                if ext == ".csv":
                    data.to_csv(filepath, index=False)
                elif ext == ".json":
                    data.to_json(filepath, orient="records", indent=2)
                elif ext in [".xlsx", ".xls"]:
                    try:
                        # Ensure openpyxl is available for .xlsx, xlwt for .xls (though pandas might use openpyxl for .xls too)
                        if ext == ".xlsx":
                            import openpyxl  # type: ignore # noqa: F401
                        # For .xls, pandas might try xlwt or openpyxl. Let's suggest openpyxl as it's more common.
                        data.to_excel(filepath, index=False)
                    except ImportError:
                        dep = "openpyxl"
                        raise ExportError(
                            f"Writing to Excel format ('{ext}') requires '{dep}'. Please install it."
                        )
                else:  # Default to CSV for unknown extensions for DataFrame
                    csv_path = filepath.with_suffix(".csv")
                    logger.warning(
                        f"Unsupported DataFrame extension '{ext}' for file '{filepath.name}'. Writing as CSV to '{csv_path}'."
                    )
                    data.to_csv(csv_path, index=False)
            elif isinstance(data, (dict, list)):
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
            elif isinstance(data, str):
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(data)
            elif isinstance(data, bytes):
                with open(filepath, "wb") as f:
                    f.write(data)
            else:
                raise TypeError(
                    f"Unsupported data type '{type(data)}' for writing to file '{filepath.name}'."
                )
        except Exception as e:
            if isinstance(e, ExportError):
                raise  # Re-raise if already our specific error type
            raise ExportError(f"Error writing data to file '{filepath}': {e}") from e

    def _sanitize_output_filename(
        self, filename_key: str, target_dir_for_file: Optional[Path] = None
    ) -> Optional[Path]:
        """
        Validates and sanitizes a filename key from transformation results.

        Args:
            filename_key: The filename key from the transformation result.
            target_dir_for_file: If writing a direct file (not into a zip),
                                 this is the base directory to resolve against and check containment.
                                 If None (e.g., for zip archive paths), only basic checks are performed.

        Returns:
            - For file output (target_dir_for_file is not None): Absolute Path object if safe and within target_dir_for_file.
            - For zip output (target_dir_for_file is None): Relative Path object if safe.
            - None if the path is unsafe.
        """
        p_filename = Path(filename_key)
        if ".." in p_filename.parts or p_filename.is_absolute():
            logger.error(
                f"Skipping potentially unsafe filename key (contains '..' or is absolute): '{filename_key}'"
            )
            return None

        if target_dir_for_file:
            resolved_target_dir = target_dir_for_file.resolve()
            abs_filepath = (resolved_target_dir / p_filename).resolve()

            # Check if resolved_target_dir is a parent of abs_filepath, or if they are the same path.
            # This robustly checks for containment.
            if (
                resolved_target_dir != abs_filepath
                and resolved_target_dir not in abs_filepath.parents
            ):
                logger.error(
                    f"Skipping filename '{filename_key}' which resolves outside target directory '{resolved_target_dir}'. Resolved: '{abs_filepath}'"
                )
                return None
            return abs_filepath  # Return the safe, absolute path for file writing
        else:
            # For zip archives, just return the sanitized (relative) path
            return p_filename


def transformation(func=None, name=None):
    """
    Decorator to register a function as a transformation.
    Can be used with or without arguments.

    @transformation
    def my_transform(conn): # conn is sqlite3.Connection
        ...

    @transformation(name="custom_name")
    def my_transform_custom_name(conn, context: dict):
        ...

    Args:
        func: The function to decorate.
        name: Optional custom name for the transformation. If None, function's __name__ is used.

    Returns:
        The decorated function, now registered and marked as a transformation.
    """

    def decorator(f):
        transform_name = name or f.__name__
        if not isinstance(transform_name, str) or not transform_name:
            raise ValueError("Transformation name must be a non-empty string.")
        if transform_name in _TRANSFORMATION_REGISTRY:
            logger.warning(
                f"Transformation name '{transform_name}' is already registered. Overwriting."
            )
        _TRANSFORMATION_REGISTRY[transform_name] = f
        # Add attributes to the function itself for identification
        setattr(f, "_is_transformation", True)
        setattr(f, "_transform_name", transform_name)
        return f

    if func is None:
        # Called as @transformation(name=...)
        return decorator
    else:
        # Called as @transformation
        return decorator(func)
