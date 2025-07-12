import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from satif_core import Standardizer
from satif_core.types import Datasource, FileConfig, SDIFPath, StandardizationResult
from sdif_db import SDIFDatabase

from satif_sdk.utils import (
    DELIMITER_SAMPLE_SIZE,
    ColumnDefinitionsConfig,
    ColumnDefinitionsInput,
    ColumnSpec,
    SkipColumnsConfig,
    SkipRowsConfig,
    detect_csv_delimiter,
    detect_file_encoding,
    normalize_list_argument,
    parse_skip_columns_config,
    parse_skip_rows_config,
    sanitize_sql_identifier,
    validate_skip_columns_config,
    validate_skip_rows_config,
)

# Constant for type inference sample size
SAMPLE_SIZE = 100
logger = logging.getLogger(__name__)


class CSVFileConfig(FileConfig):
    delimiter: Optional[str]
    encoding: Optional[str]
    has_header: bool
    skip_rows: SkipRowsConfig
    skip_columns: SkipColumnsConfig
    descriptions: Optional[Union[str, List[Optional[str]]]]
    table_names: Optional[Union[str, List[Optional[str]]]]
    column_definitions: ColumnDefinitionsConfig


class CSVStandardizer(Standardizer):
    """
    Standardizer for one or multiple CSV files into a single SDIF database.

    Transforms CSV data into the SDIF format, handling single or multiple files.
    Default CSV parsing options (delimiter, encoding, header, skip_rows,
    skip_columns) are set during initialization. These defaults can
    be overridden on a per-file basis when calling the `standardize` method.
    Includes basic type inference for columns (INTEGER, REAL, TEXT).

    Attributes:
        default_delimiter (Optional[str]): Default CSV delimiter character. If None, attempts auto-detection.
        default_encoding (Optional[str]): Default file encoding. If None, attempts auto-detection.
        default_has_header (bool): Default assumption whether CSV files have a header row.
        default_skip_rows (SkipRowsConfig): Raw config for rows to skip, validated from constructor.
        default_skip_columns (SkipColumnsConfig): Raw config for columns to skip, validated from constructor.
        descriptions (Optional[Union[str, List[Optional[str]]]]): Descriptions for the data sources.
        table_names (Optional[Union[str, List[Optional[str]]]]): Target table names in the SDIF database.
        file_configs (Optional[Union[Dict[str, CSVFileConfig], List[Optional[CSVFileConfig]]]]): File-specific configuration overrides.
        column_definitions (ColumnDefinitionsConfig): Column definitions for the data sources.
    """

    def __init__(
        self,
        # Default parsing options (can be overridden by file_configs)
        delimiter: Optional[str] = None,
        encoding: Optional[str] = None,
        has_header: bool = True,
        skip_rows: SkipRowsConfig = 0,
        skip_columns: SkipColumnsConfig = None,
        descriptions: Optional[Union[str, List[Optional[str]]]] = None,
        table_names: Optional[Union[str, List[Optional[str]]]] = None,
        column_definitions: ColumnDefinitionsConfig = None,
        file_configs: Optional[
            Union[Dict[str, CSVFileConfig], List[Optional[CSVFileConfig]]]
        ] = None,
    ):
        """
        Initialize the CSV standardizer with default and task-specific configurations.

        Args:
            delimiter: Default CSV delimiter character. If None, attempts auto-detection.
                       If auto-detection fails, defaults to ',' with a warning.
            encoding: Default file encoding. If None, attempts auto-detection using charset-normalizer.
                      If auto-detection fails, defaults to 'utf-8' with a warning.
            has_header: Default assumption whether CSV files have a header row.
            skip_rows: Rows to skip. Can be:
                       - An `int`: Skips the first N rows.
                       - A `List[int]` or `Set[int]`: Skips rows by their specific 0-based index (negative indices count from end).
                       Defaults to 0 (skip no rows). Non-negative indices only for positive specification.
            skip_columns: Columns to skip. Can be:
                          - An `int` or `str`: Skip a single column by 0-based index or name.
                          - A `List` or `Set` containing `int` or `str`: Skip multiple columns by index or name.
                          Column names are only effective if `has_header=True`. Non-negative indices only.
                          Defaults to None (skip no columns).
            descriptions: A single description for all sources, or a list of
                          descriptions (one per input file expected in standardize).
                          If None, descriptions are omitted. Used for `sdif_sources.source_description`.
            table_names: A single table name (used as a base if multiple files),
                         a list of table names (one per input file expected in standardize), or None.
                         If None, table names are derived from input filenames.
            file_configs: Optional configuration overrides. Can be a single dict
                          applied to all files, or a list of dicts (one per file expected
                          in standardize, use None in list to apply defaults). Keys in the dict
                          can include 'delimiter', 'encoding', 'has_header',
                          'skip_rows', 'skip_columns', 'description', 'table_name', 'column_definitions'.
                          These override the defaults set above.
            column_definitions: Provides explicit definitions for columns, overriding automatic header
                                processing or inference. This allows renaming columns, selecting specific
                                columns, and providing descriptions. Types are still inferred.
                                Can be:
                                - A `List[ColumnSpec]`: Defines columns for a single table. If multiple input
                                  files are processed and this single list is provided, it's applied to each.
                                  Each `ColumnSpec` is a dict:
                                    `{"original_identifier": "SourceColNameOrIndex", "final_column_name": "TargetColName", "description": "Optional desc."}`
                                    `original_identifier` (str): Name or 0-based index (as str) in the CSV.
                                    `final_column_name` (str): Desired name in the SDIF table.
                                    `description` (str, optional): Column description.
                                - A `Dict[str, List[ColumnSpec]]`: Maps final table names to their column specs.
                                  Useful when `table_names` are known and you want to define columns per table.
                                - A `List[Optional[Union[List[ColumnSpec], Dict[str, List[ColumnSpec]]]]]`:
                                  A list corresponding to each input file. Each element can be `None` (use default
                                  handling), a `List[ColumnSpec]` for that file's table, or a
                                  `Dict[str, List[ColumnSpec]]` if that file might map to specific table names
                                  (though CSV standardizer typically creates one table per file).
                                - If `None` (default), columns are derived from CSV header or generated, and types inferred.
        """
        self.default_skip_rows = validate_skip_rows_config(skip_rows)
        self.default_skip_columns = validate_skip_columns_config(skip_columns)

        self.default_delimiter = delimiter
        self.default_encoding = encoding
        self.default_has_header = has_header

        self.descriptions = descriptions
        self.table_names = table_names
        self.file_configs = file_configs
        self.column_definitions = column_definitions

    def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
    ) -> StandardizationResult:
        """
        Standardize one or more CSV files into a single SDIF database file,
        using configurations provided during initialization or overridden per file.

        Args:
            datasource: A single file path (str or Path) or a list of file paths
                        for the CSV files to be standardized.
            output_path: The path (str or Path) where the output SDIF database
                         file will be created.
            overwrite: If True, an existing SDIF file at `output_path` will be
                       overwritten. Defaults to False (raises an error if file exists).

        Returns:
            A StandardizationResult object containing the path to the created
            SDIF file and a dictionary of the final configurations used for each
            processed input file.

        Raises:
            FileNotFoundError: If an input CSV file is not found.
            ValueError: If input parameters are invalid (e.g., no input datasource,
                        input path is not a file).
            TypeError: If datasource type is incorrect.
            Various other exceptions from underlying CSV parsing or database operations
            can also be raised if critical errors occur.
        """
        output_sdif_path = Path(output_path)
        input_paths: List[Path]
        if isinstance(datasource, (str, Path)):
            input_paths = [Path(datasource).resolve()]
        elif isinstance(datasource, list):
            input_paths = [Path(p).resolve() for p in datasource]
        else:
            raise TypeError(
                "datasource must be a file path string/Path object or a list of such paths."
            )

        if not input_paths:
            raise ValueError("No input datasource provided.")

        num_inputs = len(input_paths)
        file_configs_used: Dict[str, CSVFileConfig] = {}

        descriptions_list = normalize_list_argument(
            self.descriptions, "Descriptions", num_inputs
        )
        table_names_list = normalize_list_argument(
            self.table_names, "Table names", num_inputs
        )
        file_configs_overrides_list = normalize_list_argument(
            self.file_configs, "File configs", num_inputs
        )
        column_definitions_config_list: List[Optional[ColumnDefinitionsInput]] = (
            normalize_list_argument(
                self.column_definitions, "Column definitions", num_inputs
            )
        )

        with SDIFDatabase(output_sdif_path, overwrite=overwrite) as db:
            for i, current_input_path in enumerate(input_paths):
                resolved_input_path_str = str(current_input_path.resolve())
                current_file_params: CSVFileConfig = {}
                try:
                    if not current_input_path.exists():
                        raise FileNotFoundError(
                            f"Input CSV file not found: {current_input_path}"
                        )
                    if not current_input_path.is_file():
                        raise ValueError(
                            f"Input path is not a file: {current_input_path}"
                        )

                    current_file_params = self._gather_file_processing_parameters(
                        input_path=current_input_path,
                        index=i,
                        num_inputs=num_inputs,
                        descriptions=descriptions_list,
                        table_names=table_names_list,
                        file_configs=file_configs_overrides_list,
                        column_definitions_for_all_files=column_definitions_config_list,
                    )

                    # --- CSV Data Processing ---
                    final_encoding = current_file_params["encoding"]
                    final_delimiter = current_file_params["delimiter"]
                    current_has_header = current_file_params["has_header"]
                    effective_skip_rows_raw = current_file_params["skip_rows"]
                    effective_skip_columns_raw = current_file_params["skip_columns"]
                    final_column_specs_for_table = current_file_params.get(
                        "column_definitions"
                    )
                    current_table_name_for_db = current_file_params["table_name"]
                    current_description_for_db = current_file_params["description"]

                    skip_rows_mode = parse_skip_rows_config(effective_skip_rows_raw)
                    skip_col_indices, skip_col_names = parse_skip_columns_config(
                        effective_skip_columns_raw
                    )

                    columns: Dict[str, Dict[str, Any]] = {}
                    column_keys: List[str] = []
                    data_rows: List[Dict[str, Any]] = []

                    if isinstance(skip_rows_mode, int):
                        (
                            columns,
                            column_keys,
                            data_rows,
                        ) = self._process_csv_skip_initial(
                            current_input_path,
                            final_encoding,
                            final_delimiter,
                            skip_rows_mode,
                            skip_col_indices,
                            skip_col_names,
                            current_has_header,
                            final_column_specs_for_table,
                        )
                    elif isinstance(skip_rows_mode, set):
                        (
                            columns,
                            column_keys,
                            data_rows,
                        ) = self._process_csv_skip_indexed(
                            current_input_path,
                            final_encoding,
                            final_delimiter,
                            skip_rows_mode,
                            skip_col_indices,
                            skip_col_names,
                            current_has_header,
                            final_column_specs_for_table,
                        )
                    else:
                        # This case should ideally not be reached if skip_rows_mode is validated.
                        raise TypeError(
                            f"Internal Error: Unexpected type for skip_rows_mode: {type(skip_rows_mode)}"
                        )

                    # --- SDIF Database Operations ---
                    if not columns and not data_rows:
                        logger.info(
                            f"No data processed for {current_input_path.name}. Adding source entry only."
                        )
                        db.add_source(
                            file_name=current_input_path.name, file_type="csv"
                        )
                    elif columns:
                        source_id = db.add_source(
                            file_name=current_input_path.name,
                            file_type="csv",
                        )

                        created_table_name_in_db = db.create_table(
                            table_name=current_table_name_for_db,
                            columns=columns,
                            source_id=source_id,
                            description=current_description_for_db,
                            if_exists="add",
                        )

                        if data_rows:
                            db.insert_data(
                                table_name=created_table_name_in_db,
                                data=data_rows,
                            )
                    elif not columns and data_rows:
                        logger.warning(
                            f"Data found for {current_input_path.name}, but no columns were determined. Adding as object."
                        )
                        source_id = db.add_source(
                            file_name=current_input_path.name, file_type="csv"
                        )
                        db.add_object(
                            object_name=current_table_name_for_db,
                            json_data=data_rows,
                            source_id=source_id,
                        )
                    # If neither columns nor data_rows, it's already handled by the first 'if' block.

                except FileNotFoundError as e_fnf:
                    logger.error(
                        f"File not found while processing {current_input_path.name}: {e_fnf}"
                    )
                    raise
                except (ValueError, TypeError, OSError, UnicodeDecodeError) as e_proc:
                    logger.error(
                        f"Error processing {current_input_path.name}: {e_proc}"
                    )
                    raise
                except Exception as e_unexpected:
                    logger.error(
                        f"Unexpected error processing {current_input_path.name}: {e_unexpected}",
                        exc_info=True,
                    )
                    raise

                file_configs_used[resolved_input_path_str] = current_file_params

        return StandardizationResult(
            output_path=Path(db.path).resolve(), file_configs=file_configs_used
        )

    def _gather_file_processing_parameters(
        self,
        input_path: Path,
        index: int,
        num_inputs: int,
        descriptions: List[Optional[str]],
        table_names: List[Optional[str]],
        file_configs: List[Optional[CSVFileConfig]],
        column_definitions_for_all_files: List[Optional[ColumnDefinitionsInput]],
    ) -> CSVFileConfig:
        """
        Gathers and resolves all effective parameters for processing a single CSV file.

        This includes defaults, overrides from `file_configs`, auto-detections for
        encoding/delimiter, table name generation/sanitization, and resolving
        the specific column definitions for the current file.

        Args:
            input_path: Path to the current CSV file.
            index: 0-based index of the current file in the input list.
            num_inputs: Total number of input files.
            descriptions: Normalized list of descriptions.
            table_names: Normalized list of table names (raw, pre-sanitize).
            file_configs: Normalized list of file-specific config overrides.
            column_definitions_for_all_files: Normalized list of column definition inputs.

        Returns:
            A CSVFileConfig dictionary containing all resolved parameters for the file.
        """
        current_file_params: Dict[str, Any] = {
            "file_path": str(input_path.resolve()),
        }

        current_config_override = file_configs[index] or {}

        effective_skip_rows_raw = validate_skip_rows_config(
            current_config_override.get("skip_rows", self.default_skip_rows),
            input_path.name,
        )
        effective_skip_columns_raw = validate_skip_columns_config(
            current_config_override.get("skip_columns", self.default_skip_columns),
            input_path.name,
        )
        current_file_params["skip_rows"] = effective_skip_rows_raw
        current_file_params["skip_columns"] = effective_skip_columns_raw

        current_file_params["has_header"] = current_config_override.get(
            "has_header", self.default_has_header
        )
        current_file_params["description"] = current_config_override.get(
            "description", descriptions[index]
        )

        # --- Resolve Table Name ---
        raw_table_name_from_config = current_config_override.get(
            "table_name", table_names[index]
        )
        is_single_global_name_multi_file = (
            isinstance(self.table_names, str) and num_inputs > 1
        )

        final_table_name_str: str
        if raw_table_name_from_config is None:
            # Generate from filename if no name provided for this file
            base_name = sanitize_sql_identifier(input_path.stem, "table")
            if (
                is_single_global_name_multi_file
                and index == 0
                and isinstance(self.table_names, str)
            ):
                # If a single global name was given, use it for the first file, then append index
                final_table_name_str = sanitize_sql_identifier(
                    self.table_names, f"table_{index}"
                )
            elif is_single_global_name_multi_file:
                base_name = sanitize_sql_identifier(
                    str(self.table_names), f"table_{index}"
                )
                # First file gets the base name only, subsequent files get _N suffix
                if index == 0:
                    final_table_name_str = base_name
                else:
                    final_table_name_str = f"{base_name}_{index}"
            else:  # General case: derive from filename, add index if multiple files
                if num_inputs > 1:
                    # For multiple files, first file gets plain name, others get _index suffix
                    if index == 0:
                        final_table_name_str = base_name
                    else:
                        final_table_name_str = f"{base_name}_{index}"
                else:
                    final_table_name_str = base_name

        elif is_single_global_name_multi_file and index > 0:
            # A global name was provided, and this is not the first file, so append index
            final_table_name_str = sanitize_sql_identifier(
                str(raw_table_name_from_config), f"table_{index}"
            )
            # Only append index to tables after the first one when using a single global name
            if index > 0:
                final_table_name_str = f"{final_table_name_str}_{index}"
        else:
            # A specific name was provided (either globally for single file, or per-file)
            final_table_name_str = sanitize_sql_identifier(
                str(raw_table_name_from_config), f"table_{index}"
            )
        current_file_params["table_name"] = final_table_name_str

        # --- Auto-Detect Encoding/Delimiter if needed ---
        current_encoding_override = current_config_override.get(
            "encoding", self.default_encoding
        )
        final_encoding: str
        if current_encoding_override is None:
            try:
                final_encoding = detect_file_encoding(input_path)
                logger.info(
                    f"Auto-detected encoding for {input_path.name}: {final_encoding}"
                )
            except Exception as e_enc:
                logger.warning(
                    f"Encoding detection for {input_path.name} failed: {e_enc}. Using fallback 'utf-8'."
                )
                final_encoding = "utf-8"
        else:
            final_encoding = current_encoding_override
        current_file_params["encoding"] = final_encoding

        current_delimiter_override = current_config_override.get(
            "delimiter", self.default_delimiter
        )
        final_delimiter: str
        # Make sure direct override in file_configs takes precedence over the default_delimiter
        if current_config_override.get("delimiter") is not None:
            final_delimiter = current_config_override["delimiter"]
        elif current_delimiter_override is None:
            try:
                with open(
                    input_path, encoding=final_encoding, errors="ignore"
                ) as f_sample:
                    sample_text = f_sample.read(DELIMITER_SAMPLE_SIZE)
                if sample_text:
                    final_delimiter = detect_csv_delimiter(sample_text)
                    logger.info(
                        f"Auto-detected delimiter for {input_path.name}: '{final_delimiter}'"
                    )
                else:
                    final_delimiter = ","
                    logger.warning(
                        f"File {input_path.name} is empty or very small; defaulting delimiter to ','."
                    )
            except Exception as e_delim:
                logger.warning(
                    f"Delimiter detection for {input_path.name} failed: {e_delim}. Using fallback ','."
                )
                final_delimiter = ","
        else:
            final_delimiter = current_delimiter_override
        current_file_params["delimiter"] = final_delimiter

        # --- Resolve Column Definitions for this file ---
        current_column_definitions = column_definitions_for_all_files[index]
        final_column_specs_for_table: Optional[List[ColumnSpec]] = None

        if isinstance(current_column_definitions, dict):
            # It's a map of table_name -> List[ColumnSpec]
            final_column_specs_for_table = current_column_definitions.get(
                final_table_name_str
            )
            if final_column_specs_for_table is None:
                logger.warning(
                    f"Column definitions were provided as a map for file '{input_path.name}', "
                    f"but no entry found for resolved table name '{final_table_name_str}'. "
                    f"Available keys: {list(current_column_definitions.keys())}. "
                    "Proceeding with default column handling for this table."
                )
        elif isinstance(current_column_definitions, list):
            # It's a direct List[ColumnSpec] for this file's table
            final_column_specs_for_table = current_column_definitions

        if final_column_specs_for_table is not None:
            current_file_params["column_definitions"] = final_column_specs_for_table
        # If None, key "column_definitions" will be absent from current_file_params, handled by _setup_columns

        return current_file_params

    def _resolve_skip_columns_indices(
        self,
        skip_col_indices: Set[int],
        skip_col_names: Set[str],
        raw_headers: List[str],
        has_header: bool,
        file_name: str,
    ) -> Set[int]:
        """
        Resolves column names to indices based on `raw_headers` and combines them
        with numerically specified skip indices. Performs case-insensitive fallback
        for name matching if an exact match fails.

        Args:
            skip_col_indices: Set of 0-based column indices to skip.
            skip_col_names: Set of column names to skip.
            raw_headers: The list of header strings from the CSV (or generated column names).
            has_header: Boolean indicating if `raw_headers` are from an actual header row.
            file_name: Name of the CSV file, for logging purposes.

        Returns:
            A set of resolved 0-based column indices to be skipped.

        Raises:
            ValueError: If column names are specified for skipping but `has_header` is False,
                        or if specified names cannot be found (even with case-insensitive match),
                        or if any resolved skip index is out of bounds.
        """
        final_skip_indices = set(skip_col_indices)
        num_raw_columns = len(raw_headers)

        if skip_col_names:
            if not has_header:
                raise ValueError(
                    f"Cannot skip columns by name ({skip_col_names}) when has_header=False (file: {file_name}). "
                    "Please provide column indices or ensure has_header=True."
                )

            if (
                not raw_headers
            ):  # Should not happen if has_header=True and a header was read
                raise ValueError(
                    f"Cannot resolve column names ({skip_col_names}) because no header row was found or processed (file: {file_name})."
                )

            header_map = {name: idx for idx, name in enumerate(raw_headers)}
            unresolved_names = set()

            for name in skip_col_names:
                if name in header_map:
                    final_skip_indices.add(header_map[name])
                else:
                    unresolved_names.add(name)

            if unresolved_names:
                # Try case-insensitive match as a fallback
                header_map_lower = {
                    name.lower(): idx for idx, name in enumerate(raw_headers)
                }
                still_unresolved_after_fallback = set()
                for name_to_resolve in unresolved_names:
                    lower_name = name_to_resolve.lower()
                    if lower_name in header_map_lower:
                        resolved_idx = header_map_lower[lower_name]
                        final_skip_indices.add(resolved_idx)
                        logger.warning(
                            f"Resolved column name '{name_to_resolve}' case-insensitively to '{raw_headers[resolved_idx]}' "
                            f"for skipping in file: {file_name}."
                        )
                    else:
                        still_unresolved_after_fallback.add(name_to_resolve)

                if still_unresolved_after_fallback:
                    raise ValueError(
                        f"Could not find column name(s) specified in skip_columns: {still_unresolved_after_fallback} "
                        f"(available headers: {raw_headers}) (file: {file_name})."
                    )

        # Final check: ensure all resolved indices (both numeric and from names) are within bounds
        if any(idx >= num_raw_columns for idx in final_skip_indices):
            invalid_indices = {
                idx for idx in final_skip_indices if idx >= num_raw_columns
            }
            raise ValueError(
                f"Skip column index/indices out of bounds: {invalid_indices}. "
                f"File '{file_name}' has {num_raw_columns} columns (0-indexed)."
            )
        return final_skip_indices

    def _setup_columns(
        self,
        raw_headers: List[str],
        skip_col_indices: Set[int],
        skip_col_names: Set[str],
        has_header: bool,
        file_name: str,
        defined_columns_spec: Optional[List[ColumnSpec]] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], Dict[int, int]]:
        """
        Initializes column structures based on either provided `defined_columns_spec`
        or by deriving from `raw_headers` considering skip configurations.

        Args:
            raw_headers: List of header strings from CSV or generated names.
            skip_col_indices: Set of 0-based column indices to skip (if not using defined_columns_spec).
            skip_col_names: Set of column names to skip (if not using defined_columns_spec).
            has_header: True if `raw_headers` are from an actual CSV header.
            file_name: Name of the CSV file, for logging.
            defined_columns_spec: Optional list of `ColumnSpec` dicts. If provided,
                                  this specification takes precedence for defining columns.

        Returns:
            A tuple: (columns_dict, column_keys_list, column_index_map).
            - `columns_dict`: Maps final sanitized column names to their properties (type, description, original_name).
            - `column_keys_list`: Ordered list of final sanitized column names.
            - `column_index_map`: Maps original 0-based CSV column index to the 0-based index in `column_keys_list`.
        """
        columns: Dict[str, Dict[str, Any]] = {}
        column_keys: List[str] = []
        # Maps raw_csv_index to final_column_keys_index
        col_idx_map: Dict[int, int] = {}

        if defined_columns_spec:
            # Mode 1: Columns are predefined by defined_columns_spec
            raw_header_name_to_idx_map: Dict[str, int] = (
                {header_val.lower(): idx for idx, header_val in enumerate(raw_headers)}
                if has_header and raw_headers
                else {}
            )

            raw_header_idx_to_original_name_map: Dict[int, str] = {
                idx: val for idx, val in enumerate(raw_headers)
            }

            final_idx_counter = 0
            for col_spec in defined_columns_spec:
                original_identifier = col_spec.get("original_identifier", "").strip()
                final_name_in_spec = col_spec.get("final_column_name", "").strip()
                description = col_spec.get("description")  # Optional

                if not final_name_in_spec:
                    logger.warning(
                        f"Skipping column spec in {file_name} due to missing 'final_column_name': {col_spec}"
                    )
                    continue
                if not original_identifier:
                    logger.warning(
                        f"Skipping column spec for '{final_name_in_spec}' in {file_name} "
                        f"due to missing 'original_identifier': {col_spec}"
                    )
                    continue

                original_csv_idx: Optional[int] = None
                # Try to match original_identifier by name if headers exist
                if has_header and raw_headers:
                    original_csv_idx = raw_header_name_to_idx_map.get(
                        original_identifier.lower()
                    )

                # If not found by name (or no header), try as positional index
                if original_csv_idx is None:
                    try:
                        pos_idx = int(original_identifier)
                        if 0 <= pos_idx < len(raw_headers):
                            original_csv_idx = pos_idx
                        else:
                            logger.warning(
                                f"Positional original_identifier '{original_identifier}' for column spec "
                                f"'{final_name_in_spec}' is out of bounds (0-{len(raw_headers) - 1}) "
                                f"in {file_name}. Skipping this spec."
                            )
                            continue
                    except (
                        ValueError
                    ):  # original_identifier is not a valid integer string
                        if has_header:
                            logger.warning(
                                f"Could not find header '{original_identifier}' (for column spec "
                                f"'{final_name_in_spec}') in CSV headers of {file_name} "
                                f"({raw_headers}), and it's not a valid index. Skipping this spec."
                            )
                        else:  # No header, and not a valid index
                            logger.warning(
                                f"original_identifier '{original_identifier}' (for column spec "
                                f"'{final_name_in_spec}') is not a valid positional index for "
                                f"headerless CSV in {file_name}. Max index is {len(raw_headers) - 1}. "
                                f"Skipping this spec."
                            )
                        continue

                if original_csv_idx is None:  # Should be caught above, but as safeguard
                    logger.warning(
                        f"Failed to resolve original_identifier '{original_identifier}' for column spec "
                        f"'{final_name_in_spec}' in {file_name}. Skipping."
                    )
                    continue

                # Determine the actual original name from the CSV based on the resolved index
                actual_original_name_from_csv = raw_header_idx_to_original_name_map.get(
                    original_csv_idx, f"column_{original_csv_idx}"
                )

                # Sanitize the final_name_in_spec for SQL compatibility
                final_column_name_sanitized = sanitize_sql_identifier(
                    final_name_in_spec, f"col_{final_idx_counter}"
                )
                if final_column_name_sanitized != final_name_in_spec:
                    logger.info(
                        f"Sanitized final column name from spec '{final_name_in_spec}' to '{final_column_name_sanitized}' for table from {file_name}."
                    )

                column_keys.append(final_column_name_sanitized)
                columns[final_column_name_sanitized] = {
                    "type": "TEXT",  # Initial type, will be inferred later
                    "description": description,
                    "original_column_name": actual_original_name_from_csv,
                }
                col_idx_map[original_csv_idx] = final_idx_counter
                final_idx_counter += 1

            if not columns:
                logger.warning(
                    f"No columns determined for {file_name} from defined_columns_spec. "
                    f"Raw headers (if any): {raw_headers}. Spec was: {defined_columns_spec}"
                )
                return {}, [], {}
        else:
            # Mode 2: Derive columns from raw_headers and skip_configs (legacy/default behavior)
            if (
                not raw_headers
            ):  # This can happen if the file is empty or only contains skipped rows
                logger.warning(
                    f"No raw headers available to derive columns for {file_name} (e.g., empty file or all rows skipped pre-header). "
                    "No columns will be created."
                )
                return {}, [], {}

            final_skip_column_indices = self._resolve_skip_columns_indices(
                skip_col_indices, skip_col_names, raw_headers, has_header, file_name
            )

            col_name_counts: Dict[str, int] = {}  # For de-duplicating sanitized names
            final_idx_counter = 0
            for original_idx, header_val_from_csv in enumerate(raw_headers):
                if original_idx in final_skip_column_indices:
                    continue

                base_col_name = sanitize_sql_identifier(
                    header_val_from_csv, f"column_{original_idx}"
                )
                final_column_name_sanitized = base_col_name

                count = col_name_counts.get(base_col_name, 0) + 1
                col_name_counts[base_col_name] = count
                if (
                    count > 1
                ):  # Sanitize by appending suffix if name collision after initial sanitization
                    final_column_name_sanitized = (
                        f"{base_col_name}_{count - 1}"  # e.g. name, name_1, name_2
                    )

                column_keys.append(final_column_name_sanitized)
                columns[final_column_name_sanitized] = {
                    "type": "TEXT",  # Initial type
                    "original_column_name": header_val_from_csv,
                    "description": None,  # No description in this mode unless added later
                }
                col_idx_map[original_idx] = final_idx_counter
                final_idx_counter += 1

            if not columns:
                logger.warning(
                    f"No columns determined for {file_name} after applying exclusions. "
                    f"Raw headers: {raw_headers}, Skip indices: {final_skip_column_indices}"
                )
                return {}, [], {}

        return columns, column_keys, col_idx_map

    def _infer_column_types(
        self, sample_data: List[Dict[str, str]], column_keys: List[str]
    ) -> Dict[str, str]:
        """
        Infers SQLite data types (INTEGER, REAL, TEXT) for columns based on sample data.

        Args:
            sample_data: A list of dictionaries, where each dictionary represents a row
                         and keys are final column names. Values are strings.
            column_keys: An ordered list of final column names for which to infer types.

        Returns:
            A dictionary mapping final column names to their inferred SQLite type string.
        """
        # Initialize potential types for each column key
        potential_types: Dict[str, Set[str]] = {
            key: {"INTEGER", "REAL", "TEXT"} for key in column_keys
        }

        if not sample_data:
            return {key: "TEXT" for key in column_keys}  # Default to TEXT if no sample

        for row in sample_data:
            for col_key in column_keys:
                # Skip if this column's type is already definitively TEXT or no longer being considered
                current_col_potentials = potential_types.get(col_key)
                if not current_col_potentials or current_col_potentials == {"TEXT"}:
                    continue

                value_str = row.get(col_key)
                if (
                    value_str is None or value_str == ""
                ):  # Missing or empty values are compatible with any type initially
                    continue

                # Try to narrow down types
                if "INTEGER" in current_col_potentials:
                    try:
                        int(value_str)
                    except ValueError:
                        current_col_potentials.discard("INTEGER")

                if "REAL" in current_col_potentials:
                    try:
                        # If it could have been an INTEGER but wasn't, or if INTEGER was already ruled out
                        float_val = float(value_str)
                        # If it's a float but represents a whole number (e.g., "1.0"),
                        # and INTEGER is still a possibility, don't discard INTEGER yet.
                        # Only discard INTEGER if it's clearly not an integer (e.g. "1.5").
                        if (
                            "INTEGER" in current_col_potentials
                            and not float_val.is_integer()
                        ):
                            current_col_potentials.discard("INTEGER")
                    except ValueError:
                        current_col_potentials.discard("REAL")
                        # If it failed float and previously failed int, it must be TEXT
                        if (
                            "INTEGER" not in current_col_potentials
                        ):  # if INTEGER was already discarded
                            current_col_potentials.clear()
                            current_col_potentials.add("TEXT")

                # If after checks, only TEXT remains or all are discarded (error), default to TEXT
                if not current_col_potentials - {
                    "TEXT"
                }:  # set difference: if empty or only TEXT
                    potential_types[col_key] = {"TEXT"}

        # Determine final type: INTEGER > REAL > TEXT
        final_types: Dict[str, str] = {}
        for col_key, potentials_set in potential_types.items():
            if "INTEGER" in potentials_set:
                final_types[col_key] = "INTEGER"
            elif "REAL" in potentials_set:
                final_types[col_key] = "REAL"
            else:  # Default or only option left
                final_types[col_key] = "TEXT"
        return final_types

    def _perform_type_inference(
        self,
        sample_data: List[Dict[str, str]],  # Values should be strings as read from CSV
        columns: Dict[str, Dict[str, Any]],  # Column definitions to update
        column_keys: List[str],  # Ordered list of keys in `columns`
        file_name: str,
        # context_msg: str, # No longer used
    ):
        """
        Performs type inference on sample data and updates the 'type' field
        in the `columns` dictionary.

        Args:
            sample_data: List of sample row data (dictionaries of string values).
            columns: The dictionary of column definitions to be updated in-place.
            column_keys: Ordered list of final column names.
            file_name: Name of the CSV file, for logging.
        """
        if not sample_data or not column_keys:
            logger.info(
                f"No sample data or column keys for type inference in {file_name}. Types will default to TEXT."
            )
            # Ensure all existing columns default to TEXT if not already set
            for key in column_keys:
                if key in columns and "type" not in columns[key]:
                    columns[key]["type"] = "TEXT"
            return

        inferred_sqlite_types = self._infer_column_types(sample_data, column_keys)

        for col_key, sqlite_type in inferred_sqlite_types.items():
            if col_key in columns:
                if columns[col_key].get("type") != sqlite_type:  # Update if different
                    original_type = columns[col_key].get("type", "None")
                    columns[col_key]["type"] = sqlite_type
                    logger.debug(
                        f"Type for column '{col_key}' in {file_name} inferred as {sqlite_type} (was {original_type})."
                    )
            else:  # Should not happen if column_keys is derived from columns
                logger.warning(
                    f"Column '{col_key}' found in inference results but not in column definitions for {file_name}."
                )

    def _parse_row(
        self,
        row_fields: List[str],
        col_idx_map: Dict[int, int],
        column_keys: List[str],
        expected_raw_len: int,
        file_name: str,
        row_num_for_logging: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Parses a list of raw string fields from a CSV row into a dictionary,
        mapping fields to final column names based on `col_idx_map`.

        Args:
            row_fields: List of string values from a single CSV row.
            col_idx_map: Maps original 0-based CSV column index to the
                         0-based index in `column_keys`.
            column_keys: Ordered list of final (sanitized) column names.
            expected_raw_len: The number of columns expected based on the header
                              or first processed data row. Used for warnings.
            file_name: Name of the CSV file, for logging.
            row_num_for_logging: 1-based row number in the original file, for logging.

        Returns:
            A dictionary mapping final column names to their string values for the row,
            or None if the row is effectively empty after considering included columns
            or if it's an unusable blank line.
        """
        row_len = len(row_fields)
        if row_len != expected_raw_len:
            # Log only if it's not just a completely blank line that's shorter
            # or if it's longer (which is always a mismatch)
            is_blank_line = not any(field.strip() for field in row_fields)
            if row_len > expected_raw_len or (
                row_len < expected_raw_len and not is_blank_line
            ):
                logger.warning(
                    f"Row {row_num_for_logging} in {file_name} has {row_len} columns, "
                    f"expected {expected_raw_len} based on header/first row. "
                    f"{'Extra data ignored.' if row_len > expected_raw_len else 'Missing values treated as NULL.'}"
                )

        # If the row is entirely empty strings or whitespace, and we expected columns, treat as skippable null row.
        if expected_raw_len > 0 and not any(field.strip() for field in row_fields):
            logger.debug(
                f"Skipping effectively blank row {row_num_for_logging} in {file_name}."
            )
            return None

        row_dict: Dict[str, Any] = {}
        valid_data_found_in_selected_columns = False
        for original_idx, value_str in enumerate(row_fields):
            if (
                original_idx in col_idx_map
            ):  # If this original column is part of the final set
                final_key_index = col_idx_map[original_idx]
                # Ensure final_key_index is valid for column_keys (defensive)
                if final_key_index < len(column_keys):
                    final_key = column_keys[final_key_index]
                    row_dict[final_key] = value_str
                    if value_str is not None and value_str.strip() != "":
                        valid_data_found_in_selected_columns = True
                else:  # Should not happen with correct col_idx_map
                    logger.error(
                        f"Internal error: Column index mapping issue for row {row_num_for_logging} in {file_name}. "
                        f"Original index {original_idx} mapped to final {final_key_index}, "
                        f"but only {len(column_keys)} final keys exist."
                    )
                    # Potentially skip row or raise, for now, it will have fewer keys.

        # Return row_dict if it has data in selected columns, or if it's an expected empty row (all nulls for selected cols)
        if valid_data_found_in_selected_columns or len(row_dict) == len(column_keys):
            return row_dict

        # If no data was found in any of the *selected* columns, this row is effectively empty for our purposes.
        logger.debug(
            f"Skipping row {row_num_for_logging} in {file_name} as it's empty or all its data is in skipped columns."
        )
        return None

    def _process_csv_skip_initial(
        self,
        input_path: Path,
        encoding: str,
        delimiter: str,
        initial_skip_count: int,
        skip_col_indices: Set[int],
        skip_col_names: Set[str],
        has_header: bool,
        defined_columns_spec: Optional[List[ColumnSpec]] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[Dict[str, Any]]]:
        """
        Processes a CSV file where the first `initial_skip_count` rows are skipped,
        then optionally a header is read, followed by data rows.

        Args:
            input_path: Path to the CSV file.
            encoding: File encoding.
            delimiter: CSV delimiter.
            initial_skip_count: Number of initial rows to skip.
            skip_col_indices: Set of 0-based column indices to exclude.
            skip_col_names: Set of column names to exclude.
            has_header: True if the first non-skipped line (after initial_skip_count) is a header.
            defined_columns_spec: Optional explicit column definitions.

        Returns:
            Tuple: (columns_dict, column_keys_list, data_rows_list)
        """
        file_name = input_path.name
        data_rows: List[Dict[str, Any]] = []
        columns: Dict[str, Dict[str, Any]] = {}
        column_keys: List[str] = []
        raw_headers_from_file: List[str] = []
        col_idx_map: Dict[int, int] = {}
        sample_data_for_inference: List[Dict[str, str]] = []  # Store as string dicts

        with open(input_path, encoding=encoding, newline="") as f:
            # Phase 1: Skip initial rows and find first meaningful line for header/data
            actual_lines_read_for_skip = 0
            header_candidate_line_content: Optional[str] = None
            file_pos_after_header_candidate = f.tell()

            try:
                for i in range(initial_skip_count):
                    line = f.readline()
                    if not line:  # EOF reached during initial skip
                        logger.warning(
                            f"EOF reached in {file_name} while skipping initial {initial_skip_count} rows (skipped {i}). No data processed."
                        )
                        return {}, [], []
                    actual_lines_read_for_skip += 1

                # After initial_skip_count, read until a non-blank line or EOF
                while True:
                    line = f.readline()
                    actual_lines_read_for_skip += 1  # Count this line attempt
                    if not line:  # EOF
                        logger.warning(
                            f"EOF reached in {file_name} after skipping {initial_skip_count} initial rows and subsequent blank lines. No data processed."
                        )
                        return {}, [], []
                    if line.strip():  # Found a non-blank line
                        header_candidate_line_content = line
                        file_pos_after_header_candidate = f.tell()
                        break
                    # else: it's a blank line, continue loop

                num_blank_lines_after_initial_skip = (
                    actual_lines_read_for_skip - 1
                ) - initial_skip_count
                if num_blank_lines_after_initial_skip > 0:
                    logger.info(
                        f"Skipped {num_blank_lines_after_initial_skip} blank line(s) after the initial {initial_skip_count} skips in {file_name}."
                    )

            except Exception as e_skip:  # Should be rare here
                logger.error(
                    f"Error during initial row skipping phase in {file_name}: {e_skip}",
                    exc_info=True,
                )
                raise ValueError(
                    f"Failed during initial row skipping in {file_name}"
                ) from e_skip

            # Phase 2: Parse the header_candidate_line to get raw column fields
            if (
                header_candidate_line_content is None
            ):  # Should be caught by EOF checks above
                logger.warning(
                    f"No content found in {file_name} after initial skips. No data processed."
                )
                return {}, [], []

            try:
                # Use csv.reader on a single-element list containing the line string
                parsed_header_candidate_fields = next(
                    csv.reader([header_candidate_line_content], delimiter=delimiter)
                )
            except (
                StopIteration
            ):  # Should not happen with non-None header_candidate_line_content
                logger.warning(
                    f"CSV parser found no fields in the first non-skipped line of {file_name}. No data processed."
                )
                return {}, [], []
            except Exception as e_parse_header:
                logger.error(
                    f"Error parsing first non-skipped line as CSV in {file_name} with delimiter '{delimiter}': {e_parse_header}",
                    exc_info=True,
                )
                raise ValueError(
                    f"Failed to parse header/first data line in {file_name}"
                ) from e_parse_header

            num_raw_cols_in_first_row = len(parsed_header_candidate_fields)
            first_data_row_original_index = (
                actual_lines_read_for_skip - 1
            )  # 0-indexed line number of header_candidate_line

            if has_header:
                raw_headers_from_file = parsed_header_candidate_fields
                first_data_row_original_index += (
                    1  # Actual data starts on the next line
                )
            else:  # No header, so parsed_header_candidate_fields is the first data row
                raw_headers_from_file = [
                    f"column_{j}" for j in range(num_raw_cols_in_first_row)
                ]
                # The parsed_header_candidate_fields will be processed as the first data row later

            # Phase 3: Setup columns based on raw_headers_from_file and specs
            columns, column_keys, col_idx_map = self._setup_columns(
                raw_headers_from_file,
                skip_col_indices,
                skip_col_names,
                has_header,
                file_name,
                defined_columns_spec,
            )
            if not column_keys:  # No usable columns defined
                logger.warning(
                    f"No columns determined for {file_name} in indexed skip mode. No data will be processed."
                )
                return {}, [], []  # Abort if no columns

            # Phase 4: Read and parse data rows (including sampling for type inference)
            # Reset file pointer to start reading data rows
            f.seek(
                file_pos_after_header_candidate
                if has_header
                else f.tell() - len(header_candidate_line_content.encode(encoding))
            )  # Rewind if no_header

            csv_reader_for_data = csv.reader(f, delimiter=delimiter)

            # Handle first data row if no_header=True (it was header_candidate_line)
            if not has_header:
                current_row_log_num = (
                    first_data_row_original_index + 1
                )  # 1-based for logging
                parsed_row = self._parse_row(
                    parsed_header_candidate_fields,
                    col_idx_map,
                    column_keys,
                    num_raw_cols_in_first_row,
                    file_name,
                    current_row_log_num,
                )
                if parsed_row:
                    if len(sample_data_for_inference) < SAMPLE_SIZE:
                        sample_data_for_inference.append(
                            parsed_row
                        )  # Already string dict
                    data_rows.append(parsed_row)

            # Process remaining rows for data and sampling
            for i, row_fields in enumerate(csv_reader_for_data):
                # current_row_original_index is 0-based index from start of file
                current_row_original_index = first_data_row_original_index + i
                current_row_log_num = (
                    current_row_original_index + 1
                )  # 1-based for logging

                parsed_row = self._parse_row(
                    row_fields,
                    col_idx_map,
                    column_keys,
                    num_raw_cols_in_first_row,
                    file_name,
                    current_row_log_num,
                )
                if parsed_row:
                    if len(sample_data_for_inference) < SAMPLE_SIZE:
                        sample_data_for_inference.append(
                            parsed_row
                        )  # Already string dict
                    data_rows.append(parsed_row)

            # Phase 5: Perform type inference
            if (
                sample_data_for_inference
            ):  # sample_data_for_inference contains Dict[str, str]
                self._perform_type_inference(
                    sample_data_for_inference, columns, column_keys, file_name
                )
            elif (
                data_rows
            ):  # Some data but not enough for a full sample (or sample_size=0)
                logger.info(
                    f"Not enough distinct sample data for full type inference in {file_name}; types may remain TEXT or be based on limited data."
                )
                self._perform_type_inference(
                    data_rows[:SAMPLE_SIZE], columns, column_keys, file_name
                )  # Try with what we have
            else:  # No data rows processed, but columns might exist
                logger.info(
                    f"No data rows processed for {file_name}; type inference skipped. Column types will default to TEXT."
                )
                for key in column_keys:
                    columns[key]["type"] = "TEXT"

        return columns, column_keys, data_rows

    def _resolve_skip_indices_set(
        self,
        input_path: Path,
        encoding: str,
        delimiter: str,
        skip_row_indices_set: Set[int],  # Can contain negative indices
        file_name: str,
    ) -> Set[int]:
        """
        Resolves a set of row indices to skip, supporting negative indices
        (where -1 is the last row, -2 is second to last, etc.).

        Args:
            input_path: Path to the CSV file.
            encoding: File encoding.
            delimiter: CSV delimiter.
            skip_row_indices_set: The initial set of 0-based indices, possibly containing negatives.
            file_name: Name of the file (for logging/error messages).

        Returns:
            A set of resolved 0-based positive row indices to skip.
            If negative indices are used, this involves a preliminary pass through the file
            to count total rows, which has a performance implication.
        """
        if not any(idx < 0 for idx in skip_row_indices_set):
            # No negative indices, just filter out any potentially invalid non-negative ones (e.g. if set was empty initially)
            return {idx for idx in skip_row_indices_set if idx >= 0}

        logger.info(
            f"Negative skip indices detected for {file_name}. Counting total rows (this may take time for large files)."
        )
        total_rows = 0
        try:
            with open(input_path, encoding=encoding, newline="") as f_count:
                # Simple line count is faster and safer than full CSV parsing for this purpose
                for _ in f_count:  # Iterating over file object directly counts lines
                    total_rows += 1
        except Exception as e_count:
            logger.error(
                f"Error counting rows in {file_name} for negative skip index resolution: {e_count}",
                exc_info=True,
            )
            raise ValueError(
                f"Failed to count rows in {file_name} for negative skip indices"
            ) from e_count

        if total_rows == 0:
            logger.warning(
                f"File {file_name} is empty. Negative skip indices cannot be resolved and will be ignored."
            )
            return {
                idx for idx in skip_row_indices_set if idx >= 0
            }  # Only positive ones, if any

        resolved_skip_set = set()
        for idx in skip_row_indices_set:
            if idx >= 0:
                if idx < total_rows:
                    resolved_skip_set.add(idx)
                else:
                    logger.warning(
                        f"Positive skip index {idx} is out of bounds for file {file_name} "
                        f"(total rows: {total_rows}). It will be ignored."
                    )
            else:  # Negative index
                # e.g., idx = -1, total_rows = 10 -> positive_equivalent = 10 + (-1) = 9 (correct 0-based index for last row)
                positive_equivalent = total_rows + idx
                if 0 <= positive_equivalent < total_rows:
                    resolved_skip_set.add(positive_equivalent)
                else:
                    logger.warning(
                        f"Negative skip index {idx} (resolves to {positive_equivalent}) "
                        f"is out of bounds for file {file_name} (total rows: {total_rows}). "
                        f"It will be ignored."
                    )
        logger.debug(
            f"For {file_name}, initial skip_row_indices {skip_row_indices_set} resolved to {resolved_skip_set} with {total_rows} total rows."
        )
        return resolved_skip_set

    def _process_csv_skip_indexed(
        self,
        input_path: Path,
        encoding: str,
        delimiter: str,
        skip_row_indices_to_resolve: Set[int],  # May contain negative indices
        skip_col_indices: Set[int],
        skip_col_names: Set[str],
        has_header: bool,
        defined_columns_spec: Optional[List[ColumnSpec]] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[Dict[str, Any]]]:
        """
        Processes a CSV file by skipping rows based on a specific set of 0-based indices
        (which can include negative indices to count from the end).

        Args:
            input_path: Path to the CSV file.
            encoding: File encoding.
            delimiter: CSV delimiter.
            skip_row_indices_to_resolve: Set of 0-based row indices to skip (may include negatives).
            skip_col_indices: Set of 0-based column indices to exclude.
            skip_col_names: Set of column names to exclude.
            has_header: True if a header row is expected (and should be processed if not skipped).
            defined_columns_spec: Optional explicit column definitions.

        Returns:
            Tuple: (columns_dict, column_keys_list, data_rows_list)
        """
        file_name = input_path.name
        data_rows: List[Dict[str, Any]] = []
        columns: Dict[str, Dict[str, Any]] = {}
        column_keys: List[str] = []
        raw_headers_from_file: List[str] = []
        col_idx_map: Optional[Dict[int, int]] = (
            None  # Becomes Dict once columns are set up
        )
        sample_data_for_inference: List[Dict[str, str]] = []  # Store as string dicts

        # Resolve skip_row_indices_set first, potentially reading the file for negative indices
        final_skip_row_indices = self._resolve_skip_indices_set(
            input_path, encoding, delimiter, skip_row_indices_to_resolve, file_name
        )

        columns_determined = False
        header_row_processed_if_exists = (
            False  # True if has_header and the header line is consumed
        )
        num_cols_from_first_row_for_structure = (
            0  # Based on first *processed* row if no header
        )

        with open(input_path, encoding=encoding, newline="") as f:
            csv_reader = csv.reader(f, delimiter=delimiter)

            for current_row_0_idx, row_fields in enumerate(csv_reader):
                if current_row_0_idx in final_skip_row_indices:
                    logger.debug(
                        f"Skipping row {current_row_0_idx + 1} in {file_name} due to indexed skip."
                    )
                    continue  # Skip this row based on index

                current_row_log_num = current_row_0_idx + 1  # 1-based for logging

                # If columns not yet determined, this row is the first candidate
                if not columns_determined:
                    if not row_fields and not any(
                        field.strip() for field in row_fields
                    ):  # Skip truly empty lines before header
                        logger.debug(
                            f"Skipping empty row {current_row_log_num} in {file_name} before header/column setup."
                        )
                        continue

                    num_cols_from_first_row_for_structure = len(row_fields)

                    if has_header and not header_row_processed_if_exists:
                        raw_headers_from_file = row_fields
                        header_row_processed_if_exists = True  # Consumed header
                    elif not raw_headers_from_file:  # No header, or header already processed (but shouldn't be if here)
                        # Generate default headers based on the first data row encountered
                        raw_headers_from_file = [
                            f"column_{j}"
                            for j in range(num_cols_from_first_row_for_structure)
                        ]

                    # Setup columns structure (names, types, skip_map)
                    _columns, _column_keys, _col_idx_map = self._setup_columns(
                        raw_headers_from_file,
                        skip_col_indices,
                        skip_col_names,
                        has_header,
                        file_name,
                        defined_columns_spec,
                    )
                    if not _column_keys:  # No usable columns defined
                        logger.warning(
                            f"No columns determined for {file_name} in indexed skip mode. No data will be processed."
                        )
                        return {}, [], []  # Abort if no columns

                    columns, column_keys, col_idx_map = (
                        _columns,
                        _column_keys,
                        _col_idx_map,
                    )
                    columns_determined = True

                    # If this row was the header, continue to next row for data
                    if (
                        has_header
                        and header_row_processed_if_exists
                        and current_row_0_idx not in final_skip_row_indices
                    ):  # Check skip again for header row itself
                        continue  # Don't process the header row as data

                # Process the row as data if columns are determined
                if columns_determined and col_idx_map is not None:
                    parsed_row = self._parse_row(
                        row_fields,
                        col_idx_map,
                        column_keys,
                        num_cols_from_first_row_for_structure,
                        file_name,
                        current_row_log_num,
                    )
                    if parsed_row:
                        if len(sample_data_for_inference) < SAMPLE_SIZE:
                            sample_data_for_inference.append(
                                parsed_row
                            )  # Already string dict
                        data_rows.append(parsed_row)

            # After iterating through all rows
            if columns_determined:  # If we at least set up columns
                if sample_data_for_inference:
                    self._perform_type_inference(
                        sample_data_for_inference, columns, column_keys, file_name
                    )
                elif data_rows:  # Some data but not enough for sample
                    self._perform_type_inference(
                        data_rows[:SAMPLE_SIZE], columns, column_keys, file_name
                    )
                else:  # No data rows processed, but columns might exist
                    logger.info(
                        f"No data rows processed for {file_name} (indexed skip mode); type inference skipped. Column types will default to TEXT."
                    )
                    for key in column_keys:
                        columns[key]["type"] = "TEXT"
            else:  # No rows processed at all (e.g. file empty or all rows skipped)
                logger.warning(
                    f"No processable rows found in {file_name} (indexed skip mode)."
                )

        return columns, column_keys, data_rows
