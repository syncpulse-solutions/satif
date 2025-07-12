import datetime
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
from satif_core import Standardizer
from satif_core.types import Datasource, FileConfig, SDIFPath, StandardizationResult
from sdif_db import SDIFDatabase

from satif_sdk.utils import (
    ColumnDefinitionsConfig,
    ColumnSpec,
    normalize_list_argument,
    sanitize_sql_identifier,
)

# Constants
DEFAULT_SHEET = 0
DEFAULT_HEADER_ROW = 0
DEFAULT_SKIP_ROWS = 0

# Setup basic logging
logger = logging.getLogger(__name__)


class XLSXFileConfig(FileConfig):
    """Configuration settings applied to a single XLSX file during standardization."""

    file_path: str
    sheet_name: Union[str, int]
    actual_sheet_name: str
    header_row: int
    skip_rows: int
    skip_columns: Set[Union[str, int]]
    column_definitions: Optional[List[ColumnSpec]]
    description: Optional[str]
    table_name: str


# TODO: better handling of sheets


class XLSXStandardizer(Standardizer):
    """
    Standardizer for one or multiple Excel (.xlsx) files/sheets into an SDIF database.

    Transforms data from specified sheets within Excel files into the SDIF format.
    Default options (sheet_name/index, header_row, skip_rows, skip_columns)
    are set during initialization. These defaults can be overridden on a per-file basis
    when calling the `standardize` method using the `file_configs` parameter.
    Infers SQLite types (INTEGER, REAL, TEXT) from pandas dtypes.

    If `column_definitions` are provided for a file, they take precedence for selecting,
    renaming, and describing columns. Otherwise, headers are taken from the Excel sheet
    (respecting `header_row`, `skip_rows`, and `skip_columns`).

    Attributes:
        default_sheet_name (Union[str, int]): Default sheet identifier (name or 0-based index).
        default_header_row (int): Default 0-based index for the header row.
        default_skip_rows (int): Default number of rows to skip *before* the header row.
        default_skip_columns (Set[Union[str, int]]): Default names or 0-based indices of columns to skip.
                                                     Primarily intended for names with Excel.
        descriptions (Optional[Union[str, List[Optional[str]]]]): Default descriptions for data sources.
        table_names (Optional[Union[str, List[Optional[str]]]]): Default target table names.
        column_definitions (ColumnDefinitionsConfig): Default column definitions.
        file_configs (Optional[Union[Dict[str, XLSXFileConfig], List[Optional[XLSXFileConfig]]]]):
                      Default file-specific configuration overrides.
    """

    def __init__(
        self,
        sheet_name: Optional[Union[str, int]] = None,
        header_row: int = DEFAULT_HEADER_ROW,
        skip_rows: int = DEFAULT_SKIP_ROWS,
        skip_columns: Optional[List[Union[str, int]]] = None,  # Renamed, type updated
        descriptions: Optional[Union[str, List[Optional[str]]]] = None,
        table_names: Optional[Union[str, List[Optional[str]]]] = None,
        column_definitions: ColumnDefinitionsConfig = None,  # Added
        file_configs: Optional[
            Union[Dict[str, XLSXFileConfig], List[Optional[XLSXFileConfig]]]
        ] = None,
    ):
        """
        Initialize the XLSX standardizer with default and task-specific configurations.

        Args:
            sheet_name: Default sheet to read (name as str, 0-based index as int).
                        If None, defaults to the first sheet (index 0).
            header_row: Default 0-based row index to use as column headers.
            skip_rows: Default number of rows to skip at the beginning of the sheet *before* the header row.
            skip_columns: Default list of column names (exact match, case-sensitive) or 0-based integer indices
                          to exclude from the standardization. If using indices, they refer to the column
                          order *after* `header_row` and `skip_rows` are applied by pandas.
            descriptions: A single description for all sources, or a list of
                          descriptions (one per input file expected in standardize).
                          If None, descriptions are omitted. Used for `sdif_sources.source_description`.
                          This can be overridden by `description` key in `file_configs`.
            table_names: A single table name (used as a base if multiple files),
                         a list of table names (one per input file expected in standardize), or None.
                         If None, table names are derived from sheet names (or filenames if sheet name unavailable).
                         This can be overridden by `table_name` key in `file_configs`.
            column_definitions: Default column definitions to precisely control column selection, renaming,
                                and descriptions. `original_identifier` in `ColumnSpec` maps to original
                                Excel header names. Types are still inferred from data.
            file_configs: Optional configuration overrides. Can be a single dict
                          applied to all files, or a list of dicts (one per file expected
                          in standardize, use None in list to apply defaults). Keys in the dict
                          can include 'sheet_name', 'header_row', 'skip_rows',
                          'skip_columns', 'description', 'table_name', 'column_definitions'.
                          These override the defaults set above for the specific file.
        """
        if header_row < 0:
            raise ValueError("header_row cannot be negative.")
        if skip_rows < 0:
            raise ValueError("skip_rows cannot be negative.")

        self.default_sheet_name = (
            sheet_name if sheet_name is not None else DEFAULT_SHEET
        )
        self.default_header_row = header_row
        self.default_skip_rows = skip_rows
        # Store skip_columns as a set for efficient lookup.
        # While CSV standardizer has more complex validation for skip_columns,
        # For XLSX, it's simpler as pandas handles header/skiprows first.
        # We primarily expect string names for Excel. Integer indices are less common but possible.
        _default_skip_cols = set()
        if skip_columns:
            for item in skip_columns:
                if not isinstance(item, (str, int)):
                    raise TypeError(
                        "Items in skip_columns must be strings (names) or integers (indices)."
                    )
                if isinstance(item, int) and item < 0:
                    raise ValueError(
                        "Integer indices in skip_columns cannot be negative."
                    )
                _default_skip_cols.add(item)
        self.default_skip_columns = _default_skip_cols

        self.descriptions = descriptions
        self.table_names = table_names
        self.column_definitions = column_definitions  # Added
        self.file_configs = file_configs
        self._processed_table_basenames: Dict[
            str, int
        ] = {}  # Used in standardize to track and de-duplicate table names

    def _map_pandas_dtype_to_sqlite(self, dtype: Any) -> str:
        """Maps pandas/numpy dtype to a suitable SQLite type."""
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "REAL"
        elif pd.api.types.is_bool_dtype(dtype):
            return "INTEGER"  # Store bools as 0 or 1
        elif pd.api.types.is_datetime64_any_dtype(
            dtype
        ) or pd.api.types.is_timedelta64_dtype(dtype):
            return "TEXT"  # Store datetime/timedelta as ISO 8601 strings
        else:
            return "TEXT"  # Default to TEXT for object, string, category, etc.

    def _prepare_value_for_sqlite(
        self, value: Any
    ) -> Union[str, int, float, bytes, None]:
        """Prepares a Python value for SQLite insertion based on its type."""
        if pd.isna(value):
            return None
        elif isinstance(value, (datetime.datetime, datetime.date, pd.Timestamp)):
            try:
                return value.isoformat()
            except AttributeError:
                return str(value)
        elif isinstance(value, bool):
            return 1 if value else 0
        elif isinstance(value, (int, float, str, bytes)):
            return value
        else:
            return str(value)

    def _gather_file_processing_parameters(
        self,
        input_path: Path,
        index: int,
        num_inputs: int,
        descriptions_list: List[Optional[str]],
        table_names_list: List[Optional[str]],
        file_configs_overrides_list: List[Optional[Dict[str, Any]]],
        column_definitions_config_list: List[
            Optional[ColumnDefinitionsConfig]
        ],  # Added
    ) -> XLSXFileConfig:
        """Gathers and resolves all effective parameters for processing a single Excel file."""
        current_config_override = file_configs_overrides_list[index] or {}
        if not isinstance(current_config_override, dict):
            raise ValueError(
                f"File config item {index} for {input_path.name} must be a dictionary or None."
            )

        # Determine effective parsing options
        effective_sheet_name = current_config_override.get(
            "sheet_name", self.default_sheet_name
        )
        effective_header_row = current_config_override.get(
            "header_row", self.default_header_row
        )
        effective_skip_rows = current_config_override.get(
            "skip_rows", self.default_skip_rows
        )
        effective_skip_columns = self.default_skip_columns.union(
            set(current_config_override.get("skip_columns", []))
        )

        if effective_header_row < 0:
            raise ValueError(
                f"Configured header_row ({effective_header_row}) cannot be negative for file: {input_path.name}."
            )
        if effective_skip_rows < 0:
            raise ValueError(
                f"Configured skip_rows ({effective_skip_rows}) cannot be negative for file: {input_path.name}."
            )

        # Determine actual sheet name (if an index was provided for sheet_name)
        actual_sheet_name_resolved = str(effective_sheet_name)
        if isinstance(effective_sheet_name, int):
            try:
                xl = pd.ExcelFile(input_path, engine="openpyxl")
                if 0 <= effective_sheet_name < len(xl.sheet_names):
                    actual_sheet_name_resolved = xl.sheet_names[effective_sheet_name]
                else:
                    raise ValueError(
                        f"Sheet index {effective_sheet_name} out of range for {input_path.name}. "
                        f"Available sheets: {xl.sheet_names}"
                    )
                xl.close()
            except Exception as e_sheetname:
                logger.warning(
                    f"Could not determine sheet name from index {effective_sheet_name} for {input_path.name}: {e_sheetname}. "
                    f"Using 'sheet_{effective_sheet_name}' as identifier."
                )
                actual_sheet_name_resolved = f"sheet_{effective_sheet_name}"

        # Determine description and original table name
        effective_description = current_config_override.get(
            "description", descriptions_list[index]
        )
        original_input_table_name_for_file = current_config_override.get(
            "table_name", table_names_list[index]
        )

        # Determine final table name
        table_base_name: str
        if isinstance(original_input_table_name_for_file, str):
            table_base_name = sanitize_sql_identifier(
                original_input_table_name_for_file, f"table_{index}"
            )
        else:  # Derive from sheet or filename
            if actual_sheet_name_resolved:
                table_base_name = sanitize_sql_identifier(
                    actual_sheet_name_resolved, f"table_{index}"
                )
            else:  # Fallback to filename stem
                table_base_name = sanitize_sql_identifier(
                    input_path.stem, f"table_{index}"
                )

        final_table_name_str = table_base_name
        # self._processed_table_basenames is on the instance, shared across calls to this method within one standardize() call
        count = self._processed_table_basenames.get(table_base_name, 0) + 1
        self._processed_table_basenames[table_base_name] = count

        if count > 1:
            # If a single global table name was provided by user for multiple files OR
            # if derived names collide OR
            # if user provided a list of table names with explicit duplicates.
            # In all these cases, we append a suffix to ensure uniqueness.
            final_table_name_str = f"{table_base_name}_{count - 1}"
            logger.info(
                f"Table name '{table_base_name}' for file '{input_path.name}' (sheet: '{actual_sheet_name_resolved}') was not unique. "
                f"Using '{final_table_name_str}' instead."
            )

        # Resolve Column Definitions for this file/table
        current_col_defs_input = current_config_override.get(
            "column_definitions", column_definitions_config_list[index]
        )
        final_column_specs_for_table: Optional[List[ColumnSpec]] = None
        if isinstance(current_col_defs_input, dict) and not all(
            isinstance(k, str) and isinstance(v, list)
            for k, v in current_col_defs_input.items()
        ):
            # This means it's a ColumnSpec itself, not a map of table_name -> List[ColumnSpec]
            # This case is tricky for XLSX if one file produces one table. Assume List[ColumnSpec] or None.
            # For XLSX, we expect column_definitions to be List[ColumnSpec] for the single table derived from the sheet.
            # Or, if it was Dict[str, List[ColumnSpec]] at the top level, it should have been normalized to List[Optional[List[ColumnSpec]]]
            # or List[Optional[Dict[...]]].
            # The normalize_list_argument should handle this.
            # If current_col_defs_input is a dict, it's likely a single ColumnSpec if not map.
            # This part needs careful handling based on how ColumnDefinitionsConfig is structured and normalized.
            # Let's assume normalize_list_argument gives us either List[ColumnSpec] or Dict[str, List[ColumnSpec]] or None for this file.
            pass  # Will be handled by next block

        if isinstance(
            current_col_defs_input, dict
        ):  # Map of table_name -> List[ColumnSpec]
            final_column_specs_for_table = current_col_defs_input.get(
                final_table_name_str
            )
            if (
                final_column_specs_for_table is None
                and final_table_name_str in current_col_defs_input
            ):
                # This should not happen if key exists
                pass
            elif (
                final_column_specs_for_table is None
            ):  # No specific entry for this table name
                logger.debug(
                    f"No column_definitions found for table '{final_table_name_str}' in the provided map for {input_path.name}. Using default header processing."
                )
        elif isinstance(
            current_col_defs_input, list
        ):  # Direct List[ColumnSpec] for this file
            final_column_specs_for_table = current_col_defs_input

        return XLSXFileConfig(
            file_path=str(input_path.resolve()),
            sheet_name=effective_sheet_name,
            actual_sheet_name=actual_sheet_name_resolved,
            header_row=effective_header_row,
            skip_rows=effective_skip_rows,
            skip_columns=effective_skip_columns,  # Renamed
            column_definitions=final_column_specs_for_table,  # Added
            description=effective_description,
            table_name=final_table_name_str,
        )

    def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
    ) -> StandardizationResult:
        """
        Standardize one or more Excel files into a single SDIF database file.

        Reads a specified sheet from each input Excel file and stores its data
        in a corresponding table within the output SDIF database.

        Args:
            datasource: A single path or a list of paths to the input Excel file(s) (.xlsx).
            output_path: The path for the output SDIF database file.
            overwrite: If True, overwrite the output SDIF file if it exists.

        Returns:
            A StandardizationResult object containing the path to the created
            SDIF file and a dictionary of the final configurations used for each
            processed input file.

        Raises:
            ValueError: If input files are invalid, list arguments stored in the instance
                        have incorrect lengths compared to datasource, config values are invalid,
                        or pandas/database errors occur.
            FileNotFoundError: If an input Excel file does not exist.
            ImportError: If 'pandas' or 'openpyxl' is not installed.
            RuntimeError: For errors during Excel parsing or database operations.
        """
        try:
            import openpyxl  # Check if engine is available
        except ImportError as e:
            raise ImportError(
                "Missing dependency: openpyxl. Please install it (`pip install openpyxl`)"
            ) from e
        try:
            import pandas as pd  # Ensure pandas is imported where used
        except ImportError as e:
            raise ImportError(
                "Missing dependency: pandas. Please install it (`pip install pandas`)"
            ) from e

        resolved_output_path = Path(output_path)
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
        file_configs_used: Dict[str, XLSXFileConfig] = {}

        # Normalize list arguments from __init__ or passed to standardize (if that were the case)
        descriptions_list = normalize_list_argument(
            self.descriptions, "Descriptions", num_inputs
        )
        table_names_list = normalize_list_argument(
            self.table_names, "Table names", num_inputs
        )
        file_configs_overrides_list = normalize_list_argument(
            self.file_configs, "File configs", num_inputs
        )
        # Normalize column_definitions from __init__
        column_definitions_config_list: List[Optional[ColumnDefinitionsConfig]] = (
            normalize_list_argument(
                self.column_definitions, "Column definitions", num_inputs
            )
        )
        self._processed_table_basenames.clear()  # Reset for this run

        with SDIFDatabase(resolved_output_path, overwrite=overwrite) as db:
            for i, current_input_path in enumerate(input_paths):
                resolved_input_path_str = str(current_input_path.resolve())
                try:
                    if not current_input_path.exists():
                        raise FileNotFoundError(
                            f"Input Excel file not found: {current_input_path}"
                        )
                    if not current_input_path.is_file():
                        raise ValueError(
                            f"Input path is not a file: {current_input_path}"
                        )
                    if not current_input_path.suffix.lower() == ".xlsx":
                        logger.warning(
                            f"Input file {current_input_path} does not have .xlsx extension. Attempting to read anyway."
                        )

                    current_file_config = self._gather_file_processing_parameters(
                        input_path=current_input_path,
                        index=i,
                        num_inputs=num_inputs,
                        descriptions_list=descriptions_list,
                        table_names_list=table_names_list,
                        file_configs_overrides_list=file_configs_overrides_list,
                        column_definitions_config_list=column_definitions_config_list,
                    )
                    file_configs_used[resolved_input_path_str] = current_file_config

                    # --- Read Excel Sheet ---
                    try:
                        # Pandas: header is 0-indexed row *after* skipping rows.
                        # Our skip_rows means rows before the header row.
                        # So, pandas header = config header_row. Pandas skiprows = config skip_rows.
                        df = pd.read_excel(
                            current_input_path,
                            sheet_name=current_file_config["sheet_name"],
                            header=current_file_config["header_row"],
                            skiprows=current_file_config["skip_rows"],
                            engine="openpyxl",
                            keep_default_na=True,
                            na_values=None,  # Avoid pandas interpreting 'NA', 'NULL' etc. as NaN
                        )
                    except FileNotFoundError:  # Should be caught by earlier check
                        logger.error(
                            f"File not found during pd.read_excel: {current_input_path}"
                        )
                        raise
                    except (
                        ValueError
                    ) as e_pandas_val:  # Handles sheet not found by pandas, etc.
                        raise ValueError(
                            f"Error reading Excel file {current_input_path.name} (sheet: '{current_file_config['sheet_name']}'): {e_pandas_val}"
                        ) from e_pandas_val
                    except (
                        Exception
                    ) as e_pandas_other:  # Catch other pandas/openpyxl errors
                        raise RuntimeError(
                            f"Failed to read Excel file {current_input_path.name} (sheet: '{current_file_config['sheet_name']}'): {e_pandas_other}"
                        ) from e_pandas_other

                    if df.empty:
                        logger.warning(
                            f"Sheet '{current_file_config['actual_sheet_name']}' in {current_input_path.name} "
                            f"is empty or resulted in an empty DataFrame after applying header/skiprows. Skipping table creation."
                        )
                        db.add_source(
                            file_name=current_input_path.name,
                            file_type="xlsx",
                            description=current_file_config["description"],
                        )
                        continue

                    # --- Process Columns ---
                    original_headers = list(df.columns)
                    columns_to_keep = []
                    final_column_names_for_df = []
                    sdif_columns_definition: Dict[str, Dict[str, Any]] = {}
                    col_name_counts: Dict[str, int] = {}

                    for original_header in original_headers:
                        if original_header in current_file_config["skip_columns"]:
                            continue

                        columns_to_keep.append(original_header)
                        sanitized_base_name = sanitize_sql_identifier(
                            str(original_header),
                            f"column_{len(sdif_columns_definition)}",
                        )

                        count = col_name_counts.get(sanitized_base_name, 0) + 1
                        col_name_counts[sanitized_base_name] = count
                        final_col_name = sanitized_base_name
                        if count > 1:
                            final_col_name = f"{sanitized_base_name}_{count - 1}"

                        final_column_names_for_df.append(final_col_name)

                        dtype = df[original_header].dtype
                        sqlite_type = self._map_pandas_dtype_to_sqlite(dtype)
                        sdif_columns_definition[final_col_name] = {
                            "type": sqlite_type,
                            "description": f"Column from Excel sheet '{current_file_config['actual_sheet_name']}', original header: '{original_header}'",
                            "original_column_name": str(original_header),
                        }

                    if not columns_to_keep:
                        logger.warning(
                            f"No columns remaining for sheet '{current_file_config['actual_sheet_name']}' in {current_input_path.name} "
                            f"after exclusions. Skipping table creation."
                        )
                        db.add_source(
                            file_name=current_input_path.name,
                            file_type="xlsx",
                            description=current_file_config["description"],
                        )
                        continue

                    df = df[columns_to_keep]
                    df.columns = final_column_names_for_df

                    # --- SDIF Database Operations ---
                    source_id = db.add_source(
                        file_name=current_input_path.name,
                        file_type="xlsx",
                        description=current_file_config["description"],
                    )

                    table_description = (
                        f"Data loaded from Excel file: {current_input_path.name}, "
                        f"sheet: '{current_file_config['actual_sheet_name']}'."
                    )
                    try:
                        db.create_table(
                            table_name=current_file_config["table_name"],
                            columns=sdif_columns_definition,
                            source_id=source_id,
                            description=table_description,
                            original_identifier=current_file_config[
                                "actual_sheet_name"
                            ],
                        )
                    except (ValueError, sqlite3.Error) as e_create_table:
                        raise RuntimeError(
                            f"Failed to create table '{current_file_config['table_name']}' for {current_input_path.name}: {e_create_table}"
                        ) from e_create_table

                    # --- Prepare and Insert Data ---
                    try:
                        data_to_insert = []
                        for record_dict in df.to_dict("records"):
                            prepared_record = {
                                col: self._prepare_value_for_sqlite(val)
                                for col, val in record_dict.items()
                            }
                            data_to_insert.append(prepared_record)

                        if data_to_insert:
                            db.insert_data(
                                table_name=current_file_config["table_name"],
                                data=data_to_insert,
                            )
                    except Exception as e_insert_data:
                        raise RuntimeError(
                            f"Failed to prepare or insert data into table '{current_file_config['table_name']}' "
                            f"from {current_input_path.name}: {e_insert_data}"
                        ) from e_insert_data

                except FileNotFoundError as e_fnf:
                    logger.error(
                        f"File not found for {current_input_path.name}: {e_fnf}"
                    )
                    raise
                except (ValueError, TypeError, RuntimeError, ImportError) as e_proc:
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

        return StandardizationResult(
            output_path=Path(db.path).resolve(), file_configs=file_configs_used
        )
