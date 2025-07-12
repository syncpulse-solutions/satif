---
sidebar_label: xlsx
title: satif_sdk.standardizers.xlsx
---

## XLSXFileConfig Objects

```python
class XLSXFileConfig(FileConfig)
```

> Configuration settings applied to a single XLSX file during standardization.

## XLSXStandardizer Objects

```python
class XLSXStandardizer(Standardizer)
```

> Standardizer for one or multiple Excel (.xlsx) files/sheets into an SDIF database.
>
> Transforms data from specified sheets within Excel files into the SDIF format.
> Default options (sheet_name/index, header_row, skip_rows, skip_columns)
> are set during initialization. These defaults can be overridden on a per-file basis
> when calling the `standardize` method using the `file_configs` parameter.
> Infers SQLite types (INTEGER, REAL, TEXT) from pandas dtypes.
>
> If `column_definitions` are provided for a file, they take precedence for selecting,
> renaming, and describing columns. Otherwise, headers are taken from the Excel sheet
> (respecting `header_row`, `skip_rows`, and `skip_columns`).
>
> **Attributes**:
>
> - `default_sheet_name` _Union[str, int]_ - Default sheet identifier (name or 0-based index).
> - `default_header_row` _int_ - Default 0-based index for the header row.
> - `default_skip_rows` _int_ - Default number of rows to skip *before* the header row.
> - `default_skip_columns` _Set[Union[str, int]]_ - Default names or 0-based indices of columns to skip.
>   Primarily intended for names with Excel.
> - `file_configs`0 _Optional[Union[str, List[Optional[str]]]]_ - Default descriptions for data sources.
> - `file_configs`1 _Optional[Union[str, List[Optional[str]]]]_ - Default target table names.
> - `column_definitions` _ColumnDefinitionsConfig_ - Default column definitions.
>   file_configs (Optional[Union[Dict[str, XLSXFileConfig], List[Optional[XLSXFileConfig]]]]):
>   Default file-specific configuration overrides.

#### \_\_init\_\_

```python
def __init__(
    sheet_name: Optional[Union[str, int]] = None,
    header_row: int = DEFAULT_HEADER_ROW,
    skip_rows: int = DEFAULT_SKIP_ROWS,
    skip_columns: Optional[List[Union[str, int]]] = None,
    descriptions: Optional[Union[str, List[Optional[str]]]] = None,
    table_names: Optional[Union[str, List[Optional[str]]]] = None,
    column_definitions: ColumnDefinitionsConfig = None,
    file_configs: Optional[Union[Dict[str, XLSXFileConfig],
                                 List[Optional[XLSXFileConfig]]]] = None)
```

> Initialize the XLSX standardizer with default and task-specific configurations.
>
> **Arguments**:
>
> - `sheet_name` - Default sheet to read (name as str, 0-based index as int).
>   If None, defaults to the first sheet (index 0).
> - `header_row` - Default 0-based row index to use as column headers.
> - `skip_rows` - Default number of rows to skip at the beginning of the sheet *before* the header row.
> - `skip_columns` - Default list of column names (exact match, case-sensitive) or 0-based integer indices
>   to exclude from the standardization. If using indices, they refer to the column
>   order *after* `header_row` and `skip_rows` are applied by pandas.
> - `descriptions` - A single description for all sources, or a list of
>   descriptions (one per input file expected in standardize).
>   If None, descriptions are omitted. Used for `sdif_sources.source_description`.
>   This can be overridden by `description` key in `file_configs`.
> - `header_row`0 - A single table name (used as a base if multiple files),
>   a list of table names (one per input file expected in standardize), or None.
>   If None, table names are derived from sheet names (or filenames if sheet name unavailable).
>   This can be overridden by `header_row`1 key in `file_configs`.
> - `header_row`3 - Default column definitions to precisely control column selection, renaming,
>   and descriptions. `header_row`4 in `header_row`5 maps to original
>   Excel header names. Types are still inferred from data.
> - `file_configs` - Optional configuration overrides. Can be a single dict
>   applied to all files, or a list of dicts (one per file expected
>   in standardize, use None in list to apply defaults). Keys in the dict
>   can include &#x27;sheet_name&#x27;, &#x27;header_row&#x27;, &#x27;skip_rows&#x27;,
>   &#x27;skip_columns&#x27;, &#x27;description&#x27;, &#x27;table_name&#x27;, &#x27;column_definitions&#x27;.
>   These override the defaults set above for the specific file.

#### standardize

```python
def standardize(datasource: Datasource,
                output_path: SDIFPath,
                *,
                overwrite: bool = False) -> StandardizationResult
```

> Standardize one or more Excel files into a single SDIF database file.
>
> Reads a specified sheet from each input Excel file and stores its data
> in a corresponding table within the output SDIF database.
>
> **Arguments**:
>
> - `datasource` - A single path or a list of paths to the input Excel file(s) (.xlsx).
> - `output_path` - The path for the output SDIF database file.
> - `overwrite` - If True, overwrite the output SDIF file if it exists.
>
>
> **Returns**:
>
>   A StandardizationResult object containing the path to the created
>   SDIF file and a dictionary of the final configurations used for each
>   processed input file.
>
>
> **Raises**:
>
> - `ValueError` - If input files are invalid, list arguments stored in the instance
>   have incorrect lengths compared to datasource, config values are invalid,
>   or pandas/database errors occur.
> - `FileNotFoundError` - If an input Excel file does not exist.
> - `ImportError` - If &#x27;pandas&#x27; or &#x27;openpyxl&#x27; is not installed.
> - `RuntimeError` - For errors during Excel parsing or database operations.
