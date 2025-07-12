---
sidebar_label: csv
title: satif_sdk.standardizers.csv
---

## CSVStandardizer Objects

```python
class CSVStandardizer(Standardizer)
```

> Standardizer for one or multiple CSV files into a single SDIF database.
>
> Transforms CSV data into the SDIF format, handling single or multiple files.
> Default CSV parsing options (delimiter, encoding, header, skip_rows,
> skip_columns) are set during initialization. These defaults can
> be overridden on a per-file basis when calling the `standardize` method.
> Includes basic type inference for columns (INTEGER, REAL, TEXT).
>
> **Attributes**:
>
> - `default_delimiter` _Optional[str]_ - Default CSV delimiter character. If None, attempts auto-detection.
> - `default_encoding` _Optional[str]_ - Default file encoding. If None, attempts auto-detection.
> - `default_has_header` _bool_ - Default assumption whether CSV files have a header row.
> - `default_skip_rows` _SkipRowsConfig_ - Raw config for rows to skip, validated from constructor.
> - `default_skip_columns` _SkipColumnsConfig_ - Raw config for columns to skip, validated from constructor.
> - `descriptions` _Optional[Union[str, List[Optional[str]]]]_ - Descriptions for the data sources.
> - `table_names` _Optional[Union[str, List[Optional[str]]]]_ - Target table names in the SDIF database.
> - `file_configs` _Optional[Union[Dict[str, CSVFileConfig], List[Optional[CSVFileConfig]]]]_ - File-specific configuration overrides.
> - `column_definitions` _ColumnDefinitionsConfig_ - Column definitions for the data sources.

#### \_\_init\_\_

```python
def __init__(
    delimiter: Optional[str] = None,
    encoding: Optional[str] = None,
    has_header: bool = True,
    skip_rows: SkipRowsConfig = 0,
    skip_columns: SkipColumnsConfig = None,
    descriptions: Optional[Union[str, List[Optional[str]]]] = None,
    table_names: Optional[Union[str, List[Optional[str]]]] = None,
    column_definitions: ColumnDefinitionsConfig = None,
    file_configs: Optional[Union[Dict[str, CSVFileConfig],
                                 List[Optional[CSVFileConfig]]]] = None)
```

> Initialize the CSV standardizer with default and task-specific configurations.
>
> **Arguments**:
>
> - `delimiter` - Default CSV delimiter character. If None, attempts auto-detection.
>   If auto-detection fails, defaults to &#x27;,&#x27; with a warning.
> - `encoding` - Default file encoding. If None, attempts auto-detection using charset-normalizer.
>   If auto-detection fails, defaults to &#x27;utf-8&#x27; with a warning.
> - `has_header` - Default assumption whether CSV files have a header row.
> - `skip_rows` - Rows to skip. Can be:
>   - An `int`: Skips the first N rows.
>   - A `List[int]` or `Set[int]`: Skips rows by their specific 0-based index (negative indices count from end).
>   Defaults to 0 (skip no rows). Non-negative indices only for positive specification.
> - `skip_columns` - Columns to skip. Can be:
>   - An `int` or `str`: Skip a single column by 0-based index or name.
>   - A `encoding`0 or `encoding`1 containing `int` or `str`: Skip multiple columns by index or name.
>   Column names are only effective if `encoding`4. Non-negative indices only.
>   Defaults to None (skip no columns).
> - `encoding`5 - A single description for all sources, or a list of
>   descriptions (one per input file expected in standardize).
>   If None, descriptions are omitted. Used for `encoding`6.
> - `encoding`7 - A single table name (used as a base if multiple files),
>   a list of table names (one per input file expected in standardize), or None.
>   If None, table names are derived from input filenames.
> - `encoding`8 - Optional configuration overrides. Can be a single dict
>   applied to all files, or a list of dicts (one per file expected
>   in standardize, use None in list to apply defaults). Keys in the dict
>   can include &#x27;delimiter&#x27;, &#x27;encoding&#x27;, &#x27;has_header&#x27;,
>   &#x27;skip_rows&#x27;, &#x27;skip_columns&#x27;, &#x27;description&#x27;, &#x27;table_name&#x27;, &#x27;column_definitions&#x27;.
>   These override the defaults set above.
> - `encoding`9 - Provides explicit definitions for columns, overriding automatic header
>   processing or inference. This allows renaming columns, selecting specific
>   columns, and providing descriptions. Types are still inferred.
>   Can be:
>   - A `has_header`0: Defines columns for a single table. If multiple input
>   files are processed and this single list is provided, it&#x27;s applied to each.
>   Each `has_header`1 is a dict:
> - `has_header`2{&quot;original_identifier&quot;`has_header`3
> - `has_header`2original_identifier`has_header`2 _str_ - Name or 0-based index (as str) in the CSV.
> - `has_header`2final_column_name`has_header`2 _str_ - Desired name in the SDIF table.
> - `has_header`2description`has_header`2 _str, optional_ - Column description.
>   - A `skip_rows`0: Maps final table names to their column specs.
>   Useful when `encoding`7 are known and you want to define columns per table.
>   - A `skip_rows`2:
>   A list corresponding to each input file. Each element can be `skip_rows`3 (use default
>   handling), a `has_header`0 for that file&#x27;s table, or a
>   `skip_rows`0 if that file might map to specific table names
>   (though CSV standardizer typically creates one table per file).
>   - If `skip_rows`3 (default), columns are derived from CSV header or generated, and types inferred.

#### standardize

```python
def standardize(datasource: Datasource,
                output_path: SDIFPath,
                *,
                overwrite: bool = False) -> StandardizationResult
```

> Standardize one or more CSV files into a single SDIF database file,
> using configurations provided during initialization or overridden per file.
>
> **Arguments**:
>
> - `datasource` - A single file path (str or Path) or a list of file paths
>   for the CSV files to be standardized.
> - `output_path` - The path (str or Path) where the output SDIF database
>   file will be created.
> - `overwrite` - If True, an existing SDIF file at `output_path` will be
>   overwritten. Defaults to False (raises an error if file exists).
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
> - `FileNotFoundError` - If an input CSV file is not found.
> - `ValueError` - If input parameters are invalid (e.g., no input datasource,
>   input path is not a file).
> - `TypeError` - If datasource type is incorrect.
>   Various other exceptions from underlying CSV parsing or database operations
>   can also be raised if critical errors occur.
