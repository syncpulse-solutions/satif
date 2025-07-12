---
sidebar_position: 4
---

# CSV Standardizer

The `CSVStandardizer` is a component that transforms CSV files into SDIF. It handles complex CSV parsing scenarios with extensive configuration options for delimiter detection, encoding, header handling, row/column skipping, column renaming, and type inference.

## 1. Basic Usage

```python
from satif_sdk.standardizers.csv import CSVStandardizer
from pathlib import Path

# Initialize with default settings
standardizer = CSVStandardizer()

# Standardize a single CSV file
result = standardizer.standardize(
    datasource="data.csv",
    output_path="standardized_data.sdif",
    overwrite=True
)

print(f"Standardization complete. Output at: {result.output_path}")
```

## 2. Initialization Parameters

The `CSVStandardizer` accepts numerous parameters to customize its behavior:

```python
standardizer = CSVStandardizer(
    delimiter=",",                # CSV delimiter character
    encoding="utf-8",             # File encoding
    has_header=True,              # Whether files have header rows
    skip_rows=0,                  # Rows to skip
    skip_columns=None,            # Columns to skip
    descriptions=None,            # Table descriptions
    table_names=None,             # Output table names
    column_definitions=None,      # Column specifications
    file_configs=None             # Per-file configuration overrides
)
```

### 2.1 Core Parsing Parameters

- **`delimiter`**: Character that separates fields in the CSV
  - If `None`, auto-detection is attempted using `clevercsv.Sniffer`
  - Defaults to `,` if auto-detection fails
  - Example values: `,`, `;`, `\t`, `|`

- **`encoding`**: Character encoding of the input files
  - If `None`, auto-detection is attempted using `charset-normalizer`
  - Defaults to `utf-8` if auto-detection fails
  - Common values: `utf-8`, `latin-1`, `cp1252`, `utf-16`

- **`has_header`**: Whether CSV files have a header row (default: `True`)
  - If `True`, the first non-skipped row is treated as column names
  - If `False`, column names are generated as `column_0`, `column_1`, etc.

### 2.2 Skip Configuration

- **`skip_rows`**: Rows to exclude from processing
  - Integer: Skip the first N rows (e.g., `skip_rows=3`)
  - List/Set of integers: Skip specific rows by 0-based index (e.g., `skip_rows=[0, 5, 10]`)
  - Supports negative indices to count from the end (e.g., `skip_rows=[0, 1, -1]` to skip first two rows and last row)

- **`skip_columns`**: Columns to exclude from processing
  - Integer: Skip a specific column by 0-based index (e.g., `skip_columns=0`)
  - String: Skip a column by its name (only if `has_header=True`) (e.g., `skip_columns="timestamp"`)
  - List/Set of integers/strings: Skip multiple columns (e.g., `skip_columns=["id", 2, "notes"]`)

### 2.3 Naming and Metadata

- **`descriptions`**: Descriptions for tables in the SDIF output
  - Single string: Same description for all tables
  - List of strings: One description per input file
  - Used in the SDIF metadata

- **`table_names`**: Names for tables in the SDIF output
  - Single string: Base name for all tables (will be suffixed with index for multiple files)
  - List of strings: One name per input file
  - If `None`, names are derived from input file names

### 2.4 Column Definitions

- **`column_definitions`**: Explicit column specifications to control output
  - List of column specs:
    ```python
    column_definitions=[
        {"original_identifier": "Customer ID", "final_column_name": "customer_id", "description": "Unique customer identifier"},
        {"original_identifier": "2", "final_column_name": "purchase_amount", "description": "Amount in USD"}
    ]
    ```
  - Dictionary mapping table names to column specs:
    ```python
    column_definitions={
        "customers": [
            {"original_identifier": "ID", "final_column_name": "customer_id"},
            {"original_identifier": "Name", "final_column_name": "customer_name"}
        ]
    }
    ```
  - List of definitions (one per input file)

### 2.5 Per-File Configuration

- **`file_configs`**: Overrides defaults for specific files
  - Dictionary of configurations (applies to all files):
    ```python
    file_configs={
        "delimiter": ";",
        "encoding": "latin-1",
        "has_header": False
    }
    ```
  - List of configurations (one per input file):
    ```python
    file_configs=[
        {"delimiter": ",", "encoding": "utf-8"},
        {"delimiter": ";", "encoding": "latin-1", "skip_rows": 3},
        None  # Use defaults for third file
    ]
    ```

## 3. Type Inference

The `CSVStandardizer` automatically infers column data types from a sample of the data:

- **INTEGER**: If all non-empty values in the sample can be parsed as integers
- **REAL**: If all non-empty values in the sample can be parsed as floating-point numbers
- **TEXT**: If any value cannot be parsed as a number or if explicitly specified

The type inference process:

1. Examines up to 100 rows (configurable via `SAMPLE_SIZE` constant)
2. Initially considers all possible types (INTEGER, REAL, TEXT) for each column
3. Progressively eliminates types that don't match the data
4. Follows a type hierarchy: INTEGER > REAL > TEXT (prioritizes more specific types)
5. Falls back to TEXT if any ambiguity exists

## 4. Multiple File Handling

The standardizer can process multiple CSV files into a single SDIF database:

```python
# Process multiple files
result = standardizer.standardize(
    datasource=["customers.csv", "orders.csv", "products.csv"],
    output_path="combined_data.sdif"
)
```

When standardizing multiple files:

- Each file becomes a separate table in the SDIF output
- Table names are automatically derived and made unique:
  - First file: Uses the base name
  - Subsequent files: Base name with suffix `_1`, `_2`, etc.
- File-specific configurations can be provided via the `file_configs` parameter

## 5. Advanced Usage Examples

### 5.1 Auto-detection with Fallbacks

```python
# Let the standardizer auto-detect parameters with fallbacks
standardizer = CSVStandardizer(
    delimiter=None,  # Try to auto-detect
    encoding=None,   # Try to auto-detect
    has_header=True
)
```

### 5.2 Skipping Metadata and Comments

```python
# Skip header metadata and footer rows
standardizer = CSVStandardizer(
    skip_rows=[0, 1, 2, -1, -2],  # Skip first 3 rows and last 2 rows
)

# Skip metadata rows at the beginning
standardizer = CSVStandardizer(
    skip_rows=5,  # Skip first 5 rows
)
```

### 5.3 Column Selection and Renaming

```python
# Select specific columns and rename them
standardizer = CSVStandardizer(
    column_definitions=[
        {"original_identifier": "ID", "final_column_name": "customer_id"},
        {"original_identifier": "First Name", "final_column_name": "first_name"},
        {"original_identifier": "Last Name", "final_column_name": "last_name"},
        # Skip other columns by not including them
    ]
)
```

### 5.4 Different Configurations for Multiple Files

```python
# Configure each file differently
standardizer = CSVStandardizer(
    # File-specific configurations
    file_configs=[
        {
            "delimiter": ",",
            "encoding": "utf-8",
            "has_header": True,
            "skip_rows": 2
        },
        {
            "delimiter": ";",
            "encoding": "latin-1",
            "has_header": False
        }
    ],
    # Table names
    table_names=["customers", "orders"],
    # Table descriptions
    descriptions=[
        "Customer master data",
        "Order transactions"
    ]
)

# Standardize multiple files
result = standardizer.standardize(
    datasource=["customers.csv", "orders.csv"],
    output_path="data.sdif"
)
```

### 5.5 Advanced Column Definitions

```python
# Define columns with descriptions and position-based identifiers
standardizer = CSVStandardizer(
    has_header=False,
    column_definitions=[
        {"original_identifier": "0", "final_column_name": "id", "description": "Unique identifier"},
        {"original_identifier": "1", "final_column_name": "name", "description": "Full name"},
        {"original_identifier": "3", "final_column_name": "email", "description": "Contact email"}
        # Note: Column at position 2 is skipped
    ]
)
```

## 6. Output

The standardization process returns a `StandardizationResult` object with:

- **`output_path`**: Path to the generated SDIF file
- **`file_configs`**: Configurations used for each processed file

```python
result = standardizer.standardize(...)
print(f"Output SDIF file: {result.output_path}")
print(f"Configurations used: {result.file_configs}")
```

## 7. Error Handling

The standardizer provides comprehensive error handling:

- **`FileNotFoundError`**: If input files don't exist
- **`ValueError`**: For invalid parameters or configurations
- **`TypeError`**: For incorrect argument types
- **`UnicodeDecodeError`**: For encoding issues

```python
try:
    result = standardizer.standardize(
        datasource="data.csv",
        output_path="output.sdif"
    )
except FileNotFoundError:
    print("Input file not found")
except ValueError as e:
    print(f"Invalid parameter: {e}")
except UnicodeDecodeError:
    print("Encoding issue - try specifying the correct encoding")
```

## 8. CSV Format Heuristics

The standardizer employs several heuristics to handle common CSV format issues:

- **Blank Line Handling**: Blank lines after the initial skip count are automatically skipped
- **Mismatched Column Counts**: Rows with too few columns have missing values treated as NULL; extra columns are ignored
- **Header Name Collisions**: Duplicate column names are disambiguated with numeric suffixes
- **Case-Insensitive Column Matching**: When resolving column names for skipping, falls back to case-insensitive matching
- **Name Sanitization**: Column names are sanitized for SQL compatibility (replacing special characters, ensuring valid identifiers)

## 9. Performance Considerations

- For large files with negative skip indices, a preliminary pass is required to count total rows
- Type inference uses a configurable sample size (default: 100 rows) to balance accuracy and performance
- Automatic encoding and delimiter detection read only small samples from the beginning of files
- Memory usage is optimized by processing files row-by-row rather than loading the entire file
