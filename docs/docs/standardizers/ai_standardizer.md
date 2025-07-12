---
sidebar_position: 1
---

# AIStandardizer

> **⚠️ EXPERIMENTAL: This is an experimental implementation not intended for production use. Despite the async API, there may be blocking I/O calls that could impact performance in production async environments.**

The `AIStandardizer` is a component designed to orchestrate the standardization of various file types using specialized AI-driven standardizers. It processes a datasource (which can include individual files or ZIP archives), dispatches files to appropriate AI agents (e.g., `AICSVStandardizer`, `AIXLSXStandardizer`), and merges their SDIF outputs into a single, standardized file.

## 1. Basic Usage

```python
from satif_ai.standardizers.ai import AIStandardizer
from pathlib import Path

# Initialize the standardizer
standardizer = AIStandardizer(
    llm_model="gpt-4.1"  # Optional: specify the LLM model to use
)

# Define input and output paths
input_files = ["data1.csv", "data2.xlsx"]
output_path = "standardized_output.sdif"

# Execute standardization
result = await standardizer.standardize(
    datasource=input_files,
    output_path=output_path,
    overwrite=True
)

print(f"Standardization complete. Output at: {result.output_path}")
```

## 2. Initialization Parameters

The `AIStandardizer` accepts several parameters to customize its behavior:

```python
standardizer = AIStandardizer(
    llm_model=None,           # Optional: Language model identifier (e.g., "gpt-4.1")
    sdif_schema=None,         # Optional: Schema for SDIF schema enforcement
    tidy_adapter=None         # Optional: Adapter for data tidying
)
```

- **`llm_model`**: Identifier for the language model to use for AI standardization
- **`sdif_schema`**: Optional schema definition for SDIF adaptation (as file path or dict)
- **`tidy_adapter`**: Optional adapter for data tidying operations


## 3. Supported File Types

The `AIStandardizer` supports various file types through specialized standardizers:

- **CSV files** (`.csv`): Processed by `AICSVStandardizer`
- **Excel files** (`.xlsx`, `.xls`, `.xlsm`): Processed by `AIXLSXStandardizer`

Support for additional file types can be added by extending the `ai_standardizer_map` dictionary.

## 4. Input Types

The `standardize` method accepts various forms of input datasources:

```python
# Single file path (string or Path)
result = await standardizer.standardize(
    datasource="path/to/file.csv",
    output_path="output.sdif"
)

# List of file paths
result = await standardizer.standardize(
    datasource=["file1.csv", "file2.xlsx"],
    output_path="output.sdif"
)

# Directory path (all supported files in the directory will be processed)
result = await standardizer.standardize(
    datasource="path/to/directory",
    output_path="output.sdif"
)

# ZIP archive (will be extracted and processed)
result = await standardizer.standardize(
    datasource="archive.zip",
    output_path="output.sdif"
)
```

## 5. Standardization Method Parameters

The `standardize` method accepts the following parameters:

```python
result = await standardizer.standardize(
    datasource=input_files,          # Required: Input data source(s)
    output_path=output_path,         # Required: Path for the output SDIF file
    overwrite=False,                 # Optional: Whether to overwrite existing output
    config=None                      # Optional: Configuration options for standardizers
)
```

- **`datasource`**: Input source data (single file path, list of paths, or directory path)
- **`output_path`**: Path where the standardized SDIF file will be written
- **`overwrite`**: Boolean flag to control whether existing output files should be overwritten
- **`config`**: Optional dictionary with configuration options passed to child standardizers

## 6. Output

The standardization process returns a `StandardizationResult` object with:

- **`output_path`**: Path to the generated SDIF file
- **`file_configs`**: Optional configurations for the processed files

```python
result = await standardizer.standardize(...)
print(f"Output SDIF file: {result.output_path}")
if result.file_configs:
    print(f"File configurations: {result.file_configs}")
```

## 7. Error Handling

The `AIStandardizer` provides comprehensive error handling:

- **`ValueError`**: Raised for invalid input parameters or empty datasources
- **`FileNotFoundError`**: Raised when input files don't exist
- **`FileExistsError`**: Raised when output exists and `overwrite=False`
- **`RuntimeError`**: Raised for processing errors during standardization

```python
try:
    result = await standardizer.standardize(
        datasource=input_files,
        output_path=output_path
    )
except FileNotFoundError as e:
    print(f"Input file not found: {e}")
except ValueError as e:
    print(f"Invalid input parameter: {e}")
except RuntimeError as e:
    print(f"Processing error: {e}")
```

## 8. Extending Supported File Types

To add support for additional file types, extend the `ai_standardizer_map` dictionary:

```python
from satif_ai.standardizers.ai import AIStandardizer
from my_package.standardizers import AIPDFStandardizer

# Create standardizer with extended support
standardizer = AIStandardizer()
standardizer.ai_standardizer_map[".pdf"] = AIPDFStandardizer
```

Custom standardizers must inherit from `AsyncStandardizer` and implement the required interface.
