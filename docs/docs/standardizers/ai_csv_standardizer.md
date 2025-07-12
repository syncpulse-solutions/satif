---
sidebar_position: 2
---

# AI CSV Standardizer

> **⚠️ EXPERIMENTAL: This is an experimental implementation not intended for production use. Despite the async API, there may be blocking I/O calls that could impact performance in production async environments.**

The `AICSVStandardizer` is an AI Agent to analyze CSV files and automatically determine optimal parsing parameters. It reads file structure and semantics to configure a `CSVStandardizer` with the correct parameters for standardization into SDIF format.

## 1. Basic Usage

```python
from satif_ai.standardizers.ai_csv import AICSVStandardizer
from pathlib import Path

# Initialize the standardizer
standardizer = AICSVStandardizer(
    llm_model="gpt-4.1"  # Optional: specify the LLM model to use
)

# Define input and output paths
input_file = "complex_data.csv"
output_path = Path("standardized_output.sdif")

# Execute standardization
result = await standardizer.standardize(
    datasource=input_file,
    output_path=output_path,
    overwrite=True
)

print(f"CSV standardization complete. Output at: {result.output_path}")
```

## 2. Initialization Parameters

```python
standardizer = AICSVStandardizer(
    llm_model="gpt-4.1-2025-04-14",  # LLM model to use for analysis
    initial_delimiter=None,      # Optional: Initial hint for delimiter
    initial_encoding=None        # Optional: Initial hint for encoding
)
```

- **`llm_model`**: Identifier for the language model to use (defaults to a recent GPT-4 version)
- **`initial_delimiter`**: Optional hint for the CSV delimiter character
- **`initial_encoding`**: Optional hint for the file encoding

## 3. How It Works

The `AICSVStandardizer` follows these steps:

1. **Initial Detection**: Initial guesses about encoding and delimiter using `charset-normalizer` and `clevercsv`.
2. **AI Analysis**: Uses an AI agent with specialized tools (`read_csv_sample` and `read_raw_lines`) to thoroughly analyze the CSV file.
3. **Parameter Determination**: The AI determines all necessary parameters for standardization:
   - Core parsing parameters (encoding, delimiter, header presence, rows to skip)
   - Table metadata (name, description)
   - Column definitions (original identifiers, clean names, descriptions)
4. **Standardization**: Uses the determined parameters to initialize a `CSVStandardizer` and process the CSV into SDIF format.

## 4. AI-Determined Parameters

The AI agent analyzes the CSV to determine:

- **Encoding**: The correct character encoding (e.g., "utf-8", "latin-1")
- **Delimiter**: The character that separates fields (e.g., ",", ";", "\t")
- **Header Presence**: Whether the file has a header row
- **Skip Rows**: Which rows should be skipped (metadata, comments, empty lines)
- **Table Name**: A clean, descriptive name for the data table
- **Table Description**: Optional semantic overview of the table contents
- **Column Definitions**:
  - **Original Identifiers**: How columns are identified in the source CSV
  - **Final Column Names**: Clean, sanitized names for the SDIF database
  - **Column Descriptions**: Optional semantic descriptions of column contents

## 5. Providing Initial Hints

You can provide initial hints to guide the AI analysis:

```python
standardizer = AICSVStandardizer(
    llm_model="gpt-4.1",
    initial_delimiter="|",       # Hint that the file uses pipe delimiter
    initial_encoding="latin-1"   # Hint that the file uses Latin-1 encoding
)
```

The AI will verify these hints and correct them if necessary based on file analysis.

## 6. Handling Multiple Files

The standardizer can process multiple CSV files in a single operation:

```python
# Process multiple CSV files into a single SDIF file
result = await standardizer.standardize(
    datasource=["customers.csv", "orders.csv", "products.csv"],
    output_path="combined_data.sdif"
)
```

Each file is analyzed independently, and the results are combined into a single SDIF output.

## 7. Output

The standardization process returns a `StandardizationResult` object with:

- **`output_path`**: Path to the generated SDIF file
- **`file_configs`**: Configurations used for the processed files, including all AI-determined parameters

```python
result = await standardizer.standardize(...)
print(f"Output SDIF file: {result.output_path}")

# Access the AI-determined parameters
for file_path, config in result.file_configs.items():
    print(f"File: {file_path}")
    print(f"Table name: {config['table_name']}")
    print(f"Encoding: {config['encoding']}")
    print(f"Delimiter: {config['delimiter']}")
```

## 8. Error Handling

The standardizer provides comprehensive error handling:

```python
try:
    result = await standardizer.standardize(
        datasource="complex_data.csv",
        output_path="output.sdif"
    )
except FileNotFoundError as e:
    print(f"Input file not found: {e}")
except ValueError as e:
    print(f"Invalid input or parameter: {e}")
except RuntimeError as e:
    print(f"AI analysis or standardization failed: {e}")
```
