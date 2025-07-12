---
sidebar_position: 3
---

# AI XLSX Standardizer

> **⚠️ EXPERIMENTAL: This is an experimental implementation not intended for production use. Despite the async API, there may be blocking I/O calls that could impact performance in production async environments.**

The `AIXLSXStandardizer` is an AI Agent with visual capabilities to analyze and standardize Excel files. It processes spreadsheets by visually understanding their structure, performing necessary cleaning operations, and extracting structured tables and metadata into SDIF format.

## 1. Basic Usage

```python
from satif_ai.standardizers.ai_xlsx import AIXLSXStandardizer
from pathlib import Path

# Initialize the standardizer
standardizer = AIXLSXStandardizer()

# Define input and output paths
input_file = "complex_spreadsheet.xlsx"
output_path = Path("standardized_output.sdif")

# Execute standardization
result = await standardizer.standardize(
    datasource=input_file,
    output_path=output_path,
    overwrite=True
)

print(f"XLSX standardization complete. Output at: {result.output_path}")
```

## 2. How It Works

The `AIXLSXStandardizer` leverages a sophisticated AI-powered graph system (`xlsx-to-sdif`) to process Excel files:

1. **Visual Analysis**: The standardizer exports an image of the spreadsheet for AI visual analysis
2. **Structure Recognition**: Identifies tables, headers, and data regions within complex layouts
3. **Data Cleaning**: Intelligently processes merged cells, formatting, and irregular structures
4. **Extraction**: Transforms the analyzed content into structured SDIF format

## 3. Processing Steps

Under the hood, the standardizer follows these steps:

1. **Preparation**: Sets up temporary directories and creates a thread-specific file copy
2. **LangGraph Execution**: Using the `xlsx-to-sdif` langgraph agent which:
   - Builds a spreadsheet state model
   - Captures a visual representation of the sheet
   - Uses an LLM to analyze the spreadsheet structure
   - Iteratively executes transformation tools (navigate, add/delete/update values, etc.)
   - Extract 2D Tables and metadata
   - Generates an intermediate SDIF file
3. **Consolidation**: Merges results from multiple files (if applicable) into a single SDIF output

## 4. Supported File Types

The standardizer supports various Excel file formats:

- `.xlsx` - Excel Open XML Format
- `.xlsm` - Excel Macro-Enabled Workbooks
- `.xlsb` - Excel Binary Workbook
- `.xls` - Legacy Excel 97-2003 Format

## 5. Important Disclaimers

> **⚠️ IMPORTANT: Currently, the standardizer only processes the first sheet of each Excel file.**

> **⚠️ WARNING: During standardization, the AI may alter data to fit standard formats. Guardrails to prevent unwanted modifications will be implemented in future versions.**

## 6. Handling Multiple Files

The standardizer can process multiple Excel files in a single operation:

```python
# Process multiple Excel files into a single SDIF file
result = await standardizer.standardize(
    datasource=["financial_data.xlsx", "inventory.xlsm", "legacy_report.xls"],
    output_path="combined_data.sdif"
)
```

## 7. Standardization Method Parameters

```python
result = await standardizer.standardize(
    datasource=input_file,          # Required: Input Excel file(s)
    output_path=output_path,        # Required: Path for the output SDIF file
    overwrite=False,                # Optional: Whether to overwrite existing output
    config=None                     # Optional: Configuration options (for future use)
)
```

- **`datasource`**: Input Excel file(s) (single path or list of paths)
- **`output_path`**: Path where the standardized SDIF file will be written
- **`overwrite`**: Boolean flag to control whether existing output files should be overwritten
- **`config`**: Reserved for future configuration options

## 8. Error Handling

The standardizer provides comprehensive error handling:

```python
try:
    result = await standardizer.standardize(
        datasource="complex_spreadsheet.xlsx",
        output_path="output.sdif"
    )
except FileNotFoundError as e:
    print(f"Input file not found: {e}")
except ValueError as e:
    print(f"Invalid input parameter: {e}")
except RuntimeError as e:
    print(f"Processing error: {e}")
```

## 9. Output

The standardization process returns a `StandardizationResult` object with:

- **`output_path`**: Path to the generated SDIF file

```python
result = await standardizer.standardize(...)
print(f"Output SDIF file: {result.output_path}")
```

## 10. Future Enhancements

Future versions of the `AIXLSXStandardizer` will include:

- Multi-sheet processing capabilities
- Guardrails to prevent unwanted data modifications
- Enhanced configuration options for controlling AI behavior
- Better handling of complex formulas and references
