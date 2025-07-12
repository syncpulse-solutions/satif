---
sidebar_position: 2
---

# XLSX Representer

> **🚧 IN DEVELOPMENT: Image modality using LibreOffice conversion is under development and not yet available.**

The `XlsxRepresenter` generates text representations of Excel files and will support image generation via LibreOffice conversion in future releases.

## Requirements

```bash
pip install openpyxl
```

## Basic Usage

```python
from satif_sdk.representers import XlsxRepresenter

representer = XlsxRepresenter()

# Text representation
text_representation = representer.represent("data.xlsx", num_rows=5)
print(text_representation)

# Image representation (not yet available)
# image_b64 = representer.as_base64_image("data.xlsx")  # Uses LibreOffice conversion
```

## Methods

### `represent(file_path, num_rows=10, **kwargs)`

Returns text representation of all sheets.

**Parameters:**
- **`file_path`**: Path to Excel file
- **`num_rows`**: Number of data rows per sheet (default: 10)
- **`engine`**: Pandas engine for reading (default: "openpyxl")

**Returns:** `str` - Text representation of all sheets

### `as_text(file_path, **kwargs)`

Text-only representation.

### `as_base64_image(file_path, **kwargs)`

Image representation as base64 string. Currently returns "Unsupported operation".

Future implementation will use LibreOffice to convert Excel sheets to images, providing visual representations of the actual spreadsheet layout.

## Examples

**Basic usage:**
```python
text = representer.represent("financial_report.xlsx", num_rows=15)
```

**Legacy files:**
```python
text = representer.represent(
    "legacy_file.xls",
    num_rows=25,
    engine="xlrd"
)
```

## Error Handling

- `[Excel file contains no readable sheets or is empty]`
- `[Sheet is empty]`
- `[Sheet has header but no data rows]`
- `[Error reading Excel file: details...]`

## Notes

- Processes all sheets in the workbook
- Converts all cell values to strings
- Future image modality will require LibreOffice installation
