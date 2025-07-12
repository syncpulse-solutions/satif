---
sidebar_position: 1
---

# CSV Representer

> **🚧 IN DEVELOPMENT: Multi-modal representation capabilities are under active development. Additional modalities (image, audio, etc.) will be supported in future releases.**

The `CSVRepresenter` provides multi-modal representations of CSV files, currently supporting text-based summaries with image representations planned for future releases.

## Basic Usage

```python
from satif_sdk.representers import CSVRepresenter

representer = CSVRepresenter()

# Text representation (primary modality)
representation, params = representer.represent("data.csv", num_rows=5)
print(representation)
print(f"Used encoding: {params['encoding']}")

# Text-only output
text = representer.as_text("data.csv")
print(text)

# Image representation (planned)
# image_b64 = representer.as_base64_image("data.csv")  # Future release
```

## Multi-Modal Capabilities

### Text Modality (Available)
Generates CSV content as header + sample rows in text format.

### Image Modality (Planned)
Will generate visual representations like charts, tables, or spreadsheet views.

## Initialization

```python
# With defaults
representer = CSVRepresenter(
    default_delimiter=",",
    default_encoding="utf-8",
    default_num_rows=10
)

# Auto-detection (recommended)
representer = CSVRepresenter()  # Will auto-detect delimiter and encoding
```

## Methods

### `represent(file_path, num_rows=None, **kwargs)`

Primary method returning text representation with parameters.

**Parameters:**
- **`file_path`**: Path to CSV file
- **`num_rows`**: Number of data rows to include
- **`encoding`**: Override encoding detection
- **`delimiter`**: Override delimiter detection

**Returns:** `(representation_string, used_parameters_dict)`

### `as_text(file_path, **kwargs)`

Text-only representation (current primary modality).

### `as_base64_image(file_path, **kwargs)`

Image representation as base64 string. Currently returns "Unsupported operation" - coming in future releases.

## Use Cases

**Data preview:**
```python
text, params = representer.represent("sales_data.csv", num_rows=10)
```

**Quick content check:**
```python
summary = representer.as_text("unknown_file.csv")
```
