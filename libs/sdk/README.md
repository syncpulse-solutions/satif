# SATIF SDK

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)

Core implementation of the SATIF data standardization and transformation capabilities.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Features](#features)
- [Usage](#usage)
  - [Standardization](#standardization)
  - [Transformation](#transformation)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

## Overview

The `satif-sdk` package provides concrete implementations of the SATIF toolkit's standardization and transformation capabilities without AI assistance. It serves as the foundation for the AI-powered `satif-ai` package while remaining usable as a standalone library for data processing workflows.

Key functionality includes:

- Converting heterogeneous source files (CSV, Excel, PDF, etc.) into the Standardized Data Interoperable Format (SDIF)
- Executing transformation logic to convert SDIF data into target output formats
- Supporting both manual code-based workflows and the AI-assisted pipelines in the higher-level packages

## Installation

### From PyPI

```bash
pip install satif-sdk
```

### With AI capabilities (includes satif-ai)

```bash
pip install satif-sdk[ai]
```

### From Source (for Development)

```bash
git clone https://github.com/syncpulse-solutions/satif.git
cd satif/libs/sdk
poetry install
```

## Usage

### Standardization

Convert input files to SDIF format:

```python
from pathlib import Path
from satif_sdk.standardizers import CSVStandardizer

# Create a standardizer for CSV files
csv_standardizer = CSVStandardizer()

# Standardize a CSV file (or list of files) into an SDIF database
result = csv_standardizer.standardize(
    input_path=["data.csv", "reference.csv"],
    output_path="standardized_data.sdif",
    overwrite=True
)
```

### Transformation

Transform SDIF data using custom Python code:

```python
from pathlib import Path
from satif_sdk.transformers import CodeTransformer

# Define transformation logic
def transform_data(conn):
    """
    Transform SDIF data into the desired output format.

    Args:
        conn: SQLite connection to the SDIF database

    Returns:
        dict mapping filenames to their contents
    """
    import pandas as pd

    # Query data from SDIF tables
    df = pd.read_sql_query("SELECT * FROM db1.data", conn)

    # Apply transformations
    df['calculated_value'] = df['value'] * 2

    # Return output files
    return {
        "output.csv": df,
        "summary.json": {"total_records": len(df), "average": df['value'].mean()}
    }

# Create transformer and execute
transformer = CodeTransformer(function=transform_data)
result = transformer.export(
    sdif="standardized_data.sdif",
    output_path="output_directory"
)

print(f"Transformation outputs created at: {result}")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

Maintainer: Bryan Djafer (bryan.djafer@syncpulse.fr)
