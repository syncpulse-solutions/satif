---
sidebar_position: 3
---

# CodeTransformer

The `CodeTransformer` is a component designed to execute custom Python code for transforming data stored in one or more SDIF files into various output file formats. It provides a flexible way to define and run data processing logic directly within your workflow.

## 1. Basic Usage

The simplest way to use `CodeTransformer` is by providing a Python function that performs the transformation.

**Define your transformation function:**

This function takes a `sqlite3.Connection` object, which provides access to the input SDIF data attached as schemas (e.g., `db1`). It must return a dictionary where keys are relative output filenames and values are the data to be written (e.g., a pandas DataFrame).

```python
# Example transformation function
import pandas as pd
import sqlite3
from typing import Dict, Any
from datetime import timedelta

def process_invoices(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Reads invoices, calculates due dates, and returns a DataFrame."""
    df = pd.read_sql_query("SELECT * FROM db1.factures WHERE type = 'Facture'", conn)

    # Simple calculation example
    df['IssueDateDT'] = pd.to_datetime(df['date_emission'])
    df['DueDate'] = (df['IssueDateDT'] + timedelta(days=30)).dt.strftime('%Y-%m-%d')

    # Select and rename columns
    final_df = df[['id_facture', 'client', 'montant_ttc', 'DueDate']].rename(columns={
        'id_facture': 'InvoiceID',
        'client': 'Customer',
        'montant_ttc': 'TotalAmount'
    })

    # Return dictionary: {output_filename: data_object}
    return {"processed_invoices.csv": final_df}

```

**Instantiate and run the transformer:**

```python
from satif.transformers.code import CodeTransformer
from pathlib import Path

# Assume 'input_invoices.sdif' exists
input_sdif_path = Path("input_invoices.sdif")
output_csv_path = Path("output/processed_invoices.csv")

# 1. Create the transformer instance with the function
transformer = CodeTransformer(function=process_invoices)

# 2. Execute the transformation and export the result
result_path = transformer.export(
    sdif=input_sdif_path,
    output_path=output_csv_path
)
print(f"Transformation successful. Output written to: {result_path}")

```

This will execute the `process_invoices` function using the data from `input_invoices.sdif` (accessed as schema `db1`) and write the resulting DataFrame to `output/processed_invoices.csv`.

## 2. Providing Transformation Logic

Besides passing a function object directly, you can provide the transformation logic in other ways:

**a) Code String:**

Pass the Python code as a string. You might need to specify the function name if it's not the default (`transform`).

```python
TRANSFORM_CODE = """
import pandas as pd
import sqlite3

# Default function name is 'transform'
def transform(conn: sqlite3.Connection):
    df = pd.read_sql_query("SELECT client, SUM(montant_ht) as total_ht FROM db1.factures GROUP BY client", conn)
    return {"client_summary.json": df}
"""

transformer = CodeTransformer(function=TRANSFORM_CODE)
# If function name was different, e.g., 'generate_summary':
# transformer = CodeTransformer(function=TRANSFORM_CODE, function_name="generate_summary")
```

**b) File Path:**

Provide a `pathlib.Path` object pointing to a Python file containing the transformation function.

```python
# Assume 'my_transforms/invoice_logic.py' contains the 'process_invoices' function
transform_script_path = Path("my_transforms/invoice_logic.py")

transformer = CodeTransformer(
    function=transform_script_path,
    function_name="process_invoices" # Specify the function to run from the file
)
# ... then call transformer.export(...)
```

**c) Using the `@transformation` Decorator:**

Decorate your transformation functions with `@transformation`. This registers them internally, allowing you to instantiate `CodeTransformer` using the function object or its registered name (which defaults to the function name).

```python
from satif.transformers.code import transformation

@transformation
def generate_report(conn):
    # ... logic ...
    return {"report.txt": "Report content"}

@transformation(name="custom_invoice_summary") # Register with a custom name
def create_invoice_summary(conn):
    # ... logic ...
    return {"summary.csv": df_summary}

# Instantiate using the function object
transformer1 = CodeTransformer(function=generate_report)

# Instantiate using the registered name (string)
transformer2 = CodeTransformer(function="custom_invoice_summary")

# ... then call transformer.export(...) for each
```

## 3. The Transformation Function

Your transformation code needs to adhere to specific requirements:

* **Signature:** Must accept at least one argument: `conn: sqlite3.Connection`. It can optionally accept a second argument: `context: Dict[str, Any]`.
* **Input Data Access:** Use the `conn` object to query data from the attached SDIF files. Standard SQL queries (e.g., via `pandas.read_sql_query`) work directly. Input SDIF files are attached as schemas (see Section 4).
* **Return Value:** MUST return a dictionary (`Dict[str, Any]`).
  * **Keys:** Relative paths for the output files (e.g., `"data/summary.csv"`, `"report.json"`). Subdirectories will be created automatically during export. Use POSIX-style separators (`/`).
  * **Values:** The data to be written. Supported types include:
    * `pandas.DataFrame`
    * `dict` or `list` (will be saved as JSON)
    * `str` (will be saved as UTF-8 text)
    * `bytes` (will be saved as a binary file)

## 4. Handling Input SDIFs

`CodeTransformer` can process data from one or multiple SDIF files.

**Note:** Throughout this documentation, `SDIFPath` refers to either a string path or a `pathlib.Path` object pointing to an SDIF file.

**a) Single SDIF Path:**

The default behavior shown previously. The data is accessible via the schema named `db1` (or a custom prefix, see Advanced Configuration).

```python
transformer.export(sdif="path/to/single.sdif", ...)
# SQL: SELECT * FROM db1.my_table
```

**b) List of SDIF Paths:**

Provide a list of paths. They will be attached sequentially with default schema names `db1`, `db2`, etc.

```python
transformer.export(sdif=["invoices.sdif", "clients.sdif"], ...)
# SQL: SELECT * FROM db1.invoices_table
# SQL: SELECT * FROM db2.clients_table
```

**c) Dictionary of Schema Names to Paths:**

Provide a dictionary mapping your desired schema names to their corresponding SDIF file paths. This gives you explicit control over schema naming in your SQL queries.

```python
transformer.export(
    sdif={
        "inv": "path/to/invoices.sdif",
        "cust": "path/to/customers.sdif"
    },
    ...
)
# SQL: SELECT * FROM inv.invoices_table
# SQL: SELECT i.*, c.name FROM inv.invoices_table i JOIN cust.info c ON i.cust_id = c.id
```

**d) Using an `SDIFDatabase` Instance:**

If you already have an opened `SDIFDatabase` object (from `satif.sdif_database`), you can pass it directly. The transformer will use its existing connection and schema name. The connection will *not* be closed by the transformer in this case.

```python
from satif.sdif_database import SDIFDatabase

# Assume db is an initialized SDIFDatabase instance with schema_name='input_data'
db = SDIFDatabase("my_data.sdif", read_only=True, schema_name="input_data")
try:
    transformer.export(sdif=db, ...)
    # SQL: SELECT * FROM input_data.some_table
finally:
    db.close() # Remember to close the database connection yourself
```

## 5. Exporting Results (`export` method)

The `export` method takes the transformed data and writes it to the filesystem.

* **`sdif`:** The input SDIF source(s) (as described above).
* **`output_path`:**
  * If the transformation returns a *single* output file and `output_path` *does not* point to an existing directory, `output_path` is treated as the *exact path* for that single output file.
  * Otherwise, `output_path` is treated as the *base directory* where the output files (using the relative paths from the transformation result keys) will be written. If the directory doesn't exist, it will be created.
  * Defaults to the current directory (`.`).
* **`zip_archive` (bool, default `False`):**
  * If `True`, all output files are written into a single ZIP archive named according to `output_path`. In this case, `output_path` *must* be a file path (e.g., `"output/archive.zip"`), not a directory.
  * If `False` (default), files are written directly to the filesystem based on `output_path`.

**File Writing Behavior:**

The format used for writing depends on the file extension in the keys of the dictionary returned by your transformation function and the type of the data object:

* **`.csv`:** Writes `pandas.DataFrame` using `to_csv(index=False)`.
* **`.json`:** Writes `pandas.DataFrame` using `to_json(orient="records", indent=2)`. Writes `dict` or `list` using `json.dump(indent=2)`.
* **`.xlsx` / `.xls`:** Writes `pandas.DataFrame` using `to_excel(index=False)`. Requires optional dependencies (`openpyxl` for `.xlsx`).
  * **Note:** You must install `openpyxl` to write Excel files: `pip install openpyxl`. If not installed, an `ExportError` will be raised when attempting to write Excel files.
* **`.txt` (or other text-based):** Writes `str` content.
* **Binary extensions (e.g., `.bin`):** Writes `bytes` content.
* **Fallback:** If a `DataFrame` is mapped to an unsupported extension, it defaults to writing as `.csv`. Other unsupported data types will cause an error during export.

**Security Note:** Output filenames are sanitized to prevent writing outside the target `output_path` directory (e.g., paths like `../other_dir/file.txt` or `/absolute/path/file.txt` are disallowed).

## 6. In-Memory Transformation (`transform` method)

If you only need the transformed data in memory (e.g., for further processing in Python) without writing files, use the `transform` method. It accepts the same `sdif` parameter types as the `export` method.

```python
# Get the transformed data as a dictionary {filename: data_object}
transformed_data = transformer.transform(sdif="input.sdif")

# Access the data
df_invoices = transformed_data.get("processed_invoices.csv")
if df_invoices is not None:
    print(f"Processed {len(df_invoices)} invoices in memory.")

# If you need to export this data later, you can use the _export_data method
# transformer._export_data(data=transformed_data, output_path="output/dir")
```

## 7. Advanced Configuration

You can customize the `CodeTransformer` during initialization:

* **`function_name` (str, default `"transform"`):** The name of the function to call when `function` is provided as a code string or file path. Ignored if `function` is a callable or uses the `@transformation` decorator (which sets the name automatically).
* **`extra_context` (Dict[str, Any], default `{}`):** A dictionary of arbitrary Python objects that will be passed as the `context` argument to your transformation function (if it accepts two arguments). Useful for passing parameters, configurations, or shared objects.
* **`db_schema_prefix` (str, default `"db"`):** The prefix used for default schema names (`db1`, `db2`, ...) when a *list* of SDIF paths is provided as input. Ignored if input is a single path, a dictionary, or an `SDIFDatabase` instance.
* **`code_executor` (CodeExecutor, optional):** In production, you can plug a different execution backend (e.g., a sandboxed environment with E2B). Defaults to `LocalCodeExecutor`, which runs the code in the current Python process.

**Example with Context and Custom Prefix:**

```python
# Transformation function accepting context
def process_with_context(conn, context):
    discount = context.get('discount_rate', 0.0)
    df = pd.read_sql_query(f"SELECT * FROM {context['schema']}.data", conn)
    df['discounted_price'] = df['price'] * (1 - discount)
    return {"discounted_data.csv": df}

# Instantiate with context and custom prefix for list input
transformer = CodeTransformer(
    function=process_with_context,
    extra_context={"discount_rate": 0.15, "schema": "src1"}, # Pass context
    db_schema_prefix="src" # Input list schemas will be src1, src2...
)

# Run with a list of SDIFs
transformer.export(
    sdif=["data1.sdif", "data2.sdif"],
    output_path="output/context_example"
)
# SQL inside function will use 'src1.data' because of context['schema']
```

## 8. Error Handling

* Errors during the execution of the transformation code (e.g., SQL errors, Python exceptions within your function) are caught and re-raised as an `ExportError`.
* Errors during file I/O (writing outputs) are also raised as `ExportError`.
* Configuration errors (e.g., invalid input types, non-existent input files) typically raise standard Python exceptions like `TypeError`, `ValueError`, or `FileNotFoundError`.
* Syntax errors in code strings/files might raise `ValueError` during initialization or `ExportError` during execution.

Always wrap calls to `transform` or `export` in a `try...except` block to handle potential failures gracefully.
