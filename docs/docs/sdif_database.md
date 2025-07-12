---
sidebar_position: 7
---

# SDIF Database

The `SDIFDatabase` is a component for creating, managing, and interacting with SDIF (.sqlite) files. It provides a comprehensive API for storing structured tabular data, JSON objects, binary media, and semantic relationships of a datasource in a single SQLite-based file.

## 1. Basic Usage

The simplest way to use `SDIFDatabase` is through the context manager pattern:

```python
from sdif_db import SDIFDatabase
from pathlib import Path
import pandas as pd

# Creating a new SDIF database
with SDIFDatabase(path="my_dataset.sdif", overwrite=True) as db:
    # Add a data source
    source_id = db.add_source(file_name="original_data.csv", file_type="csv",
                              description="Sales data from Q1 2023")

    # Create a table with your data
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Product A", "Product B", "Product C"],
        "price": [19.99, 24.99, 15.50]
    })

    # Write the data to an SDIF table
    db.write_dataframe(df=df, table_name="products", source_id=source_id,
                       description="Product catalog")

# Reading from an existing SDIF database
with SDIFDatabase(path="my_dataset.sdif", read_only=True) as db:
    # Read a table into a DataFrame
    products_df = db.read_table("products")

    # Execute a custom SQL query
    query_result = db.query("SELECT * FROM products WHERE price < 20")

    # Get the database schema
    schema = db.get_schema()
```

## 2. Database Initialization

The `SDIFDatabase` constructor accepts parameters to control how the database is opened and initialized.

```python
from sdif_db import SDIFDatabase
from pathlib import Path

# Create or open an existing SDIF file
db = SDIFDatabase(
    path="data.sdif",              # Path to the SDIF SQLite file (str or Path)
    overwrite=False,               # If True, overwrite the file if it exists
    read_only=False,               # If True, open in read-only mode
    schema_name="db1"              # Schema name when attached in connections
)
```

**Parameters:**

* **`path`:** The path to the SDIF SQLite file (string or `Path`).
* **`overwrite`:** If `True`, overwrite the file if it exists. Ignored when `read_only=True`. Default: `False`.
* **`read_only`:** If `True`, open in read-only mode. Will raise an error if the file doesn't exist. Default: `False`.
* **`schema_name`:** Schema name to use when the database is attached in a connection. Default: `"db1"`.

**Initialization Behavior:**

* If opened in **read-only mode** (`read_only=True`), the file must exist, and no write operations are allowed.
* If opened in **read-write mode** (default) and the file exists:
  * With `overwrite=True`: The existing file will be deleted and a new one created.
  * With `overwrite=False`: The existing file will be opened for reading and writing.
* If opened in **read-write mode** and the file doesn't exist, a new file will be created with the required SDIF metadata tables.

## 3. Adding Data

### 3.1 Adding a Data Source

Before adding any data to an SDIF file, you must register a source.

```python
source_id = db.add_source(
    file_name="sales_2023.xlsx",   # Original file name
    file_type="xlsx",              # Original file type
    description="Sales data for 2023 fiscal year"  # Optional description
)
```

**Parameters:**

* **`file_name`:** The name of the original file.
* **`file_type`:** The type of the original file (e.g., "csv", "xlsx", "json").
* **`description`:** (Optional) Description of the source.

**Returns:**

* The `source_id` of the inserted source, which you'll need when adding tables, objects, or media.

### 3.2 Creating Tables

Create a table structure to store tabular data:

```python
# Define columns with types and properties
columns = {
    "id": {"type": "INTEGER", "primary_key": True, "description": "Unique identifier"},
    "name": {"type": "TEXT", "not_null": True, "description": "Product name"},
    "category": {"type": "TEXT", "description": "Product category"},
    "price": {"type": "REAL", "description": "Current price in USD"},
    "in_stock": {"type": "INTEGER", "description": "Whether item is in stock", "original_column_name": "available"}
}

actual_table_name = db.create_table(
    table_name="products",         # Desired table name
    columns=columns,               # Column definitions
    source_id=source_id,           # Source ID from add_source
    description="Product catalog",  # Optional description
    original_identifier="Products", # Optional original identifier (e.g., sheet name)
    if_exists="fail"               # Behavior if table exists: 'fail', 'replace', or 'add'
)
```

**Parameters:**

* **`table_name`:** The name for the table (must not start with "sdif_").
* **`columns`:** Dictionary mapping column names to their properties.
* **`source_id`:** The source ID (from `add_source`).
* **`description`:** (Optional) Description of the table.
* **`original_identifier`:** (Optional) Original identifier for the table.
* **`if_exists`:** Strategy for handling table name conflicts:
  * `"fail"` (default): Raise an error if table exists.
  * `"replace"`: Drop existing table and recreate.
  * `"add"`: Create a new table with a unique suffixed name (e.g., "table_name_1").

**Column Definition Properties:**

* **`type`:** SQLite type (e.g., "TEXT", "INTEGER", "REAL", "BLOB", "NUMERIC").
* **`primary_key`:** (Optional) If `True`, column is part of primary key.
* **`not_null`:** (Optional) If `True`, column cannot contain NULL values.
* **`unique`:** (Optional) If `True`, column values must be unique.
* **`foreign_key`:** (Optional) Dictionary with keys:
  * `"table"`: Target table name
  * `"column"`: Target column name
  * `"on_delete"`: (Optional) Action on delete (e.g., "CASCADE", "SET NULL")
  * `"on_update"`: (Optional) Action on update
* **`description`:** (Optional) Column description.
* **`original_column_name`:** (Optional) Original name of the column.

**Returns:**

* The actual name of the created table (which might be different if `if_exists="add"` and a conflict occurred).

### 3.3 Inserting Data

Insert data into a previously created table:

```python
# Prepare data as a list of dictionaries
data = [
    {"id": 1, "name": "Widget A", "category": "Widgets", "price": 19.99, "in_stock": 1},
    {"id": 2, "name": "Gadget B", "category": "Gadgets", "price": 24.99, "in_stock": 1},
    {"id": 3, "name": "Thingamajig C", "category": "Misc", "price": 15.50, "in_stock": 0}
]

# Insert the data
db.insert_data(table_name="products", data=data)
```

**Parameters:**

* **`table_name`:** The name of the target table.
* **`data`:** List of dictionaries mapping column names to values.

### 3.4 Writing DataFrames Directly

When working with pandas DataFrames, you can use `write_dataframe` to handle table creation and data insertion in one step:

```python
import pandas as pd

# Create a DataFrame
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Product A", "Product B", "Product C"],
    "price": [19.99, 24.99, 15.50],
    "in_stock": [True, True, False]
})

# Write the DataFrame to a table
db.write_dataframe(
    df=df,                         # DataFrame to write
    table_name="products",         # Desired table name
    source_id=source_id,           # Source ID
    description="Product catalog", # Optional description
    if_exists="fail",              # Behavior if table exists: 'fail', 'replace', 'append'
    columns_metadata={             # Optional metadata for columns
        "name": {"description": "Product name"},
        "price": {"description": "Price in USD", "original_column_name": "unit_price"}
    }
)
```

**Parameters:**

* **`df`:** The pandas DataFrame to write.
* **`table_name`:** The name for the new table.
* **`source_id`:** The source ID (from `add_source`).
* **`description`:** (Optional) Description for the table.
* **`original_identifier`:** (Optional) Original identifier for the table.
* **`if_exists`:** Behavior if the table already exists:
  * `"fail"` (default): Raise an error if table exists.
  * `"replace"`: Drop existing table and recreate.
  * `"append"`: Append data to existing table (not fully implemented yet).
* **`columns_metadata`:** (Optional) Dictionary with column metadata.

### 3.5 Adding JSON Objects

Store structured JSON data:

```python
# Define a JSON object
config = {
    "settings": {
        "theme": "dark",
        "notifications": True,
        "refresh_interval": 60
    },
    "permissions": ["read", "write", "admin"]
}

# Add the JSON object to the SDIF file
db.add_object(
    object_name="app_config",      # Unique name
    json_data=config,              # JSON-serializable data
    source_id=source_id,           # Source ID
    description="Application configuration", # Optional description
    schema_hint={                  # Optional JSON schema
        "type": "object",
        "properties": {
            "settings": {"type": "object"},
            "permissions": {"type": "array", "items": {"type": "string"}}
        }
    }
)
```

**Parameters:**

* **`object_name`:** A unique name for the object.
* **`json_data`:** The data to store (will be converted to a JSON string).
* **`source_id`:** The source ID (from `add_source`).
* **`description`:** (Optional) Description of the object.
* **`schema_hint`:** (Optional) JSON schema as a dictionary.

### 3.6 Adding Binary Media

Store binary data such as images, audio, or any other binary content:

```python
# Read binary data
with open("logo.png", "rb") as f:
    image_data = f.read()

# Add the media to the SDIF file
db.add_media(
    media_name="company_logo",     # Unique name
    media_data=image_data,         # Binary data (bytes)
    media_type="image",            # Type: image, audio, video, binary
    source_id=source_id,           # Source ID
    description="Company logo image", # Optional description
    original_format="png",         # Optional format information
    technical_metadata={           # Optional technical details
        "width": 512,
        "height": 512,
        "color_mode": "RGB"
    }
)
```

**Parameters:**

* **`media_name`:** A unique name for the media.
* **`media_data`:** The binary data (must be bytes).
* **`media_type`:** The type of media (e.g., "image", "audio", "video", "binary").
* **`source_id`:** The source ID (from `add_source`).
* **`description`:** (Optional) Description of the media.
* **`original_format`:** (Optional) Original format (e.g., "png", "mp3").
* **`technical_metadata`:** (Optional) Technical metadata as a dictionary.

### 3.7 Adding Semantic Links

Create semantic relationships between elements:

```python
# Create a semantic link (e.g., relating a product to its image)
db.add_semantic_link(
    link_type="reference",         # Type of link
    from_element_type="table",     # Source element type
    from_element_spec={            # Source element specification
        "table": "products",
        "column": "id",
        "value": 1
    },
    to_element_type="media",       # Target element type
    to_element_spec={              # Target element specification
        "media_name": "product_1_image"
    },
    description="Product image reference" # Optional description
)
```

**Parameters:**

* **`link_type`:** The type of link (e.g., "annotation", "reference", "logical_foreign_key").
* **`from_element_type`:** Type of source element (one of "table", "column", "object", "media", "json_path", "source").
* **`from_element_spec`:** Specification of the source element (as a dictionary).
* **`to_element_type`:** Type of target element (one of "table", "column", "object", "media", "json_path", "source").
* **`to_element_spec`:** Specification of the target element (as a dictionary).
* **`description`:** (Optional) Description of the semantic link.

## 4. Reading Data

### 4.1 Reading Tables

Read tabular data into a pandas DataFrame:

```python
# Read an entire table
products_df = db.read_table("products")

# Process the data
filtered_products = products_df[products_df["price"] < 20]
```

**Parameters:**

* **`table_name`:** The name of the table to read.

**Returns:**

* A pandas DataFrame containing the table data.

### 4.2 Querying with SQL

Execute SQL queries for custom data retrieval:

```python
# Execute a query and get results as a DataFrame
results_df = db.query(
    plain_sql="SELECT p.name, p.price FROM products p WHERE p.category = 'Widgets' ORDER BY p.price DESC",
    return_format="dataframe"  # Options: "dataframe" or "dict"
)

# Execute a query and get results as a list of dictionaries
results_dict = db.query(
    plain_sql="SELECT COUNT(*) as count, category FROM products GROUP BY category",
    return_format="dict"
)
```

**Parameters:**

* **`plain_sql`:** The SQL query string to execute (SELECT statements only).
* **`return_format`:** The desired return format:
  * `"dataframe"` (default): Returns a pandas DataFrame.
  * `"dict"`: Returns a list of dictionary rows.

**Returns:**

* Results in the specified format (pandas DataFrame or list of dictionaries).

**Security Note:**

* The `query` method performs checks to ensure only read-only operations are executed, preventing modifications to the database.
* Opening the database in read-only mode (`read_only=True`) provides the strongest protection against unintended changes.

### 4.3 Retrieving JSON Objects

Get stored JSON objects:

```python
# Get and parse an object
app_config = db.get_object(
    object_name="app_config",      # Object name
    parse_json=True                # Parse JSON string to Python object
)

# Access the data
if app_config:
    theme = app_config["json_data"]["settings"]["theme"]
    permissions = app_config["json_data"]["permissions"]
```

**Parameters:**

* **`object_name`:** The name of the object to retrieve.
* **`parse_json`:** If `True` (default), parse the JSON data into Python objects.

**Returns:**

* A dictionary containing object data and metadata, including:
  * `"json_data"`: The stored data (parsed if `parse_json=True`).
  * `"source_id"`: The source ID.
  * `"description"`: Object description.
  * `"schema_hint"`: Schema information (parsed if `parse_json=True`).

### 4.4 Retrieving Binary Media

Get stored binary media:

```python
# Get media item
logo = db.get_media(
    media_name="company_logo",     # Media name
    parse_json=True                # Parse technical_metadata JSON
)

# Use the binary data
if logo:
    image_bytes = logo["media_data"]
    image_format = logo["original_format"]
    width = logo["technical_metadata"]["width"]

    # Example: save to file
    with open(f"retrieved_logo.{image_format}", "wb") as f:
        f.write(image_bytes)
```

**Parameters:**

* **`media_name`:** The name of the media item to retrieve.
* **`parse_json`:** If `True` (default), parse the technical metadata into a Python object.

**Returns:**

* A dictionary containing media data and metadata, including:
  * `"media_data"`: The binary data (as bytes).
  * `"source_id"`: The source ID.
  * `"media_type"`: Type of media.
  * `"description"`: Media description.
  * `"original_format"`: Original format information.
  * `"technical_metadata"`: Technical metadata (parsed if `parse_json=True`).

## 5. Metadata and Analysis

### 5.1 Listing Resources

Get information about available resources:

```python
# List all sources
sources = db.list_sources()

# List all tables
tables = db.list_tables()

# List all objects
objects = db.list_objects()

# List all media items
media_items = db.list_media()

# List all semantic links
links = db.list_semantic_links()
```

### 5.2 Getting Schema Information

Retrieve the complete database schema:

```python
# Get the full database schema
schema = db.get_schema()

# Access schema components
sdif_properties = schema["sdif_properties"]
sources = schema["sources"]
table_info = schema["tables"]["products"]
objects_info = schema["objects"]
media_info = schema["media"]
links = schema["semantic_links"]

# Example: Get column information for a specific table
columns = schema["tables"]["products"]["columns"]
foreign_keys = schema["tables"]["products"]["foreign_keys"]
```

**Returns:**

* A comprehensive dictionary with the complete database structure, including:
  * Global properties
  * Sources
  * Tables (with columns, types, constraints, metadata)
  * Objects metadata (excluding actual data)
  * Media metadata (excluding binary data)
  * Semantic links

### 5.3 Data Sampling and Analysis

Generate statistical analysis of the data:

```python
# Get sample data and analysis
analysis = db.get_sample_analysis(
    num_sample_rows=5,             # Number of sample rows per table
    top_n_common_values=10,        # Number of most common values to report
    include_objects=True,          # Include object metadata
    include_media=True             # Include media metadata
)

# Access analysis components
tables_analysis = analysis["tables"]
products_analysis = analysis["tables"]["products"]
sample_rows = products_analysis["sample_rows"]
column_stats = products_analysis["column_analysis"]["price"]

# Check numeric statistics for a column (if available)
if "numeric_summary" in column_stats:
    min_price = column_stats["numeric_summary"]["min"]
    max_price = column_stats["numeric_summary"]["max"]
    avg_price = column_stats["numeric_summary"]["mean"]
```

**Parameters:**

* **`num_sample_rows`:** The number of random rows to sample from each table.
* **`top_n_common_values`:** The number of most frequent distinct values to report per column.
* **`include_objects`:** If `True`, includes a list of object names and descriptions.
* **`include_media`:** If `True`, includes a list of media names and descriptions.

**Returns:**

* A detailed analysis dictionary containing:
  * Table information (row counts, sample rows)
  * Column analyses (types, null percentages, distinct counts, common values)
  * Numeric summaries for numeric columns (min, max, mean, median, etc.)
  * Optional object and media information

## 6. Error Handling

`SDIFDatabase` provides specific exceptions for various error conditions:

```python
from sdif_db import SDIFDatabase
from pathlib import Path

try:
    with SDIFDatabase("data.sdif") as db:
        # Attempt operations
        source_id = db.add_source("data.csv", "csv")
        db.create_table("products", {...}, source_id)

except FileNotFoundError:
    # Handle case where file doesn't exist in read_only mode
    print("The SDIF file was not found.")

except PermissionError:
    # Handle permission errors (e.g., writing to read_only database)
    print("Operation not permitted. Check if database is read-only.")

except ValueError as e:
    # Handle validation errors (e.g., invalid table/column names)
    print(f"Validation error: {e}")

except sqlite3.IntegrityError as e:
    # Handle integrity constraint violations
    print(f"Data integrity error: {e}")

except sqlite3.Error as e:
    # Handle other SQLite errors
    print(f"Database error: {e}")

except Exception as e:
    # Catch other unexpected errors
    print(f"Unexpected error: {e}")
    # Potentially log the full stack trace for debugging
```

**Common Exception Types:**

* **`FileNotFoundError`**: When trying to open a non-existent file in read-only mode.
* **`PermissionError`**: When attempting to write to a read-only database.
* **`ValueError`**: For validation errors in input parameters.
* **`TypeError`**: For type-related errors (e.g., providing bytes where str is expected).
* **`sqlite3.IntegrityError`**: For constraint violations (e.g., duplicate unique values).
* **`sqlite3.OperationalError`**: For SQL syntax errors or operational issues.
* **`sqlite3.Error`**: General SQLite database errors.

## 7. Database Lifecycle Management

### 7.1 Opening and Closing

Best practices for managing database connections:

```python
# Recommended: Using the context manager
with SDIFDatabase("data.sdif") as db:
    # Operations inside the block
    db.add_source(...)
    # Connection is automatically closed when leaving the block

# Alternative: Manual opening and closing
db = SDIFDatabase("data.sdif")
try:
    # Operations
    db.add_source(...)
finally:
    # Always close the connection
    db.close()
```

### 7.2 Modifications to Existing Data

Handle changes to existing database content:

```python
# Drop a table and its metadata
db.drop_table("outdated_table")

# Replace a table with new data
db.create_table("products", columns, source_id, if_exists="replace")

# Update data (using standard SQL)
db.query("UPDATE products SET price = price * 1.1 WHERE category = 'Widgets'")
```
