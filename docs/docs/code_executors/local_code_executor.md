---
sidebar_position: 1
---
# Local Code Executor

The `LocalCodeExecutor` is a code execution engine that runs Python transformation functions directly in the current Python process. It provides a rich execution environment with database connectivity, common libraries, and comprehensive error handling for data transformation tasks.

## 1. Basic Usage

```python
from satif_sdk.code_executors.local_executor import LocalCodeExecutor
from pathlib import Path

# Initialize the executor
executor = LocalCodeExecutor()

# Define transformation code
transformation_code = """
import pandas as pd

def transform(conn):
    # Query the SDIF database
    df = pd.read_sql_query("SELECT * FROM customers", conn)

    # Apply transformations
    df['full_name'] = df['first_name'] + ' ' + df['last_name']

    # Return output files
    return {
        "customers_processed.csv": df,
        "summary.json": {"total_customers": len(df)}
    }
"""

# Execute the transformation
result = executor.execute(
    code=transformation_code,
    function_name="transform",
    sdif_sources={"main": Path("customers.sdif")},
    extra_context={"config": {"format": "excel"}}
)

print(result)  # Dictionary with output file names and data
```

## 2. Initialization Parameters

```python
executor = LocalCodeExecutor(
    initial_context=None,           # Optional: Custom global variables
    disable_security_warning=False  # Optional: Suppress security warnings
)
```

### 2.1 Initial Context

The executor provides a rich set of pre-loaded libraries and utilities:

```python
# Default libraries available in transformation functions
{
    "pd": pandas,           # Data manipulation
    "json": json,          # JSON handling
    "Path": pathlib.Path,  # File path operations
    "sqlite3": sqlite3,    # Database operations
    "datetime": datetime,  # Date/time utilities
    "timedelta": timedelta,# Time intervals
    "re": re,             # Regular expressions
    "uuid": uuid,         # UUID generation
    "os": os,             # Operating system interface
    "io": io,             # I/O operations
    "BytesIO": BytesIO,   # Binary I/O
    "csv": csv,           # CSV handling
    "np": numpy,          # Numerical operations
    "unicodedata": unicodedata,  # Unicode utilities
    "SDIFDatabase": SDIFDatabase  # SDIF database wrapper
}
```

You can extend or override these with custom `initial_context`:

```python
custom_context = {
    "requests": requests,
    "custom_utils": my_utility_module,
    "config": {"api_key": "secret"}
}

executor = LocalCodeExecutor(initial_context=custom_context)
```

## 3. Execution Method

```python
result = executor.execute(
    code=code_string,               # Required: Python code to execute
    function_name=function_name,    # Required: Function to call
    sdif_sources=sdif_sources,      # Required: SDIF database sources
    extra_context=extra_context     # Required: Additional context data
)
```

### 3.1 Code Parameter

The `code` parameter should contain a Python script that defines the specified function:

```python
code = """
import pandas as pd
from datetime import datetime

def process_sales(conn, context):
    # Access configuration from context
    date_format = context.get('date_format', '%Y-%m-%d')

    # Query multiple tables
    sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
    products_df = pd.read_sql_query("SELECT * FROM products", conn)

    # Join and transform data
    result_df = sales_df.merge(products_df, on='product_id')
    result_df['sale_date'] = pd.to_datetime(result_df['sale_date'])

    # Generate multiple outputs
    return {
        "sales_report.xlsx": result_df,
        "summary.json": {
            "total_sales": result_df['amount'].sum(),
            "generated_at": datetime.now().strftime(date_format)
        },
        "raw_data.csv": result_df
    }
"""
```

### 3.2 SDIF Sources

The `sdif_sources` parameter maps schema names to SDIF file paths:

```python
# Single SDIF source
sdif_sources = {"main": "data.sdif"}

# Multiple SDIF sources
sdif_sources = {
    "sales": "sales_data.sdif",
    "inventory": "inventory_data.sdif",
    "customers": "customer_data.sdif"
}
```

Each SDIF file is attached as a schema in the SQLite database, allowing you to query tables like:

```sql
SELECT * FROM sales.transactions
SELECT * FROM inventory.products
SELECT * FROM customers.profiles
```

### 3.3 Function Signatures

The executor supports flexible function signatures:

#### Connection-Based Functions

```python
def transform(conn):
    """Function receives raw SQLite connection"""
    df = pd.read_sql_query("SELECT * FROM table", conn)
    return {"output.csv": df}

def transform_with_context(conn, context):
    """Function receives connection and context"""
    config = context.get('settings', {})
    # ... transformation logic
    return {"output.csv": df}
```

#### SDIFDatabase-Based Functions

```python
def transform(db):
    """Function receives SDIFDatabase wrapper (single source only)"""
    tables = db.list_tables()
    df = db.query("SELECT * FROM main_table")
    return {"output.csv": df}

def transform_with_context(db, context):
    """Function receives database wrapper and context"""
    # ... transformation logic
    return {"output.csv": df}
```

## 4. Database Connectivity

### 4.1 Connection Mode

For multiple SDIF sources or when your function expects a `conn` parameter:

```python
# Multiple sources attached as schemas
sdif_sources = {
    "sales": "sales.sdif",
    "inventory": "inventory.sdif"
}

def transform(conn):
    # Query across schemas
    query = """
    SELECT s.*, i.stock_level
    FROM sales.transactions s
    JOIN inventory.products i ON s.product_id = i.id
    """
    df = pd.read_sql_query(query, conn)
    return {"combined_report.csv": df}
```

### 4.2 SDIFDatabase Mode

For single SDIF source when your function expects a `db` parameter:

```python
# Single source as SDIFDatabase wrapper
sdif_sources = {"main": "data.sdif"}

def transform(db):
    # Use SDIFDatabase methods
    tables = db.list_tables()
    schema = db.get_table_schema("customers")
    df = db.query("SELECT * FROM customers WHERE active = 1")

    return {"active_customers.csv": df}
```

## 5. Error Handling

The executor provides comprehensive error handling:

```python
from satif_core.exceptions import CodeExecutionError

try:
    result = executor.execute(
        code=transformation_code,
        function_name="transform",
        sdif_sources=sdif_sources,
        extra_context={}
    )
except CodeExecutionError as e:
    print(f"Execution failed: {e}")
    # Handle specific execution errors
except Exception as e:
    print(f"Unexpected error: {e}")
```

### 5.1 Common Error Scenarios

- **Function Not Found**: The specified `function_name` doesn't exist in the code
- **Invalid Signature**: Function doesn't accept required parameters (`conn` or `db`)
- **Invalid Return Type**: Function doesn't return a dictionary
- **Database Errors**: Issues with SDIF file access or SQL queries
- **Syntax Errors**: Invalid Python code in the `code` string

## 6. Advanced Usage Examples

### 6.1 Multi-Format Output

```python
transformation_code = """
import pandas as pd
import json

def generate_reports(conn, context):
    # Query data
    sales_df = pd.read_sql_query("SELECT * FROM sales", conn)

    # Generate different format outputs
    summary_stats = {
        "total_sales": float(sales_df['amount'].sum()),
        "avg_sale": float(sales_df['amount'].mean()),
        "sale_count": len(sales_df)
    }

    # Create Excel with multiple sheets
    excel_data = sales_df.copy()
    excel_data['formatted_date'] = pd.to_datetime(excel_data['sale_date']).dt.strftime('%Y-%m-%d')

    return {
        "sales_data.xlsx": excel_data,
        "summary.json": summary_stats,
        "raw_export.csv": sales_df,
        "metadata.txt": f"Report generated with {len(sales_df)} records"
    }
"""

result = executor.execute(
    code=transformation_code,
    function_name="generate_reports",
    sdif_sources={"main": "sales.sdif"},
    extra_context={"report_date": "2024-01-01"}
)
```

### 6.2 Custom Libraries and Utilities

```python
# Custom utility module
class DataProcessor:
    @staticmethod
    def clean_names(df, column):
        return df[column].str.strip().str.title()

    @staticmethod
    def calculate_metrics(df):
        return {
            "mean": df.mean(),
            "median": df.median(),
            "std": df.std()
        }

# Initialize executor with custom context
executor = LocalCodeExecutor(
    initial_context={
        "DataProcessor": DataProcessor,
        "custom_config": {"date_format": "%d/%m/%Y"}
    }
)

transformation_code = """
def advanced_transform(conn, context):
    df = pd.read_sql_query("SELECT * FROM customers", conn)

    # Use custom utility
    df['clean_name'] = DataProcessor.clean_names(df, 'name')

    # Access custom config
    date_fmt = custom_config['date_format']

    return {"processed_customers.csv": df}
"""
```

## 7. Security Considerations

### 7.1 Trusted Environment Only

The `LocalCodeExecutor` should ONLY be used when:

- Code source is completely trusted
- Execution environment is isolated/controlled
- No external user input affects the code
- System security is not a concern

### 7.2 Sandboxed Alternatives

For untrusted code execution, consider:

- **Containerized Executors**: Docker-based isolation
- **Cloud Sandboxes**: Services like E2B, CodePen, etc.
- **Virtual Machines**: Complete OS-level isolation
- **Process Isolation**: Restricted subprocess execution

```python
# Example of safer alternatives (pseudocode)
from satif_sdk.code_executors import SandboxedExecutor, ContainerExecutor

# Use sandboxed execution for untrusted code
safe_executor = SandboxedExecutor(
    max_memory="512MB",
    max_execution_time=30,
    network_access=False
)

# Or containerized execution
container_executor = ContainerExecutor(
    image="python:3.11-slim",
    resource_limits={"memory": "1GB", "cpu": "1.0"}
)
```
