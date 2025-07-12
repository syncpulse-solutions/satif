---
sidebar_position: 3
---

# CodeAdapter (WIP)

The `CodeAdapter` is designed to execute custom Python code to produce a new, modified SDIF database file. It is mostly used to apply code logic created by other adapters.

## 1. Basic Usage

**a) Code String:**

Pass the Python code as a string. You might need to specify the function name if it's not the default (`adapt`).

```python
ADAPTATION_CODE = """
from satif_sdk import SDIFDatabase # Or from satif_core.sdif_db import SDIFDatabase
from typing import Dict, Any

# Default function name is 'adapt'
def adapt(db: SDIFDatabase) -> Dict[str, Any]:
    cursor = db.conn.cursor()

    # Example: Create a new table based on 'factures'
    # Tables are accessed directly via the db instance.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS normalized_clients AS
        SELECT
            client AS name,
            COUNT(*) AS invoice_count,
            SUM(montant_ttc) AS total_amount
        FROM factures
        GROUP BY client
    ''')

    db.conn.commit() # Commit changes made via the connection
    return {}  # Must return a dictionary, though the content is ignored by CodeAdapter
"""

adapter = CodeAdapter(function=ADAPTATION_CODE)
# If function name was different, e.g., 'normalize_clients':
# adapter = CodeAdapter(function=ADAPTATION_CODE, function_name="normalize_clients")
```

**b) File Path:**

Provide a `pathlib.Path` object pointing to a Python file containing the adaptation function. The function within the file should follow the signature described for code strings.

```python
# Assume 'my_adaptations/invoice_logic.py' contains the 'clean_invoice_data'
# function, defined similarly to the ADAPTATION_CODE example above (accepting db: SDIFDatabase).

adapt_script_path = Path("my_adaptations/invoice_logic.py")

adapter = CodeAdapter(
    function=adapt_script_path,
    function_name="clean_invoice_data"  # Specify the function to run from the file
)
# ... then call adapter.adapt(...)
```

## 3. The Adaptation Function

Your adaptation code needs to adhere to specific requirements based on how it's provided:

### Direct Python Callable:

When providing a direct callable to `CodeAdapter`, it needs to:

* **Signature:** Must accept `db: SDIFDatabase` as the first parameter, and optionally a second parameter `context: Dict[str, Any]`.
* **Operation:** Should modify the database in-place using the `db` object (e.g., `db.conn` for SQL, or methods like `db.write_dataframe`).
* **Return Value:** Typically `None`. Any returned value is ignored, as the changes are made in-place to the copied database.

Example:
```python
from satif_sdk import SDIFDatabase

def adapt_database_callable(db: SDIFDatabase, context: Dict[str, Any]) -> None:
    """Performs adaptations on the database."""
    cursor = db.conn.cursor()
    threshold = context.get("threshold", 100)

    cursor.execute(f"DELETE FROM products WHERE price < {threshold}")
    db.conn.commit()
```

### Code String or Script File:

When providing code as a string or file to be executed by a `CodeExecutor`, the identified function needs to:

* **Signature:** Must accept `db: SDIFDatabase` as the first parameter, and optionally a second parameter `context: Dict[str, Any]`.
* **Operation:** Should modify the database in-place using the provided `SDIFDatabase` instance (`db`). The `CodeExecutor` provides this `db` instance, which is connected to the (copied) database file being adapted. Tables within this database are accessed directly (e.g., using `db.read_table('my_table')` or `db.conn.execute("SELECT * FROM my_table")`).
* **Return Value:** **MUST** return a dictionary (`Dict[str, Any]`), though its contents are ignored by `CodeAdapter`. This is typically required by the underlying `CodeExecutor` interface.

Example (for a function within a code string or script file):
```python
from satif_sdk import SDIFDatabase
from typing import Dict, Any

def clean_data_in_script(db: SDIFDatabase, context: Dict[str, Any]) -> Dict[str, Any]:
    """Cleans data in the database via a script."""
    # Example: Use SDIFDatabase methods
    if 'orders' in db.list_tables():
        orders_df = db.read_table('orders')
        # ... perform some pandas operations on orders_df ...
        # db.write_dataframe(orders_df, 'orders', if_exists='replace', source_id=1) # Assuming source_id=1 exists

    # Example: Or use db.conn for direct SQL
    cursor = db.conn.cursor()
    cursor.execute("UPDATE customers SET phone = REPLACE(phone, ' ', '-') WHERE phone IS NOT NULL")
    db.conn.commit()

    return {}  # Must return a dictionary
```

## 4. Error Handling

* Errors during the execution of the adaptation code (e.g., SQL errors, Python exceptions) are caught and re-raised as an `AdapterError`.
* If an error occurs, the partially adapted output file is removed to prevent corrupted databases.
* Configuration errors (e.g., invalid input types, non-existent input files) typically raise standard Python exceptions like `TypeError`, `ValueError`, or `FileNotFoundError`.
* Syntax errors in code strings/files might raise `ValueError` during initialization or `AdapterError` during execution.

Always wrap calls to `adapt` in a `try...except` block to handle potential failures gracefully:

```python
try:
    output_path = adapter.adapt(sdif="input.sdif")
    print(f"Adaptation successful: {output_path}")
except FileNotFoundError as e:
    print(f"Input file error: {e}")
except AdapterError as e:
    print(f"Adaptation failed: {e}")
```

## 5. Advanced Configuration

You can customize the `CodeAdapter` during initialization:

* **`function_name` (str, default `"adapt"`):** The name of the function to call when `function` is provided as a code string or file path.
* **`extra_context` (Dict[str, Any], default `{}`):** A dictionary of arbitrary Python objects that will be passed as the `context` argument to your adaptation function (if it accepts it).
* **`output_suffix` (str, default `"_adapted"`):** Suffix added to the output filename. For example, with the default, `input.sdif` becomes `input_adapted.sdif`.
* **`code_executor` (CodeExecutor, optional):** In production, you can plug a different execution backend (e.g., a sandboxed environment). If not provided when `function` is a code string/file, a `LocalCodeExecutor` is typically used by default. Ensure the chosen executor provides an `SDIFDatabase` instance to the adaptation function as described.
* **`disable_security_warning` (bool, default `False`):** If `True` and a `LocalCodeExecutor` is auto-created, its security warning is suppressed.

**Example with Custom Configuration:**

```python
from satif_sdk import SDIFDatabase # For type hinting in the function
from satif_sdk.adapters.code import CodeAdapter # The adapter itself
from typing import Dict, Any # For type hinting

# Adaptation function accepting context (could be a direct callable or in a string/file)
def process_with_context(db: SDIFDatabase, context: Dict[str, Any]) -> None: # Or -> Dict[str, Any] if for string/file
    cursor = db.conn.cursor()

    currency = context.get('currency', 'USD')
    min_amount = context.get('min_amount', 0)

    cursor.execute(f"""
        DELETE FROM transactions
        WHERE amount < {min_amount} OR currency != '{currency}'
    """)

    db.conn.commit()
    # If this function was for a code string/file, it would need: return {}

# Instantiate with custom configuration
adapter = CodeAdapter(
    function=process_with_context, # Assuming it's a direct callable here for simplicity
    extra_context={"currency": "EUR", "min_amount": 50},
    output_suffix="_cleaned",
    disable_security_warning=True
)

# Run the adapter
# output_path = adapter.adapt(sdif="input.sdif")
```

Note that when using direct callable functions, the adapter does not need to create temporary files or execute code in a separate environment, making it more efficient for trusted code. When using code strings or files, the `CodeExecutor` handles the execution environment.
```
