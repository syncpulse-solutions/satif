---
sidebar_label: local_executor
title: satif_sdk.code_executors.local_executor
---

## LocalCodeExecutor Objects

```python
class LocalCodeExecutor(CodeExecutor)
```

> Executes user-provided Python code strings locally using Python&#x27;s built-in `exec`.
>
> This executor is responsible for:
> 1. Setting up an SQLite database environment based on provided SDIF source file paths.
> This includes creating an in-memory database (if multiple sources) or connecting
> to a single source, and then ATTACHing all specified SDIF files as schemas.
> 2. Executing a given `code` string in an environment where this database connection
> (or an SDIFDatabase wrapper) is accessible, along with other standard libraries
> and provided `extra_context`.
> 3. Identifying a specific function within the executed `code` by its `function_name`.
> 4. Calling this identified function, passing it the live SQLite connection (as `conn`)
> or an SDIFDatabase instance (as `db`), and context.
> 5. Returning the result produced by the called function.
> 6. Ensuring the database connection is properly closed and resources are cleaned up.
>
> **Security Warning:**
> This executor runs arbitrary Python code directly on the host machine where it is instantiated.
> It provides **NO SANDBOXING OR SECURITY ISOLATION**. Therefore, it should **ONLY** be used
> in trusted environments and with code from trusted sources.

#### \_\_init\_\_

```python
def __init__(initial_context: Optional[Dict[str, Any]] = None,
             disable_security_warning: bool = False)
```

> Initializes the LocalCodeExecutor.
>
> **Arguments**:
>
>   initial_context:
>   An optional dictionary of global variables to make available
>   during code execution. These will be merged with (and can
>   override) the default set of globals provided by the executor.
> - `disable_security_warning` - If True, suppresses the security warning log.

#### execute

```python
def execute(code: str, function_name: str, sdif_sources: Dict[str, Path],
            extra_context: Dict[str, Any]) -> Dict[str, Any]
```

> Sets up a database, executes the code string to define a function,
> then calls that function with the database connection (as `conn` or `db`) and context.
>
> **Arguments**:
>
>   code:
>   A string containing the Python script to be executed. This script
>   is expected to define the function identified by `function_name`.
>   It can include imports, helper functions, and class definitions
>   as needed for the main transformation function.
>   function_name:
>   The name of the function (defined in `code`) to be invoked.
>   sdif_sources:
>   A dictionary mapping schema names (str) to resolved `Path` objects
>   of the SDIF database files. This executor will create/connect to
>   an SQLite database and ATTACH these sources.
>   extra_context:
>   A dictionary of additional objects and data to be made available
>   to the transformation logic.
>   - The entire `extra_context` dictionary is passed as the `context`
>   argument to the transformation function if its signature includes it.
>   - Additionally, all key-value pairs in `extra_context` are injected
>   as global variables into the environment where the `code` string
>   is initially executed. If `extra_context` contains keys that
>   match standard globals (e.g., &#x27;pd&#x27;, &#x27;json&#x27;) or the explicitly
>   provided &#x27;conn&#x27; or &#x27;context&#x27; globals, they will be overwritten
>   in that global scope.
>
>
> **Returns**:
>
>   A dictionary, which is the result of calling the user-defined
>   transformation function (`function_name`). The keys are typically
>   output filenames, and values are the data to be written.
>
>
> **Raises**:
>
> - `db`1 - If any error occurs during the process, including:
>   - Database setup errors from `db`2.
>   - Syntax errors in the `code` string.
>   - The specified `function_name` not being found after executing `code`.
>   - The identified `function_name` not being a callable function.
>   - The function having an incompatible signature (e.g., not accepting `conn`).
>   - The function not returning a dictionary.
>   - Any exception raised during the execution of the user&#x27;s transformation function.
