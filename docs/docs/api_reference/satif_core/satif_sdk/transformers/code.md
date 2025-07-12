---
sidebar_label: code
title: satif_sdk.transformers.code
---

## ExportError Objects

```python
class ExportError(Exception)
```

> Custom exception for export errors.

## CodeTransformer Objects

```python
class CodeTransformer(Transformer)
```

> Executes custom Python code to transform data from an SDIF database into desired output files.
>
> Responsibilities:
> - Initializes with transformation logic (callable, code string, or file path) and a CodeExecutor.
> - When transforming with a direct callable: Manages SQLite connection setup (attaching SDIFs)
> and executes the callable in the current environment.
> - When transforming with a code string/file: Prepares SDIF source information and delegates
> to the configured `CodeExecutor`, which then handles DB setup and code execution
> in its own environment (e.g., locally or sandboxed).
> - Exports the results returned by the transformation logic to files or a zip archive.
>
> **Arguments**:
>
> - `function` - The transformation logic (Python callable, code string, or Path to script).
> - `function_name` - Name of the function to call if `function` is a code string/file.
>   Defaults to &quot;transform&quot;. Ignored if `function` is a callable.
> - `code_executor` - An instance of a `CodeExecutor` subclass. If None and transformation logic
>   is a code string or file path, a `LocalCodeExecutor` is instantiated.
> - `extra_context` - Dictionary of extra objects to pass to the transformation function&#x27;s context
>   or make available in the executor&#x27;s global scope.
> - `db_schema_prefix` - Prefix for auto-generated schema names when a list of SDIFs is given.
>   Defaults to &quot;db&quot;.
>   Transformation Function Signature:
>   The transform function should accept these parameters:
>   - `function`0 (sqlite3.Connection): A connection to an in-memory SQLite
>   database with all input SDIF files attached as schemas.
>   - `function`1 (Dict[str, Any], optional): Extra context values if needed.
>
>   The function MUST return a dictionary (`function`2) where:
>   - Keys (str): Relative output filenames (e.g., &quot;orders_extract.csv&quot;, &quot;summary/report.json&quot;).
>   - Values (Any): Data to write (e.g., `function`3, `function`4, `function`5, `function`6, `function`7).
>   The file extension in the key typically determines the output format.
>
>
> **Example**:
>
>     `function`8

#### transform

```python
def transform(
    sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]]
) -> Dict[str, Any]
```

> Transforms data from SDIF input(s) using the configured logic.
>
> - If a direct Python callable was provided to `__init__`, this method sets up
>   the SQLite connection, ATTACHes databases, executes the callable directly,
>   and then cleans up the connection.
> - If a code string or file path was provided, this method prepares a map of
>   SDIF sources and delegates to `_execute_transformation`, which in turn uses
>   the configured `CodeExecutor`. The `CodeExecutor` is then responsible for
>   database setup and code execution within its own environment.
> - Handles `SDIFDatabase` instances by using their pre-existing connection if
>   a direct callable is used.

#### export

```python
def export(sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str,
                                                                    SDIFPath]],
           output_path: FilePath = Path("."),
           zip_archive: bool = False) -> Path
```

> Transforms data from SDIF input(s) and exports results to files.
> This is a convenience method that combines transform() and export().
>
> **Arguments**:
>
> - `sdif` - Input SDIF data source. Can be:
>   - A single path (str/Path)
>   - A list of paths
>   - An SDIFDatabase instance
>   - A dictionary mapping schema names to paths (e.g., {&quot;customers&quot;: &quot;customers.sdif&quot;})
> - `output_path` - Path to the output file (if zip_archive=True or single output)
>   or directory (if multiple outputs). Defaults to current directory.
> - `zip_archive` - If True, package all output files into a single ZIP archive
>   at the specified output_path.
>
>
> **Returns**:
>
>   Path to the created output file or directory.
>
>
> **Raises**:
>
> - `ExportError` - If any error occurs during transformation or writing.
> - `ValueError` - If input arguments are invalid.
> - `FileNotFoundError` - If an input SDIF file does not exist.
> - `TypeError` - If the &#x27;sdif&#x27; argument is of an unsupported type.

#### transformation

```python
def transformation(func=None, name=None)
```

> Decorator to register a function as a transformation.
> Can be used with or without arguments.
>
> @transformation
> def my_transform(conn): # conn is sqlite3.Connection
> ...
>
> @transformation(name=&quot;custom_name&quot;)
> def my_transform_custom_name(conn, context: dict):
> ...
>
> **Arguments**:
>
> - `func` - The function to decorate.
> - `name` - Optional custom name for the transformation. If None, function&#x27;s __name__ is used.
>
>
> **Returns**:
>
>   The decorated function, now registered and marked as a transformation.
