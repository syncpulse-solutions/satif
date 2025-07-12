---
sidebar_label: code
title: satif_sdk.adapters.code
---

## AdapterError Objects

```python
class AdapterError(Exception)
```

> Custom exception for adapter errors.

## CodeAdapter Objects

```python
class CodeAdapter(Adapter)
```

> Executes custom Python code to adapt data within an SDIF database,
> producing a new, adapted SDIF database file.
>
> The adaptation logic can be provided as:
> 1.  A direct Python callable:
> The function should modify the passed `SDIFDatabase` instance in-place.
> Signatures:
> - `def adapt(db: SDIFDatabase) -> None:`
> - `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`
>
> 2.  A string containing Python code or a Path to a Python script file:
> This code will be executed by a `CodeExecutor`.
> The function identified by `function_name` within the code should be
> prepared to accept specific arguments provided by the executor:
> - EITHER `db: SDIFDatabase` (an instance connected to the database to be adapted)
> - OR `conn: sqlite3.Connection` (a direct connection to that database).
> - Optionally, `context: Dict[str, Any]` if it needs `extra_context`.
>
> Example Signatures:
> - `def adapt(db: SDIFDatabase) -> Dict[str, Any]:`
> - `def adapt(db: SDIFDatabase) -> None:`0
> - `def adapt(db: SDIFDatabase) -> None:`1
> - `def adapt(db: SDIFDatabase) -> None:`2
>
> This function **must return a dictionary** (e.g., `def adapt(db: SDIFDatabase) -> None:`3) to comply with the
> `CodeExecutor` interface, though the dictionary content is ignored by `def adapt(db: SDIFDatabase) -> None:`5.
> If `def adapt(db: SDIFDatabase) -> None:`6 is used, the database being adapted is also ATTACHed with the schema name &quot;db&quot;
> (e.g., `def adapt(db: SDIFDatabase) -> None:`7 refers to a table in the main file).
> If `db: SDIFDatabase` is used, methods on the `def adapt(db: SDIFDatabase) -> None:`9 object operate directly on this main database file.
>
>
> **Arguments**:
>
> - `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`0 - The callable, code string, or file path containing the adaptation logic.
> - `function_name` - Name of the function to execute (defaults to &quot;adapt&quot;).
>   Used when `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`0 is a code string or file path.
> - `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`3 - Optional `CodeExecutor` instance. If `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`0 is code/file
>   and this is None, a `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`6 will be used by default.
> - `extra_context` - Optional dictionary of objects to make available.
>   - For direct callables: passed as `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`8 argument if accepted.
>   - For code via executor: passed to `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`9 and
>   made available in the execution scope and as `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`8 argument.
> - `CodeExecutor`1 - Suffix for the output adapted file (defaults to &quot;_adapted&quot;).
> - `CodeExecutor`2 - If True and a `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`6 is auto-created,
>   its security warning for local execution is suppressed.
>   Defaults to False.

#### adapt

```python
def adapt(sdif: SDIFPath) -> Path
```

> Applies the adaptation logic to the input SDIF database file,
> producing a new adapted SDIF file.
>
> **Arguments**:
>
> - `sdif` - The SDIF data source(s) to adapt. This can be:
>   - A single SDIF file path (str or Path).
>   - An `SDIFDatabase` instance.
>
>
> **Returns**:
>
>   The path to the newly created adapted SDIF file.
>
>
> **Raises**:
>
> - `FileNotFoundError` - If the input SDIF file path does not exist.
> - `AdapterError` - If code execution or adaptation logic fails.
