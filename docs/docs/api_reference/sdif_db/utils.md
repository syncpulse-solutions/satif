---
sidebar_label: utils
title: sdif_db.utils
---

## DBConnectionError Objects

```python
class DBConnectionError(Exception)
```

> Exception raised for errors in SDIF database connection.

#### create\_db\_connection

```python
def create_db_connection(
    sdif_sources: Dict[str, Union[Path, str]]
) -> Tuple[sqlite3.Connection, Dict[str, Path]]
```

> Creates an SQLite connection and attaches SDIF sources as schemas.
>
> - If one source is provided, connects directly to that file and attaches it
> under its specified schema name. The main connection will be to the file itself.
> - If multiple sources are provided, creates an in-memory database and attaches
> all sources.
>
> **Arguments**:
>
> - `sdif_sources` - Dictionary mapping schema names to resolved SDIF file paths.
>
>
> **Returns**:
>
>   A tuple containing the sqlite3.Connection and a dictionary of
>   successfully attached schemas (schema_name: path).
>
>
> **Raises**:
>
> - `DBConnectionError` - If no sources are provided, a file is not found,
>   or an SQLite error occurs during connection/attachment.

#### cleanup\_db\_connection

```python
def cleanup_db_connection(conn: Optional[sqlite3.Connection],
                          attached_schemas: Dict[str, Path],
                          should_close: bool = True) -> None
```

> Cleans up an SQLite connection by detaching schemas and optionally closing it.
>
> **Arguments**:
>
> - `conn` - The sqlite3.Connection to clean up. Can be None.
> - `attached_schemas` - A dictionary of schemas (schema_name: path) that were attached
>   and should be detached.
> - `should_close` - Whether to close the connection after detaching schemas.
