---
sidebar_label: database
title: sdif_db.database
---

## SDIFDatabase Objects

```python
class SDIFDatabase()
```

#### \_\_init\_\_

```python
def __init__(path: Union[str, Path],
             overwrite: bool = False,
             read_only: bool = False,
             schema_name: str = "db1")
```

> Initialize the SDIFDatabase.
>
> **Arguments**:
>
> - `path` - Path to the SDIF SQLite file.
> - `overwrite` - If True, overwrite the file if it exists (only applies if read_only=False).
> - `read_only` - If True, open the database in read-only mode. Will raise error if file doesn&#x27;t exist.
> - `schema_name` - Schema name to use when the database is attached in a connection. Default: &quot;db1&quot;

#### add\_source

```python
def add_source(file_name: str,
               file_type: str,
               description: Optional[str] = None) -> int
```

> Add a source to the SDIF file.
>
> **Arguments**:
>
> - `file_name` - The name of the original file
> - `file_type` - The type of the original file (csv, xlsx, json, etc.)
> - `description` - Optional description of the source
>
>
> **Returns**:
>
>   The source_id of the inserted source

#### create\_table

```python
def create_table(table_name: str,
                 columns: Dict[str, Dict[str, Any]],
                 source_id: int,
                 description: Optional[str] = None,
                 original_identifier: Optional[str] = None,
                 if_exists: str = "fail") -> str
```

> Create a data table in the SDIF file and registers its metadata.
> Handles conflicts if a table with the same name already exists.
>
> **Arguments**:
>
> - `table_name` - The name of the table to create (must not start with &#x27;sdif_&#x27;)
> - `columns` - Dict mapping column names to their properties.
> - `Example` - {&quot;col_name&quot;: {&quot;type&quot;: &quot;TEXT&quot;, &quot;not_null&quot;: True, &quot;description&quot;: &quot;...&quot;, &quot;original_column_name&quot;: &quot;...&quot;, &quot;original_format&quot;: &quot;...&quot;}}
>   Supported properties: type (SQLite type), not_null (bool), primary_key (bool),
>   foreign_key ({&quot;table&quot;: &quot;target_table&quot;, &quot;column&quot;: &quot;target_col&quot;}),
>   description (str), original_column_name (str), original_format (str).
> - `source_id` - The source_id reference.
> - `description` - Optional description of the table for sdif_tables_metadata.
> - `original_identifier` - Optional original identifier for sdif_tables_metadata.
> - `if_exists` - Strategy to handle pre-existing table:
>   - &quot;fail&quot; (default): Raise ValueError if table exists.
>   - &quot;replace&quot;: Drop existing table and create anew.
>   - &quot;add&quot;: Create the new table with a unique suffixed name (e.g., table_name_1).
>
>
> **Returns**:
>
>   The actual name of the table created in the database (could be suffixed).
>
>
> **Raises**:
>
> - `PermissionError` - If database is read-only.
> - `ValueError` - If table_name is invalid, columns are empty, source_id is invalid,
>   or if table exists and if_exists=&#x27;fail&#x27;, or invalid if_exists value.
> - `sqlite3.Error` - For underlying database errors.
> - `columns`0 - If &#x27;add&#x27; fails to find a unique name.

#### insert\_data

```python
def insert_data(table_name: str, data: List[Dict[str, Any]])
```

> Insert data into a table. Assumes table has been created via create_table.
>
> **Arguments**:
>
> - `table_name` - The name of the table
> - `data` - List of dictionaries mapping column names to values

#### add\_object

```python
def add_object(object_name: str,
               json_data: Any,
               source_id: int,
               description: Optional[str] = None,
               schema_hint: Optional[Dict] = None)
```

> Add a JSON object to the SDIF file.
>
> **Arguments**:
>
> - `object_name` - A unique name for the object
> - `json_data` - The data to store (will be converted to JSON string)
> - `source_id` - The source_id reference
> - `description` - Optional description
> - `schema_hint` - Optional JSON schema (as dict, will be stored as JSON string)

#### add\_media

```python
def add_media(media_name: str,
              media_data: bytes,
              media_type: str,
              source_id: int,
              description: Optional[str] = None,
              original_format: Optional[str] = None,
              technical_metadata: Optional[Dict] = None)
```

> Add binary media data to the SDIF file.
>
> **Arguments**:
>
> - `media_name` - A unique name for the media
> - `media_data` - The binary data (must be bytes)
> - `media_type` - The type of media (image, audio, video, binary)
> - `source_id` - The source_id reference
> - `description` - Optional description
> - `original_format` - Optional format (png, jpeg, etc.)
> - `technical_metadata` - Optional technical metadata (as dict, stored as JSON string)

#### add\_semantic\_link

```python
def add_semantic_link(link_type: str,
                      from_element_type: str,
                      from_element_spec: Dict,
                      to_element_type: str,
                      to_element_spec: Dict,
                      description: Optional[str] = None)
```

> Add a semantic link between elements.
>
> **Arguments**:
>
> - `link_type` - The type of link (annotation, reference, logical_foreign_key)
> - `from_element_type` - Type of source element (&#x27;table&#x27;, &#x27;column&#x27;, &#x27;object&#x27;, &#x27;media&#x27;, &#x27;json_path&#x27;, &#x27;source&#x27;)
> - `from_element_spec` - Specification of the source element (as dict, stored as JSON string)
> - `to_element_type` - Type of target element (&#x27;table&#x27;, &#x27;column&#x27;, &#x27;object&#x27;, &#x27;media&#x27;, &#x27;json_path&#x27;, &#x27;source&#x27;)
> - `to_element_spec` - Specification of the target element (as dict, stored as JSON string)
> - `description` - Optional description

#### close

```python
def close()
```

> Close the database connection.

#### \_\_enter\_\_

```python
def __enter__()
```

> Context manager enter method.
>
> **Returns**:
>
> - `self` - The database object

#### \_\_exit\_\_

```python
def __exit__(exc_type, exc_val, exc_tb)
```

> Context manager exit method.
>
> **Arguments**:
>
> - `exc_type` - Exception type
> - `exc_val` - Exception value
> - `exc_tb` - Exception traceback

#### \_\_del\_\_

```python
def __del__()
```

> Ensure connection is closed when object is garbage collected
> Note: __del__ can be unreliable, using context manager is better.

#### get\_properties

```python
def get_properties() -> Optional[Dict[str, Any]]
```

> Get the global properties from sdif_properties.

#### list\_sources

```python
def list_sources() -> List[Dict[str, Any]]
```

> List all sources from sdif_sources.

#### list\_tables

```python
def list_tables() -> List[str]
```

> List the names of all user data tables registered in metadata.

#### get\_table\_metadata

```python
def get_table_metadata(table_name: str) -> Optional[Dict[str, Any]]
```

> Get metadata for a specific user table from SDIF metadata tables.

#### read\_table

```python
def read_table(table_name: str) -> pd.DataFrame
```

> Read a user data table into a pandas DataFrame.
>
> **Arguments**:
>
> - `table_name` - The name of the user data table to read.
>
>
> **Returns**:
>
>   A pandas DataFrame containing the table data.
>
>
> **Raises**:
>
> - `ValueError` - If the table does not exist physically in the database.
> - `sqlite3.Error` - If there&#x27;s an issue reading from the database.

#### drop\_table

```python
def drop_table(table_name: str)
```

> Drops a table and its associated metadata.

#### list\_objects

```python
def list_objects() -> List[str]
```

> List the names of all stored JSON objects.

#### get\_object

```python
def get_object(object_name: str,
               parse_json: bool = True) -> Optional[Dict[str, Any]]
```

> Retrieve a stored JSON object and its metadata.
>
> **Arguments**:
>
> - `object_name` - The name of the object to retrieve.
> - `parse_json` - If True (default), parse json_data and schema_hint strings into Python objects.
>   If False, return them as raw strings.
>
>
> **Returns**:
>
>   A dictionary containing the object data and metadata, or None if the object doesn&#x27;t exist.
>   &#x27;json_data&#x27; and &#x27;schema_hint&#x27; keys will contain parsed objects or strings based on parse_json flag.
>
>
> **Raises**:
>
> - `ValueError` - If parsing fails when parse_json is True.

#### list\_media

```python
def list_media() -> List[str]
```

> List the names of all stored media items.

#### get\_media

```python
def get_media(media_name: str,
              parse_json: bool = True) -> Optional[Dict[str, Any]]
```

> Retrieve stored media data and its metadata.
>
> **Arguments**:
>
> - `media_name` - The name of the media item to retrieve.
> - `parse_json` - If True (default), parse technical_metadata string into Python object.
>   If False, return it as a raw string.
>
>
> **Returns**:
>
>   A dictionary containing the media data (&#x27;media_data&#x27; key as bytes)
>   and its metadata, or None if the media item doesn&#x27;t exist.
>   &#x27;technical_metadata&#x27; key will contain parsed object or string based on parse_json flag.
>
>
> **Raises**:
>
> - `ValueError` - If parsing fails when parse_json is True.

#### list\_semantic\_links

```python
def list_semantic_links(parse_json: bool = True) -> List[Dict[str, Any]]
```

> List all semantic links.
>
> **Arguments**:
>
> - `parse_json` - If True (default), parse from/to_element_spec strings into Python objects.
>   If False, return them as raw strings.
>
>
> **Returns**:
>
>   A list of dictionaries, each representing a semantic link.
>   &#x27;from_element_spec&#x27; and &#x27;to_element_spec&#x27; keys will contain parsed objects or strings.
>
>
> **Raises**:
>
> - `ValueError` - If parsing fails when parse_json is True.

#### get\_schema

```python
def get_schema() -> Dict[str, Any]
```

> Retrieves the complete structural schema of the SDIF database.
>
> This includes global properties, sources, detailed table schemas (columns, types,
> constraints, metadata), object metadata (excluding data), media metadata
> (excluding data), and semantic links.
>
> **Returns**:
>
>   A dictionary representing the database schema. Structure:
>   {
> - `"sdif_properties"` - { ... },
> - `"sources"` - [ { ... } ],
> - `"tables"` - {
> - `"table_name"` - {
> - `"metadata"` - { ... }, // From sdif_tables_metadata
> - `"columns"` - [ { name, sqlite_type, not_null, primary_key, // From PRAGMA
>   description, original_data_format // From sdif_columns_metadata
>   }, ... ],
> - `"foreign_keys"` - [ { from_column, target_table, target_column, on_update, on_delete, match }, ... ] // From PRAGMA
>   }, ...
>   },
> - `"objects"` - {
> - `"object_name"` - { source_id, description, schema_hint }, ... // Parsed schema_hint
>   },
> - `"media"` - {
> - `"sources"`0 - { source_id, media_type, description, original_format, technical_metadata }, ... // Parsed tech meta
>   },
> - `"sources"`1 - [ { ... } ] // Parsed specs
>   }
>
>
> **Raises**:
>
> - `"sources"`2 - If there are issues querying the database.
> - `"sources"`3 - If inconsistencies are found (e.g., invalid JSON in metadata).

#### get\_sample\_analysis

```python
def get_sample_analysis(num_sample_rows: int = 5,
                        top_n_common_values: int = 10,
                        include_objects: bool = False,
                        include_media: bool = False) -> Dict[str, Any]
```

> Provides a sample of data and basic statistical analysis for tables
> within the SDIF database, intended to give a better understanding
> of the data content beyond just the schema.
>
> **Arguments**:
>
> - `num_sample_rows` - The number of random rows to sample from each table.
> - `top_n_common_values` - The number of most frequent distinct values to report per column.
> - `include_objects` - If True, includes a list of object names and descriptions.
> - `include_media` - If True, includes a list of media names and descriptions.
>
>
> **Returns**:
>
>   A dictionary containing samples and analysis. Structure:
>   {
> - `"tables"` - {
> - `"table_name"` - {
> - `"row_count"` - int, // From metadata
> - `"sample_rows"` - [ {col1: val1, ...}, ... ], // List of sample row dicts
> - `"column_analysis"` - {
> - `"column_name"` - {
> - `top_n_common_values`0 - str, // From PRAGMA in get_schema (or re-query)
> - `top_n_common_values`1 - str, // Pandas inferred type
> - `top_n_common_values`2 - float,
> - `top_n_common_values`3 - int,
> - `top_n_common_values`4 - bool,
> - `top_n_common_values`5 - bool, # Basic check
> - `top_n_common_values`6 - [ [value, count], ... ], // Top N
> - `top_n_common_values`7 - { // Only if is_numeric
> - `top_n_common_values`8 - float/int,
> - `top_n_common_values`9 - float/int,
> - `include_objects`0 - float,
> - `include_objects`1 - float,
> - `include_objects`2 - float,
> - `include_objects`3 - float, # 25th percentile
> - `include_objects`4 - float, # 75th percentile
>   }
>   }, ...
>   }
>   }, ...
>   },
> - `include_objects`5 - { // Optional, based on include_objects
> - `include_objects`6 - int,
> - `include_objects`7 - [ {&quot;name&quot;: str, &quot;description&quot;: str, &quot;source_id&quot;: int }, ... ]
>   },
> - `include_objects`8 - { // Optional, based on include_media
> - `include_objects`6 - int,
> - `include_objects`7 - [ {&quot;name&quot;: str, &quot;description&quot;: str, &quot;media_type&quot;: str, &quot;source_id&quot;: int }, ... ]
>   }
>   }
>
>
> **Raises**:
>
> - `include_media`1 - If database querying fails.
> - `include_media`2 - If table listed in metadata cannot be read.
> - `include_media`3 - For unexpected errors during analysis.

#### query

```python
def query(
    plain_sql: str,
    return_format: str = "dataframe"
) -> Union[pd.DataFrame, List[Dict[str, Any]]]
```

> Executes a read-only SQL query string against the SDIF database.
>
> This method allows flexible querying using raw SQL SELECT statements.
> It includes checks to prevent modification queries (INSERT, UPDATE, DELETE, etc.)
> and potentially harmful PRAGMA/ATTACH commands, ensuring the database
> state is not altered by the query. This is suitable for use cases
> where an automated agent (like an AI) generates queries for analysis
> or transformation planning, but should not modify the source data.
>
> **Arguments**:
>
> - `plain_sql` - The raw SQL SELECT query string to execute.
>   No parameter binding is performed; the string is executed as is.
> - `return_format` - The desired format for the results.
>   Options:
>   - &quot;dataframe&quot; (default): Returns a pandas DataFrame.
>   - &quot;dict&quot;: Returns a list of dictionaries (one per row).
>
>
> **Returns**:
>
>   The query results in the specified format (pandas DataFrame or list of dicts).
>
>
> **Raises**:
>
> - `PermissionError` - If the query appears to be non-SELECT or contains
>   disallowed keywords (e.g., UPDATE, PRAGMA, ATTACH, INSERT).
> - `sqlite3.Error` - If any database error occurs during query execution.
> - `ValueError` - If an invalid `return_format` is specified.
>
>   Safety Note:
>   This method executes the provided SQL string directly after performing
>   keyword checks to prevent modifications. While these checks block common
>   modification commands, using the SDIFDatabase in read-only mode
>   (`read_only=True`) provides the strongest guarantee against unintended
>   data changes at the database level.

#### write\_dataframe

```python
def write_dataframe(df: pd.DataFrame,
                    table_name: str,
                    source_id: int,
                    description: Optional[str] = None,
                    original_identifier: Optional[str] = None,
                    if_exists: str = "fail",
                    columns_metadata: Optional[Dict[str, Dict[str,
                                                              Any]]] = None)
```

> Writes a pandas DataFrame to a new table in the SDIF database.
> Handles table creation, metadata registration, and data insertion.
>
> **Arguments**:
>
> - `df` - The pandas DataFrame to write.
> - `table_name` - The name for the new table.
> - `source_id` - The ID of the source for this data.
> - `description` - Optional description for the table.
> - `original_identifier` - Optional original identifier for the table (e.g., sheet name).
> - `if_exists` - Behavior if the table already exists (&#x27;fail&#x27;, &#x27;replace&#x27;, &#x27;append&#x27;).
> - `columns_metadata` - Optional. A dictionary where keys are final column names
>   and values are dicts like {&quot;description&quot;: &quot;...&quot;, &quot;original_column_name&quot;: &quot;...&quot;}.
>   This metadata is used during table creation.
