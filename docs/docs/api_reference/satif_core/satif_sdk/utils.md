---
sidebar_label: utils
title: satif_sdk.utils
---

#### ENCODING\_SAMPLE\_SIZE

> Bytes for encoding detection

#### DELIMITER\_SAMPLE\_SIZE

> Bytes for delimiter detection

#### sanitize\_sql\_identifier

```python
def sanitize_sql_identifier(name: str, prefix: str = "item") -> str
```

> Clean up a string to be a safe SQL identifier.
> Replaces problematic characters with underscores, ensures it starts with a
> letter or underscore, and appends an underscore if it&#x27;s a basic SQL keyword.

#### normalize\_list\_argument

```python
def normalize_list_argument(arg_value: Optional[Union[T, List[Optional[T]]]],
                            arg_name_for_error: str,
                            expected_len: int) -> List[Optional[T]]
```

> Normalizes an argument that can be a single item or a list into a list
> of a specific expected length.
>
> If arg_value is a single item, it&#x27;s repeated expected_len times.
> If arg_value is a list, its length must match expected_len.
> If arg_value is None, a list of Nones of expected_len is returned.

#### validate\_skip\_rows\_config

```python
def validate_skip_rows_config(
        config: SkipRowsConfig,
        file_name_for_error: Optional[str] = None) -> SkipRowsConfig
```

> Validate types and values for skip_rows config.

#### validate\_skip\_columns\_config

```python
def validate_skip_columns_config(
        config: SkipColumnsConfig,
        file_name_for_error: Optional[str] = None) -> SkipColumnsConfig
```

> Validate types and values for skip_columns config.

#### parse\_skip\_rows\_config

```python
def parse_skip_rows_config(
        skip_rows_config: SkipRowsConfig) -> Union[int, Set[int]]
```

> Parse validated skip_rows config into int (for initial skip) or Set[int] (for indexed skip).

#### parse\_skip\_columns\_config

```python
def parse_skip_columns_config(
        skip_columns_config: SkipColumnsConfig) -> Tuple[Set[int], Set[str]]
```

> Parse validated skip_columns config into separate sets for indices and names.

#### detect\_file\_encoding

```python
def detect_file_encoding(file_path: Path,
                         sample_size: int = ENCODING_SAMPLE_SIZE) -> str
```

> Detect file encoding using charset-normalizer.

#### detect\_csv\_delimiter

```python
def detect_csv_delimiter(sample_text: str) -> str
```

> Detect CSV delimiter using clevercsv.Sniffer.
