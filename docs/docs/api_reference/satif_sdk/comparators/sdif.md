---
sidebar_label: sdif
title: satif_sdk.comparators.sdif
---

#### SourceMap

> Maps source_id from file1 to source_id in file2

#### TableMap

> Maps table_name from file1 to table_name in file2 (or None if no match)

#### ColumnMap

> Maps col_name from table1 to col_name in table2 (or None if no match)

#### NameMap

> Maps object/media name from file1 to file2 (or None if no match)

## SDIFComparator Objects

```python
class SDIFComparator(Comparator)
```

> Compares two SDIF (SQLite Data Interoperable Format) files for equivalence
> using the SDIFDatabase helper class.
>
> Focuses on comparing the structure and content of user data tables,
> JSON objects, and media data, based on the SDIF specification (v1.0).
> Allows configuration to ignore certain names or metadata aspects.

#### compare

```python
def compare(file_path1: Union[str, Path], file_path2: Union[str, Path],
            **kwargs: Any) -> Dict[str, Any]
```

> Compares two SDIF files using SDIFDatabase and specified options.
>
> Kwargs Options:
>     ignore_user_table_names (bool): Map tables by name only (mapping by original_identifier not implemented). Default: False.
>     ignore_user_column_names (bool): Map columns by name only (mapping by original_column_name not implemented). Default: False.
>     compare_user_table_row_order (bool): Compare table row content respecting order. Default: False.
>     ignore_source_original_file_name (bool): Ignore original_file_name in sdif_sources mapping. Default: False.
>     ignore_object_names (bool): Map objects by name only (mapping by hash not implemented). Default: False.
>     ignore_media_names (bool): Map media by name only (mapping by hash not implemented). Default: False.
>     decimal_places (Optional[int]): Decimal places for comparing REAL/float numbers in tables. Default: None (exact comparison).
>     max_examples (int): Max number of differing row/item examples. Default: 5.
