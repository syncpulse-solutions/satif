---
sidebar_label: csv
title: satif_sdk.comparators.csv
---

## CSVComparator Objects

```python
class CSVComparator(Comparator)
```

> Compares two CSV files for equivalence based on specified criteria.
>
> Provides a detailed report on differences found in headers and row content.
> Supports options like ignoring row order, header case sensitivity, etc.

#### compare

```python
def compare(file_path1: Union[str, Path],
            file_path2: Union[str, Path],
            file_config: Optional[dict[str, Any]] = None,
            **kwargs: Any) -> Dict[str, Any]
```

> Compares two CSV files using specified options.
>
> Kwargs Options:
>     ignore_row_order (bool): Compare row content regardless of order (default: True).
>     check_header_order (bool): Require header columns in the same order (default: True).
>     check_header_case (bool): Ignore case when comparing header names (default: True).
>     strip_whitespace (bool): Strip leading/trailing whitespace from headers/cells (default: True).
>     delimiter (Optional[str]): Delimiter for both files (default: auto-detect).
>     encoding (str): Text encoding for reading files (default: &#x27;utf-8&#x27;).
>     decimal_places (Optional[int]): Number of decimal places to consider for float comparison (default: 2 - 0.01 precision).
>     max_examples (int): Max number of differing row examples (default: 5).
>     check_structure_only (bool): If True, only compare headers. Row data is ignored for equivalence (default: False).
