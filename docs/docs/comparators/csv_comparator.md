---
sidebar_position: 1
---
# CSV Comparator

The `CSVComparator` compares two CSV files for equivalence and provides detailed difference reports.

## Basic Usage

```python
from satif_sdk.comparators import CSVComparator

comparator = CSVComparator()

result = comparator.compare(
    file_path1="file1.csv",
    file_path2="file2.csv",
    ignore_row_order=True,
    check_header_case=False
)

print(f"Files equivalent: {result['are_equivalent']}")
for message in result['summary']:
    print(f"- {message}")
```

## Parameters

- **`ignore_row_order`**: Compare content regardless of row order (default: `True`)
- **`check_header_order`**: Require headers in same order (default: `True`)
- **`check_header_case`**: Case-sensitive header comparison (default: `True`)
- **`strip_whitespace`**: Remove leading/trailing whitespace (default: `True`)
- **`delimiter`**: CSV delimiter, auto-detected if `None` (default: `None`)
- **`encoding`**: File encoding (default: `"utf-8"`)
- **`decimal_places`**: Decimal precision for float comparison (default: `2`)
- **`max_examples`**: Maximum difference examples in report (default: `5`)
- **`check_structure_only`**: Compare only headers, ignore row data (default: `False`)

## Output Structure

```python
{
    "files": {"file1": "path1.csv", "file2": "path2.csv"},
    "comparison_params": { ... },
    "are_equivalent": bool,
    "summary": ["High-level findings..."],
    "details": {
        "errors": ["Any read errors..."],
        "header_comparison": {
            "result": "Identical|Different names|...",
            "diff": ["Specific differences..."]
        },
        "row_comparison": {
            "result": "Identical content|Different content|...",
            "row_count1": 100,
            "row_count2": 95,
            "unique_rows1": [[row_data], ...],
            "unique_rows2": [[row_data], ...],
            "count_diffs": [{"row": [data], "count1": 2, "count2": 1}, ...]
        }
    }
}
```

## Common Use Cases

**Structure-only comparison:**
```python
result = comparator.compare("file1.csv", "file2.csv", check_structure_only=True)
```

**Content comparison ignoring order:**
```python
result = comparator.compare(
    "file1.csv", "file2.csv",
    ignore_row_order=True,
    check_header_order=False
)
```

**Strict ordered comparison:**
```python
result = comparator.compare(
    "file1.csv", "file2.csv",
    ignore_row_order=False,
    check_header_order=True
)
```
