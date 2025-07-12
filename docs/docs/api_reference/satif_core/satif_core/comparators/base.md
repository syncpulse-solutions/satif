---
sidebar_label: base
title: satif_core.comparators.base
---

## Comparator Objects

```python
class Comparator(abc.ABC)
```

> Abstract base class for file comparators.
>
> Defines the common interface for comparing two files of a specific type
> and generating a structured report on their equivalence and differences.

#### compare

```python
@abc.abstractmethod
def compare(file_path1: Path,
            file_path2: Path,
            file_config: Optional[dict[str, Any]] = None,
            **kwargs: Any) -> Dict[str, Any]
```

> Compares two files and returns a detailed report.
>
> **Arguments**:
>
> - `file_path1` - Path to the first file.
> - `file_path2` - Path to the second file.
> - `file_config` - Configuration for the files for parsing.
> - `**kwargs` - Comparator-specific options (e.g., ignore_row_order for CSV).
>
>
> **Returns**:
>
>   A dictionary containing the comparison results, typically including:
>   {
> - `"files"` - {&quot;file1&quot;: str, &quot;file2&quot;: str},
> - `"comparison_params"` - {param_name: value, ...}, // Parameters used
> - `"are_equivalent"` - bool, // Overall equivalence based on findings
> - `"summary"` - List[str], // High-level summary of findings
> - `"details"` - { ... } // Detailed breakdown of differences (structure depends on comparator)
>   }
