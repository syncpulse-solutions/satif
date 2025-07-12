---
sidebar_label: comparators
title: satif_sdk.comparators
---

#### compare\_output\_files

```python
def compare_output_files(generated_output_files: List[Path],
                         target_output_files: List[Path],
                         file_configs: Optional[List[dict[str, Any]]] = None,
                         **kwargs: Any) -> dict[str, Any]
```

> Compare generated output files with target output files.
>
> **Arguments**:
>
> - `generated_output_files` - List of paths to generated output files
> - `target_output_files` - List of paths to target output files
>
>
> **Returns**:
>
>   Dictionary containing comparison results and success status
