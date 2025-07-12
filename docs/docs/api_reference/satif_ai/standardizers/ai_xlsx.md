---
sidebar_label: ai_xlsx
title: satif_ai.standardizers.ai_xlsx
---

## AIXLSXStandardizer Objects

```python
class AIXLSXStandardizer(AsyncStandardizer)
```

> An asynchronous standardizer for XLSX files that leverages the `xlsx-to-sdif` library.
>
> This standardizer processes one or more XLSX files, converts each to an
> intermediate SDIF (Standardized Data Interchange Format) file using the
> `xlsx-to-sdif` processing graph, and then consolidates these intermediate
> files into a single final SDIF file.

#### \_\_init\_\_

```python
def __init__(*args: Any, **kwargs: Any)
```

> Initializes the AIXLSXStandardizer.
>
> **Arguments**:
>
>   ...

#### standardize

```python
async def standardize(datasource: Datasource,
                      output_path: SDIFPath,
                      *,
                      overwrite: bool = False,
                      config: Optional[Dict[str, Any]] = None,
                      **kwargs: Any) -> StandardizationResult
```

> Standardizes one or more XLSX files into a single SDIF file.
>
> **Arguments**:
>
> - `datasource` - A single file path (str or Path) or a list of file paths
>   to XLSX files.
> - `output_path` - The path where the final consolidated SDIF file will be saved.
> - `overwrite` - If True, overwrite the output_path if it already exists.
>   Defaults to False.
> - `config` - General configuration options (currently not used by this standardizer
>   for graph interaction but preserved for API consistency).
> - `**kwargs` - Additional keyword arguments (currently ignored).
>
>
> **Returns**:
>
>   A StandardizationResult object containing the path to the final SDIF file.
>
>
> **Raises**:
>
> - `ValueError` - If the datasource is invalid or no XLSX files are found.
> - `RuntimeError` - If critical errors occur during processing, such as the
>   `xlsx-to-sdif` graph not being available or failing.
> - `FileNotFoundError` - If input files are not found or graph outputs are invalid.
> - `FileExistsError` - If output_path exists and overwrite is False.
