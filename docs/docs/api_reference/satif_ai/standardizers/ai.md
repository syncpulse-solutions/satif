---
sidebar_label: ai
title: satif_ai.standardizers.ai
---

## AIStandardizer Objects

```python
class AIStandardizer(AsyncStandardizer)
```

> Orchestrates the standardization of various file types using specialized AI standardizers.
> It processes a datasource, which can include individual files or ZIP archives.
> Files are dispatched to appropriate AI agents (e.g., AICSVStandardizer),
> and their SDIF outputs are merged into a single, final SDIF.

#### standardize

```python
async def standardize(datasource: Datasource,
                      output_path: SDIFPath,
                      *,
                      overwrite: bool = False,
                      config: Optional[Dict[str, Any]] = None,
                      **kwargs) -> StandardizationResult
```

> Standardizes datasource to a single SDIF SQLite file.
>
> **Arguments**:
>
> - `datasource` - Source data (file path, list of paths, or directory path).
> - `output_path` - Path to the target output SDIF SQLite file (e.g., &quot;./output/data.sdif&quot;).
> - `overwrite` - If True, overwrite existing output file. Defaults to False.
> - `config` - Optional configuration dictionary for standardizers.
> - `**kwargs` - Additional arguments passed to child standardizers.
>
>
> **Returns**:
>
>   StandardizationResult with the path to the created SDIF SQLite file.
