---
sidebar_label: merge_sdif
title: satif_ai.utils.merge_sdif
---

#### merge\_sdif\_files

```python
def merge_sdif_files(sdif_paths: List[SDIFPath], output_path: Path) -> Path
```

> Merges multiple SDIF files into a single new SDIF file.
>
> **Arguments**:
>
> - `sdif_paths` - A list of paths to the SDIF files to merge.
> - `output_path` - The full path where the merged SDIF file should be saved.
>   Its parent directory will be created if it doesn&#x27;t exist.
>   If output_path is an existing file, it will be overwritten.
>   If output_path is an existing directory, a ValueError is raised.
>
>
> **Returns**:
>
>   Path to the newly created merged SDIF file (same as output_path).
>
>
> **Raises**:
>
> - `ValueError` - If no SDIF files are provided, or output_path is invalid (e.g., an existing directory).
> - `FileNotFoundError` - If a source SDIF file does not exist.
> - `sqlite3.Error` - For database-related errors during merging.
> - `RuntimeError` - For critical errors like inability to generate unique names.
