---
sidebar_label: zip
title: satif_ai.utils.zip
---

#### extract\_zip\_archive\_async

```python
async def extract_zip_archive_async(
        zip_path: Path,
        extract_to: Path,
        ignored_member_prefixes: Tuple[str,
                                       ...] = _IGNORED_ZIP_MEMBER_PREFIXES,
        ignored_filename_prefixes: Tuple[str,
                                         ...] = _IGNORED_ZIP_FILENAME_PREFIXES,
        ignored_filenames: Tuple[str,
                                 ...] = _IGNORED_ZIP_FILENAMES) -> List[Path]
```

> Asynchronously extracts a ZIP archive to a specified directory, filtering out ignored files.
>
> **Arguments**:
>
> - `zip_path` - Path to the ZIP archive.
> - `extract_to` - Directory where the contents will be extracted.
> - `ignored_member_prefixes` - Tuple of member path prefixes to ignore.
> - `ignored_filename_prefixes` - Tuple of filename prefixes to ignore.
> - `ignored_filenames` - Tuple of exact filenames to ignore.
>
>
> **Returns**:
>
>   A list of paths to the successfully extracted files.
>
>
> **Raises**:
>
> - `ValueError` - If the zip_path is invalid or corrupted.
> - `RuntimeError` - If any other error occurs during extraction.
