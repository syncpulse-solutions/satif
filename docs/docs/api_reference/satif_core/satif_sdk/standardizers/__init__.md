---
sidebar_label: standardizers
title: satif_sdk.standardizers
---

#### get\_standardizer

```python
def get_standardizer(datasource: Datasource) -> Optional[Type[Standardizer]]
```

> Selects the appropriate standardizer based on the datasource file type(s).
>
> **Arguments**:
>
> - `datasource` - A single file path or a list of file paths.
>
>
> **Returns**:
>
>   The Standardizer class type if a suitable one is found, otherwise None.
