---
sidebar_label: representers
title: satif_sdk.representers
---

#### get\_representer

```python
def get_representer(file_path: Union[str, Path]) -> Optional[Representer]
```

> Factory function to get the appropriate file representer based on extension.
>
> **Arguments**:
>
> - `file_path` - Path to the file.
>
>
> **Returns**:
>
>   An instance of a BaseRepresenter subclass, or None if the file type
>   is unsupported or the file doesn&#x27;t exist.
