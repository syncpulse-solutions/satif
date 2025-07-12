---
sidebar_label: xlsx
title: satif_sdk.representers.xlsx
---

## XlsxRepresenter Objects

```python
class XlsxRepresenter(Representer)
```

> Generates representation for XLSX files using pandas.

#### represent

```python
def represent(file_path: Union[str, Path],
              num_rows: int = 10,
              **kwargs: Any) -> str
```

> Generates a string representation of an XLSX file by showing
> the header and the first N data rows for each sheet.
>
> Kwargs Options:
>     engine (str): Pandas engine for reading (default: &#x27;openpyxl&#x27;).
