---
sidebar_label: csv
title: satif_sdk.representers.csv
---

## CSVRepresenter Objects

```python
class CSVRepresenter(Representer)
```

> Generates representation for CSV files.
> Can be initialized with default encoding and delimiter.

#### \_\_init\_\_

```python
def __init__(default_delimiter: Optional[str] = None,
             default_encoding: str = "utf-8",
             default_num_rows: int = 10)
```

> Initialize CSVRepresenter.
>
> **Arguments**:
>
> - `default_delimiter` - Default CSV delimiter. Auto-detected if None.
> - `default_encoding` - Default file encoding.
> - `default_num_rows` - Default number of data rows to represent.

#### represent

```python
def represent(file_path: Union[str, Path],
              num_rows: Optional[int] = None,
              **kwargs: Any) -> Tuple[str, Dict[str, Any]]
```

> Generates a string representation of a CSV file by showing
> the header and the first N data rows.
>
> Kwargs Options:
> encoding (str): File encoding. Overrides instance default.
> delimiter (str): CSV delimiter. Overrides instance default.
>
> **Returns**:
>
>   Tuple[str, Dict[str, Any]]:
>   - The string representation.
>   - A dictionary containing used parameters: &#x27;encoding&#x27; and &#x27;delimiter&#x27;.
