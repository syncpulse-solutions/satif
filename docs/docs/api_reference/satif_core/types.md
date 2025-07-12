---
sidebar_label: types
title: satif_core.types
---

#### Datasource

> Path(s) to input file(s)

#### OutputData

> Path(s) to output file(s)

## StandardizationResult Objects

```python
@dataclass
class StandardizationResult()
```

> Represents the result of a standardization process.
>
> **Attributes**:
>
> - `output_path` - The path to the generated SDIF file.
> - `file_configs` - An optional dictionary where keys are string representations
>   of input file paths and values are `FileConfig` dictionaries
>   containing the configuration used during the standardization for that file.
>   The order of items will reflect the order of datasources processed.
>   Will be None if no such configuration is returned by the standardizer.

## TransformationResult Objects

```python
@dataclass
class TransformationResult()
```

> Represents the result of a transformation process.
>
> **Attributes**:
>
> - `output_path` - The path to the generated output (file or directory) from the transformation.
> - `function_code` - The source code of the transformation function that was executed.
