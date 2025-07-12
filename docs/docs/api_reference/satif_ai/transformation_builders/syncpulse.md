---
sidebar_label: syncpulse
title: satif_ai.transformation_builders.syncpulse
---

#### execute\_transformation

```python
@function_tool
async def execute_transformation(code: str) -> str
```

> Executes the transformation code on the input and returns the
> comparison difference between the transformed output and the target output example.
>
> **Arguments**:
>
> - `code` - The code to execute on the input.

## SyncpulseTransformationBuilder Objects

```python
class SyncpulseTransformationBuilder(AsyncTransformationBuilder)
```

> This class is used to build a transformation code that will be used to transform a SDIF database into a set of files following the format of the given output files.
