---
sidebar_label: base
title: satif_core.transformation_builders.base
---

## TransformationBuilder Objects

```python
class TransformationBuilder(ABC)
```

> Abstract Base Class for synchronous transformation code builders.
>
> Transformation builders are responsible for generating the code (as a string)
> that defines the logic for transforming data, typically from a standardized
> intermediate format (like SDIF) to the desired output format(s).
>
> Concrete implementations define how this transformation code is constructed,
> often leveraging AI based on various inputs provided via `**kwargs` to the `build` method.

#### build

```python
@abstractmethod
def build(**kwargs) -> str
```

> Generates the transformation code as a string.
>
> **Arguments**:
>
> - `**kwargs` - Arbitrary keyword arguments specific to the concrete
>   builder implementation. These might include input data
>   examples, output specifications, user instructions, etc.
>
>
> **Returns**:
>
> - `str` - The generated transformation code as a string.

## AsyncTransformationBuilder Objects

```python
class AsyncTransformationBuilder(ABC)
```

> Abstract Base Class for asynchronous transformation code builders.
>
> Similar to `TransformationBuilder`, but designed for builders that require
> asynchronous operations (e.g., I/O-bound tasks like calling external APIs
> or services) during the code generation process.
>
> Concrete implementations define how transformation code is constructed
> asynchronously, based on inputs provided via `**kwargs` to the `build` method.

#### build

```python
@abstractmethod
async def build(**kwargs) -> str
```

> Asynchronously generates the transformation code as a string.
>
> **Arguments**:
>
> - `**kwargs` - Arbitrary keyword arguments specific to the concrete
>   builder implementation. These might include input data
>   examples, output specifications, user instructions, etc.
>
>
> **Returns**:
>
> - `str` - The generated transformation code as a string.
