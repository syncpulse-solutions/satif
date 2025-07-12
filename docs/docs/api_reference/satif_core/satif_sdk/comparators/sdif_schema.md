---
sidebar_label: sdif_schema
title: satif_sdk.comparators.sdif_schema
---

## SDIFSchemaComparator Objects

```python
class SDIFSchemaComparator()
```

> Compares two SDIF structural schemas based on a flexible configuration.
> Provides methods for checking equivalence and compatibility (subset relationship).
> The input schemas are expected to be the direct output of SDIFDatabase.get_schema().

#### \_\_init\_\_

```python
def __init__(config: Optional[SDIFSchemaConfig] = None)
```

> Initializes the comparator with a specific configuration.
>
> **Arguments**:
>
> - `config` - An SDIFSchemaConfig instance. If None, a default config is used.

#### compare

```python
def compare(
    schema1: Dict[str, Any],
    schema2: Dict[str, Any],
    verbose_diff_level: int = 0
) -> Tuple[bool, Union[List[str], Dict[str, Any]]]
```

> Compares two structural SDIF schemas.
>
> **Arguments**:
>
> - `schema1` - The first structural schema (output of SDIFDatabase.get_schema()).
> - `schema2` - The second structural schema.
> - `verbose_diff_level` - Controls verbosity of the difference report.
> - `0` - Returns a summarized list of human-readable differences.
> - `1` - Returns the DeepDiff object as a dictionary.
> - `2` _or more_ - Returns the full DeepDiff object (can be large).
>
>
> **Returns**:
>
>   A tuple: (are_equivalent: bool, differences: Union[List[str], Dict[str, Any]]).
>   &#x27;differences&#x27; depends on verbose_diff_level.

#### is\_compatible\_with

```python
def is_compatible_with(consumer_schema: Dict[str, Any],
                       producer_schema: Dict[str, Any]) -> bool
```

> Checks if the producer_schema is structurally compatible with the consumer_schema,
> based on the requirements defined in the comparator&#x27;s configuration (self.config).
>
> Compatibility means the producer_schema provides at least all the structural
> elements and guarantees required by the consumer_schema according to the config.
> The producer_schema can have additional elements not required by the consumer.
>
> **Arguments**:
>
> - `consumer_schema` - The schema defining the requirements (consumer&#x27;s view).
> - `producer_schema` - The schema being checked for compliance (producer&#x27;s actual schema).
>
>
> **Returns**:
>
>   True if producer_schema is compatible with consumer_schema&#x27;s requirements,
>   False otherwise.
