---
sidebar_label: adapters
title: satif_sdk.adapters
---

#### load\_adapter

```python
def load_adapter(name: str, **kwargs) -> object
```

> Loads and instantiates an adapter by its registered name.
>
> **Arguments**:
>
> - `name` - The unique name the adapter was registered with (e.g., &#x27;tidy_ai&#x27;).
> - `**kwargs` - Keyword arguments to pass to the adapter&#x27;s constructor.
>
>
> **Returns**:
>
>   An instance of the requested adapter.
>
>
> **Raises**:
>
> - `KeyError` - If no adapter with the given name is found.
> - `Exception` - If the adapter class fails to instantiate.

#### get\_available\_adapters

```python
def get_available_adapters() -> list[str]
```

> Returns a list of names of discovered adapters.
