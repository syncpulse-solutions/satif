---
sidebar_label: standardize
title: satif_ai.standardize
---

#### astandardize

```python
async def astandardize(
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
        sdif_schema: Optional[Union[FilePath, Dict[str, Any]]] = None,
        tidy_adapter: Union[bool, TidyAdapter] = False,
        config: Optional[Dict[str, Any]] = None,
        standardizer: Optional[AsyncStandardizer] = None,
        mcp_server: Optional[Any] = None,
        mcp_session: Optional[Any] = None,
        llm_model: Optional[str] = None) -> StandardizationResult
```

> Asynchronously standardizes a datasource into a single, canonical SDIF SQLite file.
>
> This function serves as the primary entry point for the SATIF standardization layer.
> It orchestrates the conversion of various input file formats (e.g., CSV, Excel, PDF)
> from the provided datasource into a unified SDIF (Standard Data Interchange Format)
> SQLite file. The process may involve AI-driven parsing, schema adaptation, and
> data tidying, depending on the configuration and the capabilities of the
> underlying standardizer.
>
> **Arguments**:
>
> - `datasource` - The source of the data to be standardized. This can be a
>   single file path (str or Path), a list of file paths, or other
>   datasource types supported by the chosen standardizer.
> - `output_path` - The path (str or Path) where the output SDIF SQLite database file
>   will be created (e.g., &quot;./output/my_data.sdif&quot;).
> - `overwrite` - If True, an existing SDIF file at `output_path` will be
>   overwritten. Defaults to False.
> - `sdif_schema` - Optional. Path to an SDIF schema definition file (e.g., a JSON file)
>   or a dictionary representing the schema. If provided, the
>   standardization process (specifically if using the default
>   `AIStandardizer`) may attempt to adapt the data to this
>   target schema.
> - `tidy_adapter` - Optional. If True, a default `TidyAdapter` may be used.
>   Alternatively, a specific `TidyAdapter` instance can be provided
>   to perform data tidying processes (e.g., cleaning, normalization,
>   restructuring tables). If False (default), no explicit tidying
>   step is initiated by this top-level function, though underlying
>   standardizers might perform their own internal tidying.
>   The specifics depend on the standardizer&#x27;s capabilities.
> - `config` - Optional. A dictionary for advanced or standardizer-specific
>   configurations. This config is passed directly to the
>   `output_path`0 method of the chosen standardizer.
> - `output_path`1 - Optional. An instance of an `output_path`2 subclass.
>   If provided, this instance will be used for standardization.
>   This allows for using pre-configured or custom standardizers.
>   If None, a default `AIStandardizer` is instantiated using
>   `output_path`4, `output_path`5, `output_path`6, `sdif_schema`,
>   and `tidy_adapter`.
> - `output_path`4 - Optional. The MCP (Model Coordination Platform) server instance.
>   Used if `output_path`1 is None for the default `AIStandardizer`.
> - `output_path`5 - Optional. The MCP session or transport object.
>   Used if `output_path`1 is None for the default `AIStandardizer`.
> - `output_path`6 - Optional. The language model to be used by the default `AIStandardizer`
>   if no `output_path`1 instance is provided (e.g., &quot;gpt-4o&quot;).
>   Each standardizer may have its own default model.
>
>
> **Returns**:
>
>   A `overwrite`8 object containing:
>   - `output_path`: The absolute `output_path`0 to the created or updated SDIF database file.
>   - `output_path`1: An optional dictionary detailing configurations used for
>   each processed input file, if applicable and returned by
>   the standardizer.
>
>
> **Raises**:
>
> - `output_path`2 - If the `datasource` (or parts of it) does not exist.
> - `output_path`4 - If `output_path` exists and `overwrite` is False.
> - `output_path`7 - If input arguments are invalid (e.g., unsupported datasource type).
> - `output_path`8 - For general errors during the standardization process.
>   Specific exceptions may also be raised by the underlying
>   standardizer implementation.
