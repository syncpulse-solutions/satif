from pathlib import Path
from typing import Any, Dict, Optional, Union

from satif_core.standardizers.base import AsyncStandardizer
from satif_core.types import Datasource, FilePath, SDIFPath, StandardizationResult

from satif_ai.adapters.tidy import TidyAdapter
from satif_ai.standardizers.ai import AIStandardizer


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
    llm_model: Optional[str] = None,
) -> StandardizationResult:
    """
    Asynchronously standardizes a datasource into a single, canonical SDIF SQLite file.

    This function serves as the primary entry point for the SATIF standardization layer.
    It orchestrates the conversion of various input file formats (e.g., CSV, Excel, PDF)
    from the provided datasource into a unified SDIF (Standard Data Interchange Format)
    SQLite file. The process may involve AI-driven parsing, schema adaptation, and
    data tidying, depending on the configuration and the capabilities of the
    underlying standardizer.

    Args:
        datasource: The source of the data to be standardized. This can be a
                    single file path (str or Path), a list of file paths, or other
                    datasource types supported by the chosen standardizer.
        output_path: The path (str or Path) where the output SDIF SQLite database file
                     will be created (e.g., "./output/my_data.sdif").
        overwrite: If True, an existing SDIF file at `output_path` will be
                   overwritten. Defaults to False.
        sdif_schema: Optional. Path to an SDIF schema definition file (e.g., a JSON file)
                     or a dictionary representing the schema. If provided, the
                     standardization process (specifically if using the default
                     `AIStandardizer`) may attempt to adapt the data to this
                     target schema.
        tidy_adapter: Optional. If True, a default `TidyAdapter` may be used.
                      Alternatively, a specific `TidyAdapter` instance can be provided
                      to perform data tidying processes (e.g., cleaning, normalization,
                      restructuring tables). If False (default), no explicit tidying
                      step is initiated by this top-level function, though underlying
                      standardizers might perform their own internal tidying.
                      The specifics depend on the standardizer's capabilities.
        config: Optional. A dictionary for advanced or standardizer-specific
                configurations. This config is passed directly to the
                `standardize` method of the chosen standardizer.
        standardizer: Optional. An instance of an `AsyncStandardizer` subclass.
                      If provided, this instance will be used for standardization.
                      This allows for using pre-configured or custom standardizers.
                      If None, a default `AIStandardizer` is instantiated using
                      `mcp_server`, `mcp_session`, `llm_model`, `sdif_schema`,
                      and `tidy_adapter`.
        mcp_server: Optional. The MCP (Model Coordination Platform) server instance.
                    Used if `standardizer` is None for the default `AIStandardizer`.
        mcp_session: Optional. The MCP session or transport object.
                     Used if `standardizer` is None for the default `AIStandardizer`.
        llm_model: Optional. The language model to be used by the default `AIStandardizer`
                   if no `standardizer` instance is provided (e.g., "gpt-4o").
                   Each standardizer may have its own default model.

    Returns:
        A `StandardizationResult` object containing:
        - `output_path`: The absolute `Path` to the created or updated SDIF database file.
        - `file_configs`: An optional dictionary detailing configurations used for
                          each processed input file, if applicable and returned by
                          the standardizer.

    Raises:
        FileNotFoundError: If the `datasource` (or parts of it) does not exist.
        FileExistsError: If `output_path` exists and `overwrite` is False.
        ValueError: If input arguments are invalid (e.g., unsupported datasource type).
        RuntimeError: For general errors during the standardization process.
                      Specific exceptions may also be raised by the underlying
                      standardizer implementation.
    """
    if standardizer is None:
        standardizer = AIStandardizer(
            mcp_server=mcp_server,
            mcp_session=mcp_session,
            llm_model=llm_model,
            sdif_schema=sdif_schema,
            tidy_adapter=tidy_adapter
            if isinstance(tidy_adapter, TidyAdapter)
            else (TidyAdapter() if tidy_adapter else None),
        )

    result = await standardizer.standardize(
        datasource=datasource,
        output_path=output_path,
        overwrite=overwrite,
        config=config,
    )

    output_sdif_path = (
        Path(result.output_path)
        if isinstance(result.output_path, str)
        else result.output_path
    )

    return StandardizationResult(
        output_path=output_sdif_path, file_configs=result.file_configs
    )
