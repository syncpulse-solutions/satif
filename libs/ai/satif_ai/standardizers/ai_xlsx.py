import logging
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from xlsx_to_sdif.graph import graph as xlsx_graph
    from xlsx_to_sdif.state import State as XLSXState
except ImportError:
    xlsx_graph = None  # type: ignore
    XLSXState = None  # type: ignore
    logging.getLogger(__name__).warning(
        "Failed to import xlsx_to_sdif. AIXLSXStandardizer will not be functional."
    )


from satif_core.standardizers.base import AsyncStandardizer
from satif_core.types import Datasource, SDIFPath, StandardizationResult

from satif_ai.utils.merge_sdif import merge_sdif_files

logger = logging.getLogger(__name__)


class AIXLSXStandardizer(AsyncStandardizer):
    """
    An asynchronous standardizer for XLSX files that leverages the `xlsx-to-sdif` library.

    This standardizer processes one or more XLSX files, converts each to an
    intermediate SDIF (Standardized Data Interchange Format) file using the
    `xlsx-to-sdif` processing graph, and then consolidates these intermediate
    files into a single final SDIF file.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        """
        Initializes the AIXLSXStandardizer.

        Args:
            ...
        """

    async def _invoke_xlsx_graph(
        self, input_file_path: Path, graph_config: Dict[str, Any]
    ) -> Path:
        """
        Invokes the `xlsx-to-sdif` graph for a single XLSX file.

        Args:
            input_file_path: Path to the input XLSX file.
            graph_config: Configuration for the `xlsx-to-sdif` graph invocation,
                          including a unique `thread_id`.

        Returns:
            Path to the SDIF file produced by the graph.

        Raises:
            RuntimeError: If the `xlsx-to-sdif` graph is not available, fails to
                          return a final state, or does not produce an output path.
            FileNotFoundError: If the graph reports an output file that doesn't exist.
        """
        if not xlsx_graph or not XLSXState:
            raise RuntimeError(
                "xlsx_to_sdif is not available. "
                "Please ensure 'xlsx-to-sdif' library is installed correctly."
            )

        initial_state: XLSXState = {"spreadsheet_path": str(input_file_path)}  # type: ignore

        thread_id = graph_config.get("configurable", {}).get(
            "thread_id", "unknown_thread"
        )
        logger.info(
            f"Invoking xlsx_to_sdif graph for: {input_file_path.name} with thread_id: {thread_id}"
        )

        # Stream events for logging or potential progress updates
        async for event in xlsx_graph.astream_events(
            initial_state, graph_config, version="v1"
        ):
            event_type = event["event"]
            event_name = event.get("name", "")
            if event_type in ["on_tool_start", "on_chain_start"]:
                logger.debug(
                    f"Graph event for {input_file_path.name} (Thread: {thread_id}): {event_type} - {event_name}"
                )
            elif event_type in ["on_tool_error", "on_chain_error", "on_llm_error"]:
                logger.warning(
                    f"Graph error event for {input_file_path.name} (Thread: {thread_id}): {event_type} - {event_name}. Data: {event.get('data')}"
                )

        final_snapshot = await xlsx_graph.aget_state(graph_config)
        if not final_snapshot or not final_snapshot.values:
            raise RuntimeError(
                f"xlsx_to_sdif graph did not return a final state for {input_file_path.name} (Thread: {thread_id})."
            )

        output_sdif_path_str = final_snapshot.values.get("output_sdif_path")
        if not output_sdif_path_str:
            raise RuntimeError(
                f"xlsx_to_sdif graph for {input_file_path.name} (Thread: {thread_id}) "
                f"did not produce an 'output_sdif_path' in its final state. State: {final_snapshot.values}"
            )

        output_sdif_path = Path(output_sdif_path_str)
        if not output_sdif_path.is_file():
            raise FileNotFoundError(
                f"xlsx_to_sdif graph for {input_file_path.name} (Thread: {thread_id}) "
                f"reported output file '{output_sdif_path}', but it does not exist or is not a file."
            )

        logger.info(
            f"xlsx_to_sdif graph successfully processed {input_file_path.name} (Thread: {thread_id}). Output at {output_sdif_path}"
        )
        return output_sdif_path

    def _resolve_and_filter_input_files(self, datasource: Datasource) -> List[Path]:
        """Resolves and validates datasource, returning a list of XLSX file paths."""
        input_files: List[Path]
        if isinstance(datasource, (str, Path)):
            input_files = [Path(datasource)]
        elif isinstance(datasource, list) and all(
            isinstance(p, (str, Path)) for p in datasource
        ):
            input_files = [Path(p) for p in datasource]
        else:
            raise ValueError(
                "Datasource must be a file path (str or Path) or a list of such paths."
            )

        if not input_files:
            raise ValueError("No input XLSX files provided in the datasource.")

        xlsx_input_files = []
        for f_path in input_files:
            if not f_path.is_file():
                raise FileNotFoundError(f"Input file not found: {f_path}")
            if f_path.suffix.lower() not in (
                ".xlsx",
                ".xlsm",
                ".xlsb",
                ".xls",
            ):  # Common Excel extensions
                logger.warning(
                    f"File {f_path.name} is not a typical XLSX file extension, but will be attempted."
                )
            xlsx_input_files.append(f_path)

        if not xlsx_input_files:
            raise ValueError(
                "No processable XLSX files found in the datasource after filtering."
            )
        return xlsx_input_files

    def _prepare_final_output_path(
        self, output_path: SDIFPath, overwrite: bool
    ) -> Path:
        """Prepares the final output path, handling overwrites and directory creation."""
        final_output_path = Path(output_path)
        if final_output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Output file {final_output_path} already exists and overwrite is False."
            )
        elif final_output_path.exists() and overwrite:
            logger.info(
                f"Overwrite active: Deleting existing output file {final_output_path}"
            )
            try:
                if (
                    final_output_path.is_dir()
                ):  # Should not happen if SDIFPath is file path
                    raise IsADirectoryError(
                        f"Output path {final_output_path} is a directory."
                    )
                final_output_path.unlink()
            except OSError as e:
                raise RuntimeError(
                    f"Failed to delete existing output file {final_output_path}: {e}"
                ) from e

        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        return final_output_path

    def _setup_temp_directories(self) -> Tuple[Path, Path, Path]:
        """Creates and returns paths for temporary working directories."""
        run_temp_dir = Path(tempfile.mkdtemp(prefix="satif_aixlsx_run_"))
        intermediate_sdif_dir = run_temp_dir / "intermediate_sdifs"
        intermediate_sdif_dir.mkdir()
        temp_input_copies_dir = (
            run_temp_dir / "temp_input_copies"
        )  # Directory for temporary input copies
        temp_input_copies_dir.mkdir()
        return run_temp_dir, intermediate_sdif_dir, temp_input_copies_dir

    async def _process_single_file_to_intermediate_sdif(
        self,
        input_xlsx_file: Path,
        final_output_path_stem: str,
        temp_input_copies_dir: Path,
        intermediate_sdif_dir: Path,
    ) -> Path:
        """Processes a single XLSX file to an intermediate SDIF in a controlled location."""
        logger.info(f"Processing file: {input_xlsx_file.name}")
        graph_thread_id = f"satif_aixlsx_{final_output_path_stem}_{input_xlsx_file.stem}_{uuid.uuid4().hex[:8]}"

        temp_input_file_for_graph = (
            temp_input_copies_dir
            / f"{input_xlsx_file.stem}_{graph_thread_id}{input_xlsx_file.suffix}"
        )
        shutil.copy2(input_xlsx_file, temp_input_file_for_graph)
        logger.debug(
            f"Created temporary copy of {input_xlsx_file.name} at {temp_input_file_for_graph}"
        )

        graph_config_for_file = {
            "configurable": {"thread_id": graph_thread_id},
            "recursion_limit": 50,  # Default, make configurable if needed
        }

        try:
            graph_produced_sdif_path = await self._invoke_xlsx_graph(
                temp_input_file_for_graph, graph_config_for_file
            )

            target_intermediate_sdif_path = (
                intermediate_sdif_dir
                / f"intermediate_{input_xlsx_file.stem}_{graph_thread_id}.sdif"
            )
            shutil.move(
                str(graph_produced_sdif_path),
                str(target_intermediate_sdif_path),
            )
            logger.info(
                f"Moved graph output for {input_xlsx_file.name} to {target_intermediate_sdif_path}"
            )
            return target_intermediate_sdif_path
        except Exception as e:
            error_msg = f"Failed to process file {input_xlsx_file.name} (using copy {temp_input_file_for_graph.name}) with xlsx-to-sdif graph: {e}"
            logger.error(error_msg, exc_info=True)
            # Re-raise to be caught by the main standardize method's loop or error handling
            raise RuntimeError(
                f"Error processing {input_xlsx_file.name}. Halting batch."
            ) from e

    def _consolidate_intermediate_sdifs(
        self, intermediate_sdif_paths: List[Path], final_output_path: Path
    ) -> None:
        """Consolidates intermediate SDIF files into the final output path."""
        if not intermediate_sdif_paths:
            # This case should ideally be handled before calling, but as a safeguard:
            raise RuntimeError(
                "No intermediate SDIF files were provided for consolidation."
            )

        if len(intermediate_sdif_paths) == 1:
            logger.info(
                f"Only one intermediate SDIF generated. Moving {intermediate_sdif_paths[0]} to {final_output_path}"
            )
            shutil.move(str(intermediate_sdif_paths[0]), str(final_output_path))
        else:
            logger.info(
                f"Merging {len(intermediate_sdif_paths)} intermediate SDIF files into {final_output_path}"
            )
            merge_sdif_files(
                source_db_paths=intermediate_sdif_paths,
                target_db_path=final_output_path,
            )

    async def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
        config: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> StandardizationResult:
        """
        Standardizes one or more XLSX files into a single SDIF file.

        Args:
            datasource: A single file path (str or Path) or a list of file paths
                        to XLSX files.
            output_path: The path where the final consolidated SDIF file will be saved.
            overwrite: If True, overwrite the output_path if it already exists.
                       Defaults to False.
            config: General configuration options (currently not used by this standardizer
                    for graph interaction but preserved for API consistency).
            **kwargs: Additional keyword arguments (currently ignored).

        Returns:
            A StandardizationResult object containing the path to the final SDIF file.

        Raises:
            ValueError: If the datasource is invalid or no XLSX files are found.
            RuntimeError: If critical errors occur during processing, such as the
                          `xlsx-to-sdif` graph not being available or failing.
            FileNotFoundError: If input files are not found or graph outputs are invalid.
            FileExistsError: If output_path exists and overwrite is False.
        """
        if not xlsx_graph or not XLSXState:
            raise RuntimeError(
                "AIXLSXStandardizer cannot operate because `xlsx_to_sdif.graph` or `xlsx_to_sdif.state` is not available. "
                "Please ensure the 'xlsx-to-sdif' library is installed and accessible."
            )

        xlsx_input_files = self._resolve_and_filter_input_files(datasource)
        final_output_path = self._prepare_final_output_path(output_path, overwrite)
        run_temp_dir, intermediate_sdif_dir, temp_input_copies_dir = (
            self._setup_temp_directories()
        )

        intermediate_sdif_paths: List[Path] = []
        processing_errors: List[str] = []

        try:
            # Process each file sequentially. Consider asyncio.gather for parallel if graph supports it well for many files.
            for i, input_xlsx_file in enumerate(xlsx_input_files):
                try:
                    logger.info(
                        f"Starting processing for file {i + 1}/{len(xlsx_input_files)}: {input_xlsx_file.name}"
                    )
                    intermediate_sdif_path = (
                        await self._process_single_file_to_intermediate_sdif(
                            input_xlsx_file,
                            final_output_path.stem,  # Pass stem for unique naming
                            temp_input_copies_dir,
                            intermediate_sdif_dir,
                        )
                    )
                    intermediate_sdif_paths.append(intermediate_sdif_path)
                except Exception:
                    logger.error(
                        f"Halting standardization due to error processing {input_xlsx_file.name}."
                    )
                    raise  # Re-raise the exception to be caught by the outer try/finally

            if not intermediate_sdif_paths:
                # This condition might be redundant if _process_single_file_to_intermediate_sdif always raises on failure
                # and we re-raise immediately.
                if processing_errors:  # This list would be empty if we fail fast
                    raise RuntimeError(
                        f"No XLSX files were successfully processed. Errors: {'; '.join(processing_errors)}"
                    )
                else:
                    raise RuntimeError(
                        "No intermediate SDIF files were generated, though no specific errors were caught."
                    )

            self._consolidate_intermediate_sdifs(
                intermediate_sdif_paths, final_output_path
            )

            logger.info(f"Successfully created final SDIF: {final_output_path}")
            return StandardizationResult(
                output_path=final_output_path, file_configs=None
            )  # file_configs not available from this process

        finally:
            if run_temp_dir.exists():
                try:
                    shutil.rmtree(run_temp_dir)
                    logger.debug(f"Cleaned up temporary directory: {run_temp_dir}")
                except Exception as e_clean:
                    logger.error(
                        f"Error cleaning up temporary directory {run_temp_dir}: {e_clean}",
                        exc_info=True,
                    )
