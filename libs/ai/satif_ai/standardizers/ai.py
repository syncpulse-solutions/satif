import asyncio
import logging
import shutil
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from satif_core.standardizers.base import AsyncStandardizer
from satif_core.types import Datasource, FilePath, SDIFPath, StandardizationResult

from satif_ai.adapters.tidy import TidyAdapter
from satif_ai.standardizers.ai_xlsx import AIXLSXStandardizer
from satif_ai.utils.merge_sdif import merge_sdif_files
from satif_ai.utils.zip import extract_zip_archive_async

from .ai_csv import AICSVStandardizer

logger = logging.getLogger(__name__)


class AIStandardizer(AsyncStandardizer):
    """
    Orchestrates the standardization of various file types using specialized AI standardizers.
    It processes a datasource, which can include individual files or ZIP archives.
    Files are dispatched to appropriate AI agents (e.g., AICSVStandardizer),
    and their SDIF outputs are merged into a single, final SDIF.
    """

    def __init__(
        self,
        mcp_server: Optional[Any] = None,
        mcp_session: Optional[Any] = None,
        llm_model: Optional[str] = None,
        sdif_schema: Optional[Union[FilePath, Dict[str, Any]]] = None,
        tidy_adapter: Optional[TidyAdapter] = None,
    ):
        self.mcp_server = mcp_server
        self.mcp_session = mcp_session
        self.llm_model = llm_model
        self.sdif_schema = sdif_schema  # TODO: Implement schema adaptation logic
        self.tidy_adapter = tidy_adapter  # TODO: Implement tidying logic

        self.ai_standardizer_map: Dict[str, Type[AsyncStandardizer]] = {
            ".csv": AICSVStandardizer,
            ".xlsx": AIXLSXStandardizer,
            ".xls": AIXLSXStandardizer,
            ".xlsm": AIXLSXStandardizer,
            # ".pdf": AIPDFStandardizer,
            # ".json": AIJSONStandardizer,
            # ".xml": AIXMLStandardizer,
        }
        for ext, standardizer_class in self.ai_standardizer_map.items():
            if not issubclass(standardizer_class, AsyncStandardizer):
                raise TypeError(
                    f"Standardizer for '{ext}' ({standardizer_class.__name__}) "
                    "must inherit from AsyncStandardizer."
                )

    def _get_ai_standardizer_class(
        self, extension: str
    ) -> Optional[Type[AsyncStandardizer]]:
        return self.ai_standardizer_map.get(extension.lower())

    def _resolve_file_path(
        self, raw_path_item: Union[str, Path], temp_processing_dir: Path
    ) -> List[Path]:
        """
        Resolves a single input path to a list of file paths.
        This method contains blocking file system operations.
        """
        raw_path = Path(raw_path_item).resolve()
        input_file_paths: List[Path] = []

        if not raw_path.exists():
            raise FileNotFoundError(f"Input path not found: {raw_path}")

        if raw_path.is_file():
            if raw_path.suffix.lower() == ".zip":
                # Zip extraction is handled asynchronously in the calling method
                return [raw_path]
            else:
                input_file_paths.append(raw_path)
        elif raw_path.is_dir():
            logger.info(f"Processing directory datasource: {raw_path}")
            for child_item in raw_path.iterdir():
                if child_item.is_file():
                    input_file_paths.append(child_item)
                # Deeper recursion to be implemented.
        else:
            logger.warning(
                f"Input path '{raw_path}' is not a file or directory and will be ignored."
            )

        return input_file_paths

    async def _resolve_input_files(
        self, datasource: Datasource, temp_processing_dir: Path
    ) -> List[Path]:
        """
        Resolves the input datasource to a list of individual file paths.
        Handles single files, lists of files, and extracts ZIP archives.
        """
        raw_paths_to_check: List[Union[str, Path]] = []
        all_input_file_paths: List[Path] = []

        if isinstance(datasource, (str, Path)):
            raw_paths_to_check = [datasource]
        elif isinstance(datasource, list) and all(
            isinstance(p, (str, Path)) for p in datasource
        ):
            raw_paths_to_check = datasource
        else:
            # This also catches the case where datasource is an empty list initially
            raise ValueError(
                "Datasource must be a non-empty file path (string or Path) or a non-empty list of such paths."
            )

        if not raw_paths_to_check:  # Should be caught by above, but defensive
            raise ValueError("No input datasource paths provided.")

        # Process each path item in a thread to avoid blocking the event loop
        for raw_path_item in raw_paths_to_check:
            resolved_paths = await asyncio.to_thread(
                self._resolve_file_path, raw_path_item, temp_processing_dir
            )

            for raw_path in resolved_paths:
                if raw_path.suffix.lower() == ".zip":
                    zip_extract_target = (
                        temp_processing_dir
                        / f"extracted_{raw_path.stem}_{uuid.uuid4().hex[:8]}"
                    )
                    try:
                        extracted_from_zip = await extract_zip_archive_async(
                            raw_path, zip_extract_target
                        )
                        all_input_file_paths.extend(extracted_from_zip)
                    except Exception as e_zip:
                        logger.error(
                            f"Failed to extract ZIP archive '{raw_path}': {e_zip}",
                            exc_info=True,
                        )
                        # Decide if one failed zip should stop all, or just be skipped.
                        # For now, skipping problematic zips.
                        continue
                else:
                    all_input_file_paths.append(raw_path)

        if not all_input_file_paths:
            # This means all inputs were invalid, unresolvable, or zips failed etc.
            logger.error("No processable files found after resolving datasource.")
            raise ValueError("Datasource resolution resulted in no processable files.")

        return all_input_file_paths

    def _group_files_by_standardizer(
        self, file_paths: List[Path]
    ) -> Tuple[Dict[Type[AsyncStandardizer], List[Path]], List[Path]]:
        """Groups files by the AI standardizer responsible for them based on extension."""
        grouped: Dict[Type[AsyncStandardizer], List[Path]] = defaultdict(list)
        unsupported_files: List[Path] = []
        for file_path in file_paths:
            standardizer_class = self._get_ai_standardizer_class(file_path.suffix)
            if standardizer_class:
                grouped[standardizer_class].append(file_path)
            else:
                unsupported_files.append(file_path)
        if unsupported_files:
            logger.warning(
                f"Unsupported files found and will be ignored: "
                f"{[str(f.name) for f in unsupported_files]}"
            )
        return grouped, unsupported_files

    async def _process_file_groups(
        self,
        grouped_files: Dict[Type[AsyncStandardizer], List[Path]],
        temp_sdif_dir: Path,
        config: Optional[Dict[str, Any]],
        **kwargs,
    ) -> Tuple[List[Path], List[Dict[str, Any]]]:
        """
        Processes groups of files using their respective AI standardizers.
        Child standardizers are expected to produce a single SDIF SQLite file.

        Returns:
            A tuple containing:
            - List of Paths to successfully created intermediate SDIF SQLite files.
            - List of aggregated file configurations from child standardizers.
        """
        processing_tasks = []
        standardizer_instances_info = []

        for standardizer_class, files_in_group in grouped_files.items():
            if not files_in_group:
                continue

            standardizer_init_kwargs = {}
            # TODO: Pass standardizer-specific config from main 'config' if available for this standardizer_class

            try:
                ai_child_standardizer = standardizer_class(
                    mcp_server=self.mcp_server,
                    mcp_session=self.mcp_session,
                    llm_model=self.llm_model,
                    **standardizer_init_kwargs,
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialize standardizer {standardizer_class.__name__} for '{files_in_group[0].name}': {e}",
                    exc_info=True,
                )
                raise RuntimeError(
                    f"Initialization failed for {standardizer_class.__name__}: {e}"
                )

            # Generate a unique filename for the intermediate SDIF SQLite file
            intermediate_sdif_filename = f"intermediate_{standardizer_class.__name__}_{uuid.uuid4().hex[:12]}.sdif"
            intermediate_sdif_file_path = temp_sdif_dir / intermediate_sdif_filename

            logger.info(
                f"Queueing standardization for {len(files_in_group)} file(s) "
                f"with {standardizer_class.__name__} (output file: {intermediate_sdif_file_path})"
            )

            task = ai_child_standardizer.standardize(
                datasource=files_in_group,
                output_path=intermediate_sdif_file_path,
                overwrite=True,  # Temporary intermediate files are always new/overwritten
                config=config,
                **kwargs,
            )
            processing_tasks.append(task)
            standardizer_instances_info.append(
                {
                    "class_name": standardizer_class.__name__,
                    "output_file": intermediate_sdif_file_path,
                }
            )

        gathered_outputs = await asyncio.gather(
            *processing_tasks, return_exceptions=True
        )

        successful_intermediate_sdif_files: List[Path] = []
        aggregated_file_configs: List[Dict[str, Any]] = []

        for i, result_or_exc in enumerate(gathered_outputs):
            info = standardizer_instances_info[i]
            expected_output_file: Path = info["output_file"]

            if isinstance(result_or_exc, StandardizationResult):
                # Child standardizer's output_path should be a file path.
                child_reported_output_file = Path(result_or_exc.output_path)

                if not child_reported_output_file.is_file():
                    logger.error(
                        f"Standardizer {info['class_name']} reported success, but its output path "
                        f"'{child_reported_output_file}' is not a file or does not exist. Skipping."
                    )
                    continue  # Skip this problematic result

                if (
                    child_reported_output_file.resolve()
                    != expected_output_file.resolve()
                ):
                    logger.warning(
                        f"Standardizer {info['class_name']} reported output file '{child_reported_output_file}' "
                        f"which differs from expected '{expected_output_file}'. Using reported path."
                    )

                logger.info(
                    f"Successfully standardized group with {info['class_name']}. "
                    f"Intermediate SDIF file: {child_reported_output_file}"
                )
                successful_intermediate_sdif_files.append(child_reported_output_file)
                if result_or_exc.file_configs:
                    aggregated_file_configs.extend(result_or_exc.file_configs)

            elif isinstance(result_or_exc, Exception):
                logger.error(
                    f"Standardization by {info['class_name']} for target '{expected_output_file}' failed: {result_or_exc}",
                    exc_info=result_or_exc,
                )
                # Optionally, try to clean up the expected_output_file if it was created before erroring
                if expected_output_file.exists():
                    try:
                        expected_output_file.unlink()
                    except OSError:
                        pass

        return successful_intermediate_sdif_files, aggregated_file_configs

    def _consolidate_results(
        self,
        intermediate_sdif_files: List[Path],
        aggregated_file_configs: Optional[List[Dict[str, Any]]],
        final_sdif_file_target: Path,
        overwrite: bool,
    ) -> StandardizationResult:
        """
        Merges or moves intermediate SDIF SQLite files to the final target SDIF SQLite file.
        Cleans up intermediate files.
        """
        if not intermediate_sdif_files:
            raise RuntimeError(
                "No intermediate SDIF files were successfully generated to consolidate."
            )

        final_sdif_file_target.parent.mkdir(parents=True, exist_ok=True)

        if final_sdif_file_target.exists():
            if not overwrite:
                raise FileExistsError(
                    f"Final output file {final_sdif_file_target} already exists and overwrite is False."
                )
            logger.info(
                f"Overwriting existing final output file: {final_sdif_file_target}"
            )
            try:
                final_sdif_file_target.unlink()
            except OSError as e_unlink:
                logger.error(
                    f"Could not delete existing file {final_sdif_file_target}: {e_unlink}"
                )
                raise  # Re-raise as this is critical for overwrite

        final_sdif_path_str: str
        if len(intermediate_sdif_files) == 1:
            source_sqlite_file = intermediate_sdif_files[0]
            logger.info(
                f"Moving single intermediate SDIF SQLite file '{source_sqlite_file}' to final output '{final_sdif_file_target}'."
            )
            try:
                shutil.move(str(source_sqlite_file), str(final_sdif_file_target))
                final_sdif_path_str = str(final_sdif_file_target)
            except Exception as e_move:
                logger.error(
                    f"Failed to move {source_sqlite_file} to {final_sdif_file_target}: {e_move}"
                )
                # Attempt to copy as a fallback, then try to remove source
                try:
                    shutil.copy2(str(source_sqlite_file), str(final_sdif_file_target))
                    final_sdif_path_str = str(final_sdif_file_target)
                    source_sqlite_file.unlink(
                        missing_ok=True
                    )  # Try to clean up source after copy
                except Exception as e_copy_fallback:
                    logger.error(
                        f"Fallback copy also failed for {source_sqlite_file}: {e_copy_fallback}"
                    )
                    raise RuntimeError(
                        f"Could not place intermediate file into final location: {e_copy_fallback}"
                    ) from e_copy_fallback
        else:
            logger.info(
                f"Merging {len(intermediate_sdif_files)} intermediate SDIF SQLite files into '{final_sdif_file_target}'."
            )
            merged_target_path = merge_sdif_files(
                intermediate_sdif_files,
                final_sdif_file_target,
            )
            final_sdif_path_str = str(merged_target_path)

        # Clean up original intermediate files (they have been moved or their content merged)
        for temp_file in intermediate_sdif_files:
            if (
                temp_file.exists()
                and temp_file.resolve() != Path(final_sdif_path_str).resolve()
            ):  # Don't delete the final file if it was one of the intermediates (single file case)
                try:
                    temp_file.unlink()
                    logger.debug(f"Cleaned up intermediate file: {temp_file}")
                except Exception as e_clean_file:
                    logger.warning(
                        f"Error cleaning up intermediate file {temp_file}: {e_clean_file}"
                    )

        logger.info(
            f"Consolidation complete. Final SDIF SQLite file: {final_sdif_path_str}"
        )
        return StandardizationResult(
            output_path=Path(final_sdif_path_str),
            file_configs=aggregated_file_configs if aggregated_file_configs else None,
        )

    def _setup_workspace(
        self, output_path: Path, overwrite: bool
    ) -> Tuple[Path, Path, Path]:
        """
        Sets up the temporary workspace directories and validates the output path.
        Contains blocking file system operations.
        """
        final_sdif_file_target = output_path.resolve()

        if final_sdif_file_target.is_dir():
            raise ValueError(
                f"Target output_path '{final_sdif_file_target}' is a directory. "
                "It must be a full file path for the target SDIF SQLite database (e.g., data.sqlite or data.sdif)."
            )
        if not final_sdif_file_target.suffix:
            logger.warning(
                f"Target output_path '{final_sdif_file_target}' has no file extension. "
                "It should be a path to an SDIF SQLite database file (e.g., data.sqlite or data.sdif)."
            )
        elif final_sdif_file_target.suffix.lower() not in (".sdif", ".sqlite", ".db"):
            logger.warning(
                f"Target output_path '{final_sdif_file_target}' does not have a common SQLite extension. "
                "Ensure this is the intended SQLite file path."
            )

        # Create a unique temporary directory for this standardization run
        run_temp_dir = Path(tempfile.mkdtemp(prefix="satif_aistd_run_"))
        intermediate_sdif_files_dir = run_temp_dir / "intermediate_sdif_files"
        intermediate_sdif_files_dir.mkdir(parents=True, exist_ok=True)
        file_processing_temp_dir = run_temp_dir / "file_processing_temp"
        file_processing_temp_dir.mkdir(parents=True, exist_ok=True)

        return (
            final_sdif_file_target,
            intermediate_sdif_files_dir,
            file_processing_temp_dir,
        )

    async def _cleanup_workspace(self, run_temp_dir: Path) -> None:
        """
        Cleans up the temporary workspace directory.
        Contains blocking file system operations.
        """
        if run_temp_dir.exists():
            try:
                await asyncio.to_thread(shutil.rmtree, run_temp_dir)
                logger.info(f"Cleaned up temporary run directory: {run_temp_dir}")
            except Exception as e_clean:
                logger.error(
                    f"Error cleaning up temporary run directory {run_temp_dir}: {e_clean}",
                    exc_info=True,
                )

    async def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,  # Expected to be the path to the target *SDIF file*
        *,
        overwrite: bool = False,
        config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> StandardizationResult:
        """
        Standardizes datasource to a single SDIF SQLite file.

        Args:
            datasource: Source data (file path, list of paths, or directory path).
            output_path: Path to the target output SDIF SQLite file (e.g., "./output/data.sdif").
            overwrite: If True, overwrite existing output file. Defaults to False.
            config: Optional configuration dictionary for standardizers.
            **kwargs: Additional arguments passed to child standardizers.

        Returns:
            StandardizationResult with the path to the created SDIF SQLite file.
        """
        logger.info(
            f"AIStandardizer starting process for output SDIF file: {output_path}"
        )

        # Setup workspace and validate output path - moved to a separate async function
        (
            final_sdif_file_target,
            intermediate_sdif_files_dir,
            file_processing_temp_dir,
        ) = await asyncio.to_thread(self._setup_workspace, Path(output_path), overwrite)

        run_temp_dir = file_processing_temp_dir.parent

        try:
            resolved_files = await self._resolve_input_files(
                datasource, file_processing_temp_dir
            )
            logger.info(f"Resolved {len(resolved_files)} file(s) for standardization.")

            # File grouping - potentially move to a thread if the list is very large
            grouped_by_std, unsupported = await asyncio.to_thread(
                self._group_files_by_standardizer, resolved_files
            )

            if not grouped_by_std:
                user_message = (
                    "No files found that can be handled by configured AI standardizers."
                )
                if unsupported:
                    user_message += (
                        f" Unsupported files: {[str(f.name) for f in unsupported]}"
                    )
                raise ValueError(user_message)

            logger.debug(
                f"File groups for standardization: { {cls.__name__: [f.name for f in paths] for cls, paths in grouped_by_std.items()} }"
            )

            (
                intermediate_sdif_files,
                aggregated_file_configs,
            ) = await self._process_file_groups(
                grouped_by_std, intermediate_sdif_files_dir, config, **kwargs
            )

            if not intermediate_sdif_files:
                raise RuntimeError(
                    "No intermediate SDIF SQLite files were successfully generated."
                )
            logger.info(
                f"Successfully generated {len(intermediate_sdif_files)} intermediate SDIF SQLite file(s)."
            )

            final_result = await asyncio.to_thread(
                self._consolidate_results,
                intermediate_sdif_files,
                aggregated_file_configs,
                final_sdif_file_target,
                overwrite,
            )

            logger.info(
                f"AIStandardizer process completed. Final SDIF file at: {final_result.output_path}"
            )
            return final_result

        except Exception as e:
            logger.error(f"AIStandardizer failed: {e}", exc_info=True)
            if isinstance(e, (ValueError, FileNotFoundError, FileExistsError)):
                raise
            raise RuntimeError(f"AIStandardizer processing error: {e}") from e
        finally:
            # Clean up using a dedicated async method
            await self._cleanup_workspace(run_temp_dir)
