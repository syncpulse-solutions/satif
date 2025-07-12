import asyncio
import logging
import zipfile
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Constants for ZIP file processing, kept local to this utility or passed as args if needed
_IGNORED_ZIP_MEMBER_PREFIXES = ("__MACOSX/",)
_IGNORED_ZIP_FILENAME_PREFIXES = ("._",)
_IGNORED_ZIP_FILENAMES = (".DS_Store",)


async def extract_zip_archive_async(
    zip_path: Path,
    extract_to: Path,
    ignored_member_prefixes: Tuple[str, ...] = _IGNORED_ZIP_MEMBER_PREFIXES,
    ignored_filename_prefixes: Tuple[str, ...] = _IGNORED_ZIP_FILENAME_PREFIXES,
    ignored_filenames: Tuple[str, ...] = _IGNORED_ZIP_FILENAMES,
) -> List[Path]:
    """
    Asynchronously extracts a ZIP archive to a specified directory, filtering out ignored files.

    Args:
        zip_path: Path to the ZIP archive.
        extract_to: Directory where the contents will be extracted.
        ignored_member_prefixes: Tuple of member path prefixes to ignore.
        ignored_filename_prefixes: Tuple of filename prefixes to ignore.
        ignored_filenames: Tuple of exact filenames to ignore.

    Returns:
        A list of paths to the successfully extracted files.

    Raises:
        ValueError: If the zip_path is invalid or corrupted.
        RuntimeError: If any other error occurs during extraction.
    """

    def blocking_extract() -> List[Path]:
        extracted_file_paths = []
        logger.info(f"Extracting ZIP archive '{zip_path.name}' to '{extract_to}'...")
        try:
            extract_to.mkdir(
                parents=True, exist_ok=True
            )  # Ensure extract_to directory exists

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Security: Preliminary check for unsafe paths before extraction
                for member_name in zip_ref.namelist():
                    if member_name.startswith(("/", "..")):
                        logger.error(
                            f"Skipping potentially unsafe path in ZIP: {member_name}"
                        )
                        # Depending on security policy, might raise an error here
                        continue

                # Extract all members
                zip_ref.extractall(extract_to)

            # After extractall, collect all *file* paths, applying filters
            # This second pass of filtering ensures that even if extractall creates them,
            # we don't return paths to ignored files.
            for root, _, files in extract_to.walk():
                for filename in files:
                    full_path = root / filename
                    # Create a path relative to 'extract_to' to check against member prefixes
                    # This ensures that '__MACOSX/file.txt' is correctly ignored,
                    # not just a top-level '__MACOSX' directory.
                    try:
                        relative_path_to_check = full_path.relative_to(extract_to)
                    except ValueError:
                        # This can happen if full_path is not under extract_to,
                        # which ideally shouldn't occur if zip_ref.extractall worked as expected
                        # and target_path checks were effective.
                        logger.warning(
                            f"File {full_path} seems to be outside extraction root {extract_to}. Skipping."
                        )
                        continue

                    path_str_to_check_prefixes = str(relative_path_to_check)

                    if not (
                        any(
                            path_str_to_check_prefixes.startswith(p)
                            for p in ignored_member_prefixes
                        )
                        or any(
                            full_path.name.startswith(p)
                            for p in ignored_filename_prefixes
                        )
                        or full_path.name in ignored_filenames
                    ):
                        extracted_file_paths.append(full_path)
                    else:
                        logger.debug(f"Ignoring file post-extraction: {full_path}")

            if not extracted_file_paths:
                logger.warning(
                    f"ZIP archive '{zip_path.name}' is empty or contains no processable files after filtering."
                )
            else:
                logger.info(
                    f"Successfully extracted {len(extracted_file_paths)} file(s) from '{zip_path.name}'."
                )
            return extracted_file_paths
        except zipfile.BadZipFile as e:
            logger.error(
                f"Invalid or corrupted ZIP file: {zip_path.name}", exc_info=True
            )
            raise ValueError(f"Invalid or corrupted ZIP file: {zip_path.name}") from e
        except Exception as e:
            logger.error(
                f"Failed to extract ZIP archive '{zip_path.name}': {e}", exc_info=True
            )
            raise RuntimeError(
                f"Unexpected error during ZIP extraction for '{zip_path.name}'"
            ) from e

    return await asyncio.to_thread(blocking_extract)
