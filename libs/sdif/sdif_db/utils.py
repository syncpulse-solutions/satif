import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class DBConnectionError(Exception):
    """Exception raised for errors in SDIF database connection."""

    pass


def create_db_connection(
    sdif_sources: Dict[str, Union[Path, str]],
) -> Tuple[sqlite3.Connection, Dict[str, Path]]:
    """
    Creates an SQLite connection and attaches SDIF sources as schemas.

    - If one source is provided, connects directly to that file and attaches it
      under its specified schema name. The main connection will be to the file itself.
    - If multiple sources are provided, creates an in-memory database and attaches
      all sources.

    Args:
        sdif_sources: Dictionary mapping schema names to resolved SDIF file paths.

    Returns:
        A tuple containing the sqlite3.Connection and a dictionary of
        successfully attached schemas (schema_name: path).

    Raises:
        DBConnectionError: If no sources are provided, a file is not found,
                            or an SQLite error occurs during connection/attachment.
    """
    if not sdif_sources:
        raise DBConnectionError(
            "No SDIF sources provided to set up database connection."
        )

    # Validate paths first - this is crucial as Path.exists() needs to be called on resolved paths
    for schema_name, file_path in sdif_sources.items():
        if not Path(file_path).exists() or not Path(file_path).is_file():
            # Using DBConnectionError for consistency with LocalCodeExecutor's existing error types for setup issues.
            raise DBConnectionError(
                f"Input SDIF file for schema '{schema_name}' not found or is not a file: {file_path}"
            )

    db_conn: sqlite3.Connection
    successfully_attached_schemas: Dict[str, Path] = {}

    try:
        if len(sdif_sources) == 1:
            # For a single source, connect to it directly.
            # Then, ATTACH it again under its designated schema_name. This ensures that
            # even if the main db connection is to 'file.sdif' (accessible as 'main'),
            # the user-provided schema_name (e.g., 'db1') is also available.
            schema_name, file_path = next(iter(sdif_sources.items()))
            logger.debug(
                f"Connecting directly to single SDIF source: {file_path}. It will be attached as schema '{schema_name}'."
            )
            db_conn = sqlite3.connect(str(file_path))
            try:
                db_conn.execute(
                    f"ATTACH DATABASE ? AS {schema_name}", (str(file_path),)
                )
                successfully_attached_schemas[schema_name] = file_path
                logger.debug(
                    f"Successfully attached '{file_path}' as schema '{schema_name}'."
                )
            except sqlite3.Error as e:
                db_conn.close()
                db_conn = None  # type: ignore
                raise DBConnectionError(
                    f"Failed to attach single database '{file_path}' as schema '{schema_name}': {e}"
                ) from e
        else:
            # For multiple sources, create an in-memory database and attach all.
            logger.debug(
                "Creating in-memory database for attaching multiple SDIF sources."
            )
            db_conn = sqlite3.connect(":memory:")
            for schema_name, file_path in sdif_sources.items():
                logger.debug(
                    f"Attaching SDIF source {file_path} as schema '{schema_name}'."
                )
                try:
                    db_conn.execute(
                        f"ATTACH DATABASE ? AS {schema_name}", (str(file_path),)
                    )
                    successfully_attached_schemas[schema_name] = file_path
                except sqlite3.Error as e:
                    db_conn.close()
                    db_conn = None  # type: ignore
                    raise DBConnectionError(
                        f"Failed to attach database '{file_path}' as schema '{schema_name}': {e}"
                    ) from e
        return db_conn, successfully_attached_schemas
    except Exception as e:
        # Catch any other unexpected error during setup, ensure connection is closed if partially opened
        if "db_conn" in locals() and db_conn is not None:  # type: ignore
            try:
                db_conn.close()  # type: ignore
            except sqlite3.Error as close_err:
                logger.error(
                    f"Failed to close DB connection during setup error handling: {close_err}"
                )
        if isinstance(e, DBConnectionError):
            raise
        # Wrap other exceptions in DBConnectionError for consistency from this utility
        raise DBConnectionError(
            f"Unexpected error during database connection setup: {e}"
        ) from e


def cleanup_db_connection(
    conn: Optional[sqlite3.Connection],
    attached_schemas: Dict[str, Path],
    should_close: bool = True,
) -> None:
    """
    Cleans up an SQLite connection by detaching schemas and optionally closing it.

    Args:
        conn: The sqlite3.Connection to clean up. Can be None.
        attached_schemas: A dictionary of schemas (schema_name: path) that were attached
                          and should be detached.
        should_close: Whether to close the connection after detaching schemas.
    """
    if conn is None:
        return

    for schema_name in attached_schemas.keys():
        try:
            logger.debug(f"Detaching schema '{schema_name}' during cleanup.")
            conn.execute(f"DETACH DATABASE {schema_name};")
        except sqlite3.Error as e:
            # Log error but continue cleanup for other schemas and final close.
            logger.error(
                f"Error detaching database '{schema_name}' during cleanup: {e}"
            )

    if should_close:
        try:
            logger.debug("Closing database connection during cleanup.")
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error closing database connection during cleanup: {e}")
