import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


class SDIFDatabase:
    def __init__(
        self,
        path: Union[str, Path],
        overwrite: bool = False,
        read_only: bool = False,
        schema_name: str = "db1",
    ):
        """
        Initialize the SDIFDatabase.

        Args:
            path: Path to the SDIF SQLite file.
            overwrite: If True, overwrite the file if it exists (only applies if read_only=False).
            read_only: If True, open the database in read-only mode. Will raise error if file doesn't exist.
            schema_name: Schema name to use when the database is attached in a connection. Default: "db1"
        """
        self.path = Path(path).resolve()
        self.read_only = read_only
        self.schema_name = schema_name

        if read_only:
            # --- Read-Only Logic ---
            if not self.path.exists():
                raise FileNotFoundError(f"SDIF file not found for reading: {self.path}")
            if overwrite:
                log.warning(
                    f"Ignoring 'overwrite=True' as database is opened in read-only mode: {self.path}"
                )
            try:
                # Use mode=ro URI
                self.conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
            except sqlite3.OperationalError as e:
                raise sqlite3.OperationalError(
                    f"Failed to connect to {self.path} in read-only mode: {e}"
                ) from e
        else:
            # --- Read-Write Logic ---
            if self.path.exists():
                if overwrite:
                    # --- Overwrite Existing File ---
                    log.warning(f"Overwriting existing file: {self.path}")
                    try:
                        # Attempt to close connection if held by this instance (unlikely but safe)
                        if hasattr(self, "conn") and self.conn:
                            try:
                                self.conn.close()
                            except Exception:
                                pass  # Ignore close errors
                            self.conn = None  # type: ignore
                        self.path.unlink()
                        # Let the 'create new' logic handle connection and table setup
                    except OSError as e:
                        raise OSError(
                            f"Could not remove existing file {self.path} during overwrite: {e}"
                        ) from e
                    # Fall through to the connection logic below (as if file didn't exist)

                else:
                    # --- Open Existing File (Read-Write) ---
                    log.debug(f"Opening existing file for read-write: {self.path}")
                    try:
                        # Connect using default mode (rwc via URI, effectively read-write)
                        self.conn = sqlite3.connect(
                            f"file:{self.path}?mode=rwc", uri=True
                        )  # Ensure rwc mode explicitly
                        # Set pragmas for existing connection too
                        self.conn.execute(
                            "PRAGMA journal_mode=WAL;"
                        )  # Attempt to set WAL
                        self.conn.execute("PRAGMA foreign_keys = ON;")
                        # Optional: Verify if it's a minimal SDIF DB (e.g., check for metadata tables)?
                        # For now, assume connection success means it's usable.
                    except sqlite3.Error as e:
                        raise sqlite3.Error(
                            f"Failed to connect to existing database {self.path}: {e}"
                        ) from e
                    # Skip _create_metadata_tables for existing files

            # --- Create New File (or connect after overwrite) ---
            if (
                not self.path.exists() or overwrite
            ):  # Condition to create/connect after potential unlink
                log.debug(f"Creating or connecting to specified file: {self.path}")
                self.path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    self.conn = sqlite3.connect(
                        f"file:{self.path}?mode=rwc", uri=True
                    )  # Create or open
                    self.conn.execute("PRAGMA journal_mode=WAL;")
                    self.conn.execute("PRAGMA foreign_keys = ON;")
                    # Only create tables if the file was genuinely *new* or overwritten
                    # We check if essential metadata tables exist; if not, create them.
                    cursor = self.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='sdif_properties'"
                    )
                    if not cursor.fetchone():
                        log.info(f"Initializing SDIF metadata tables in {self.path}")
                        self._create_metadata_tables()
                    else:
                        log.debug(f"SDIF metadata tables already exist in {self.path}")

                except sqlite3.Error as e:
                    raise sqlite3.Error(
                        f"Failed to create or connect to database {self.path}: {e}"
                    ) from e

        # --- Common Post-Connection Setup (for all cases: ro, rw-existing, rw-new) ---
        if not hasattr(self, "conn") or not self.conn:
            # This should not happen if logic above is correct
            raise RuntimeError(
                f"Database connection was not established for {self.path}"
            )

        self.conn.row_factory = sqlite3.Row
        try:
            # Use PRAGMA database_list instead of re-attaching if possible
            # This avoids "already attached" errors more reliably.
            # However, attaching ensures the schema name is known/consistent for this instance.
            # Let's try attaching and handle the specific error gracefully.
            self.conn.execute(
                f"ATTACH DATABASE ? AS {self.schema_name}", (str(self.path),)
            )
            log.debug(f"Attached database {self.path} as schema '{self.schema_name}'")
        except sqlite3.OperationalError as e:
            # Check if the error is specifically "database is already attached"
            # The exact error message might vary slightly across SQLite versions
            if "already attached" in str(e) or "duplicate database name" in str(e):
                log.warning(
                    f"Database {self.path} seems already attached (possibly as '{self.schema_name}'). Skipping attach command."
                )
            else:
                # Re-raise other operational errors during attach
                raise sqlite3.OperationalError(
                    f"Failed to attach database {self.path} as '{self.schema_name}': {e}"
                ) from e

    def _validate_connection(self):
        """Checks if the database connection is active."""
        if not self.conn:
            raise sqlite3.ProgrammingError("Database connection is closed.")
        # Optionally, add a ping or quick check
        # try:
        #     self.conn.execute("SELECT 1").fetchone()
        # except sqlite3.ProgrammingError: # Handles cases where connection might be closed unexpectedly
        #     raise sqlite3.ProgrammingError("Database connection is closed or invalid.")

    def _generate_unique_table_name(self, base_name: str) -> str:
        """Generates a unique table name by appending a suffix if the base name exists.

        Args:
            base_name: The initial desired table name.

        Returns:
            A unique table name, which might be the base_name or base_name_N.

        Raises:
            RuntimeError: If a unique name cannot be found after a reasonable number of attempts.
        """
        self._validate_connection()

        def name_exists(name_to_check: str) -> bool:
            # Check in SDIF metadata
            meta_exists_cursor = self.conn.execute(
                "SELECT 1 FROM sdif_tables_metadata WHERE table_name = ?",
                (name_to_check,),
            )
            if meta_exists_cursor.fetchone():
                return True
            # Check in sqlite_master (physical tables/views)
            phys_exists_cursor = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
                (name_to_check,),
            )
            if phys_exists_cursor.fetchone():
                return True
            return False

        if not name_exists(base_name):
            return base_name

        i = 1
        while True:
            new_name = f"{base_name}_{i}"
            if not name_exists(new_name):
                return new_name
            i += 1
            if i > 1000:  # Safety break
                log.error(
                    f"Failed to generate a unique name for base '{base_name}' after {i - 1} attempts."
                )
                raise RuntimeError(
                    f"Could not find a unique table name for base '{base_name}' after 1000 attempts."
                )

    def _create_metadata_tables(self):
        """Create the required SDIF metadata tables."""
        self._validate_connection()
        try:
            with self.conn:  # Use context manager for transaction
                # sdif_properties table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_properties (
                    sdif_version TEXT NOT NULL,
                    creation_timestamp TEXT
                )
                """)

                # Check if we need to insert the properties row
                if not self.conn.execute("SELECT 1 FROM sdif_properties").fetchone():
                    self.conn.execute(
                        "INSERT INTO sdif_properties (sdif_version, creation_timestamp) VALUES (?, ?)",
                        ("1.0", datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")),
                    )

                # sdif_sources table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_sources (
                    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_file_name TEXT NOT NULL,
                    original_file_type TEXT NOT NULL,
                    source_description TEXT,
                    processing_timestamp TEXT
                )
                """)

                # sdif_tables_metadata table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_tables_metadata (
                    table_name TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    description TEXT,
                    original_identifier TEXT,
                    row_count INTEGER,
                    FOREIGN KEY (source_id) REFERENCES sdif_sources(source_id) ON DELETE RESTRICT -- Protect sources
                )
                """)

                # sdif_columns_metadata table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_columns_metadata (
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    description TEXT,
                    original_column_name TEXT,
                    PRIMARY KEY (table_name, column_name),
                    FOREIGN KEY (table_name) REFERENCES sdif_tables_metadata(table_name) ON DELETE CASCADE
                )
                """)

                # sdif_objects table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_objects (
                    object_name TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    json_data TEXT NOT NULL,
                    description TEXT,
                    schema_hint TEXT, -- Store as TEXT (JSON string)
                    FOREIGN KEY (source_id) REFERENCES sdif_sources(source_id) ON DELETE RESTRICT -- Protect sources
                )
                """)

                # sdif_media table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_media (
                    media_name TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    media_data BLOB NOT NULL,
                    description TEXT,
                    original_format TEXT,
                    technical_metadata TEXT, -- Store as TEXT (JSON string)
                    FOREIGN KEY (source_id) REFERENCES sdif_sources(source_id) ON DELETE RESTRICT -- Protect sources
                )
                """)

                # sdif_semantic_links table
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sdif_semantic_links (
                    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    link_type TEXT NOT NULL,
                    description TEXT,
                    from_element_type TEXT NOT NULL CHECK (from_element_type IN ('table', 'column', 'object', 'media', 'json_path', 'source')), -- Added source
                    from_element_spec TEXT NOT NULL, -- Store as TEXT (JSON string)
                    to_element_type TEXT NOT NULL CHECK (to_element_type IN ('table', 'column', 'object', 'media', 'json_path', 'source')), -- Added source
                    to_element_spec TEXT NOT NULL -- Store as TEXT (JSON string)
                )
                """)
        except sqlite3.Error as e:
            log.error(f"Error creating metadata tables: {e}")
            raise  # Re-raise the exception

    def add_source(
        self, file_name: str, file_type: str, description: Optional[str] = None
    ) -> int:
        """
        Add a source to the SDIF file.

        Args:
            file_name: The name of the original file
            file_type: The type of the original file (csv, xlsx, json, etc.)
            description: Optional description of the source

        Returns:
            The source_id of the inserted source
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        try:
            cursor = self.conn.execute(
                """
                INSERT INTO sdif_sources
                (original_file_name, original_file_type, source_description, processing_timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (
                    file_name,
                    file_type,
                    description,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            self.conn.commit()
            source_id = cursor.lastrowid
            if source_id is None:
                raise RuntimeError("Failed to get last inserted source_id.")
            return source_id
        except sqlite3.Error as e:
            log.error(f"Error adding source '{file_name}': {e}")
            self.conn.rollback()  # Rollback on error
            raise

    def create_table(
        self,
        table_name: str,
        columns: Dict[str, Dict[str, Any]],
        source_id: int,
        description: Optional[str] = None,
        original_identifier: Optional[str] = None,
        if_exists: str = "fail",  # Options: 'fail', 'replace', 'add'
    ) -> str:
        """
        Create a data table in the SDIF file and registers its metadata.
        Handles conflicts if a table with the same name already exists.

        Args:
            table_name: The name of the table to create (must not start with 'sdif_')
            columns: Dict mapping column names to their properties.
                     Example: {"col_name": {"type": "TEXT", "not_null": True, "description": "...", "original_column_name": "...", "original_format": "..."}}
                     Supported properties: type (SQLite type), not_null (bool), primary_key (bool),
                                           foreign_key ({"table": "target_table", "column": "target_col"}),
                                           description (str), original_column_name (str), original_format (str).
            source_id: The source_id reference.
            description: Optional description of the table for sdif_tables_metadata.
            original_identifier: Optional original identifier for sdif_tables_metadata.
            if_exists: Strategy to handle pre-existing table:
                       - "fail" (default): Raise ValueError if table exists.
                       - "replace": Drop existing table and create anew.
                       - "add": Create the new table with a unique suffixed name (e.g., table_name_1).

        Returns:
            The actual name of the table created in the database (could be suffixed).

        Raises:
            PermissionError: If database is read-only.
            ValueError: If table_name is invalid, columns are empty, source_id is invalid,
                        or if table exists and if_exists='fail', or invalid if_exists value.
            sqlite3.Error: For underlying database errors.
            RuntimeError: If 'add' fails to find a unique name.
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        if table_name.startswith("sdif_"):
            raise ValueError("User data tables must not start with 'sdif_'")
        if not columns:
            raise ValueError("Cannot create a table with no columns.")

        # Validate source_id exists
        source_cursor = self.conn.execute(
            "SELECT 1 FROM sdif_sources WHERE source_id = ?", (source_id,)
        )
        if not source_cursor.fetchone():
            raise ValueError(
                f"Invalid source_id: {source_id} does not exist in sdif_sources."
            )

        effective_table_name = table_name

        # Check for existing table (metadata and physical)
        metadata_exists_cursor = self.conn.execute(
            "SELECT 1 FROM sdif_tables_metadata WHERE table_name = ?", (table_name,)
        )
        metadata_exists = metadata_exists_cursor.fetchone() is not None

        physical_exists_cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        physical_exists = physical_exists_cursor.fetchone() is not None

        table_actually_exists = metadata_exists or physical_exists

        if table_actually_exists:
            if if_exists == "fail":
                error_msg = f"Table '{table_name}' already exists."
                if metadata_exists and not physical_exists:
                    error_msg += " (Found in SDIF metadata but not physically)."
                elif not metadata_exists and physical_exists:
                    error_msg += " (Found physically but not in SDIF metadata)."
                else:  # Exists in both
                    error_msg += " (Found in SDIF metadata and physically)."
                error_msg += " Use if_exists='replace' or 'add' to resolve."
                raise ValueError(error_msg)
            elif if_exists == "replace":
                log.warning(
                    f"Table '{table_name}' exists and if_exists='replace'. Dropping and recreating."
                )
                try:
                    self.drop_table(
                        table_name
                    )  # This handles metadata and physical table
                except Exception as e:
                    # Log original error and re-raise wrapped error
                    log.error(
                        f"Error during table replacement process for '{table_name}': {e}"
                    )
                    raise RuntimeError(
                        f"Failed to replace table '{table_name}'. Original error: {e}"
                    ) from e
                effective_table_name = table_name  # Stays the same
            elif if_exists == "add":
                log.info(
                    f"Table '{table_name}' exists and if_exists='add'. Attempting to find a new unique name."
                )
                effective_table_name = self._generate_unique_table_name(table_name)
                log.info(f"New table will be created as '{effective_table_name}'.")
            else:
                raise ValueError(
                    f"Invalid if_exists strategy: '{if_exists}'. Choose 'fail', 'replace', or 'add'."
                )
        elif if_exists not in ["fail", "replace", "add"]:
            # This case handles if table does NOT exist, but an invalid if_exists strategy was given.
            raise ValueError(
                f"Invalid if_exists strategy: '{if_exists}'. Choose 'fail', 'replace', or 'add'."
            )

        column_defs = []
        column_metadata_rows = []

        for col_name, col_props in columns.items():
            if not isinstance(col_props, dict):
                raise TypeError(
                    f"Properties for column '{col_name}' must be a dictionary."
                )

            col_type = col_props.get("type", "TEXT").upper()
            valid_types = [
                "TEXT",
                "INTEGER",
                "REAL",
                "BLOB",
                "NUMERIC",
                "DATE",
                "DATETIME",
            ]
            if not any(col_type.startswith(valid) for valid in valid_types):
                log.warning(
                    f"Potentially non-standard SQLite type '{col_type}' used for column '{col_name}' in table '{effective_table_name}'."
                )

            constraints = []
            if col_props.get("primary_key"):
                constraints.append("PRIMARY KEY")
            if col_props.get("not_null"):
                constraints.append("NOT NULL")
            if col_props.get("unique"):
                constraints.append("UNIQUE")

            column_defs.append(f'"{col_name}" {col_type} {" ".join(constraints)}')

            column_metadata_rows.append(
                (
                    effective_table_name,  # Use effective_table_name
                    col_name,
                    col_props.get("description"),
                    col_props.get("original_column_name"),
                )
            )

        table_constraints = []
        pk_cols = [
            f'"{name}"' for name, props in columns.items() if props.get("primary_key")
        ]
        if len(pk_cols) > 1:
            table_constraints.append(f"PRIMARY KEY ({', '.join(pk_cols)})")
        # Remove individual PRIMARY KEY constraints if a table-level one exists
        if table_constraints:
            column_defs = [c.replace(" PRIMARY KEY", "") for c in column_defs]

        for col_name, col_props in columns.items():
            fk = col_props.get("foreign_key")
            if isinstance(fk, dict) and "table" in fk and "column" in fk:
                target_table = fk["table"]
                target_col = fk["column"]
                on_delete = fk.get("on_delete", "").upper()
                on_update = fk.get("on_update", "").upper()
                fk_clause = f'FOREIGN KEY ("{col_name}") REFERENCES "{target_table}"("{target_col}")'
                if on_delete in [
                    "CASCADE",
                    "SET NULL",
                    "SET DEFAULT",
                    "RESTRICT",
                    "NO ACTION",
                ]:
                    fk_clause += f" ON DELETE {on_delete}"
                if on_update in [
                    "CASCADE",
                    "SET NULL",
                    "SET DEFAULT",
                    "RESTRICT",
                    "NO ACTION",
                ]:
                    fk_clause += f" ON UPDATE {on_update}"
                table_constraints.append(fk_clause)

        create_table_sql = f"""
        CREATE TABLE "{effective_table_name}" (
            {", ".join(column_defs)}
            {", " + ", ".join(table_constraints) if table_constraints else ""}
        )
        """

        try:
            with self.conn:  # Transaction
                # Check if table already exists physically (should be handled by if_exists logic if replacing)
                # But a final check here with effective_table_name is good practice before CREATE
                final_check_cursor = self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (effective_table_name,),
                )
                if (
                    final_check_cursor.fetchone() and if_exists != "replace"
                ):  # if 'replace', it should have been dropped.
                    # if 'add', this name should be unique.
                    # if 'fail', this block shouldn't be reached.
                    # This indicates a potential race condition or logic error if reached for non-replace modes.
                    log.error(
                        f"Critical: Table '{effective_table_name}' unexpectedly exists before creation attempt. Strategy: {if_exists}"
                    )
                    raise sqlite3.OperationalError(
                        f"Table '{effective_table_name}' already exists in the database, conflict with if_exists='{if_exists}' logic."
                    )

                # Check if table metadata already exists for effective_table_name
                meta_check_cursor = self.conn.execute(
                    "SELECT 1 FROM sdif_tables_metadata WHERE table_name = ?",
                    (effective_table_name,),
                )
                if meta_check_cursor.fetchone() and if_exists != "replace":
                    log.error(
                        f"Critical: Metadata for '{effective_table_name}' unexpectedly exists. Strategy: {if_exists}"
                    )
                    raise ValueError(
                        f"Metadata for table '{effective_table_name}' already exists, conflict with if_exists='{if_exists}' logic."
                    )

                self.conn.execute(create_table_sql)

                self.conn.execute(
                    """
                    INSERT INTO sdif_tables_metadata
                    (table_name, source_id, description, original_identifier, row_count)
                    VALUES (?, ?, ?, ?, 0)
                    """,
                    (effective_table_name, source_id, description, original_identifier),
                )

                self.conn.executemany(
                    """
                    INSERT INTO sdif_columns_metadata
                    (table_name, column_name, description, original_column_name)
                    VALUES (?, ?, ?, ?)
                    """,
                    column_metadata_rows,
                )
            return effective_table_name  # Return the actual table name used
        except sqlite3.Error as e:
            log.error(
                f"Error creating table '{effective_table_name}' or its metadata: {e}"
            )
            raise

    def insert_data(self, table_name: str, data: List[Dict[str, Any]]):
        """
        Insert data into a table. Assumes table has been created via create_table.

        Args:
            table_name: The name of the table
            data: List of dictionaries mapping column names to values
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        if not data:
            log.info(f"No data provided for insertion into table '{table_name}'.")
            return

        # Verify table exists in metadata
        cursor = self.conn.execute(
            "SELECT row_count FROM sdif_tables_metadata WHERE table_name = ?",
            (table_name,),
        )
        meta_row = cursor.fetchone()
        if not meta_row:
            raise ValueError(
                f"Table '{table_name}' not found in sdif_tables_metadata. Use create_table first."
            )
        current_row_count = (
            meta_row["row_count"] if meta_row["row_count"] is not None else 0
        )

        # Get column names from the first row (assuming all rows have the same keys)
        # It's safer to get column names from the table schema if possible,
        # but using the data keys is common practice. Validate keys against schema?
        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        # Wrap column names in double quotes
        quoted_columns = [f'"{col}"' for col in columns]
        columns_str = ", ".join(quoted_columns)

        # Prepare the insert statement, wrap table name in double quotes
        # Using INSERT OR IGNORE or INSERT OR REPLACE might be options depending on desired behavior for duplicates,
        # but standard INSERT fails on constraint violations, which is usually desired.
        insert_sql = (
            f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
        )

        # Prepare the data: ensure order matches columns list
        values = []
        for i, row in enumerate(data):
            if row.keys() != set(columns):
                # Handle rows with missing/extra keys compared to the first row
                log.warning(
                    f"Row {i} in data for table '{table_name}' has different keys than the first row. Trying to insert based on first row's keys."
                )
                # Attempt to build the value list based on the expected 'columns'
                row_values = [row.get(col) for col in columns]
            else:
                row_values = [row[col] for col in columns]
            values.append(row_values)

        try:
            with self.conn:  # Transaction
                # Execute insert
                self.conn.executemany(insert_sql, values)

                # Update row count in metadata
                new_row_count = current_row_count + len(data)
                self.conn.execute(
                    "UPDATE sdif_tables_metadata SET row_count = ? WHERE table_name = ?",
                    (new_row_count, table_name),
                )
        except sqlite3.Error as e:
            log.error(f"Error inserting data into table '{table_name}': {e}")
            # Rollback handled by context manager
            raise  # Re-raise

    def add_object(
        self,
        object_name: str,
        json_data: Any,
        source_id: int,
        description: Optional[str] = None,
        schema_hint: Optional[Dict] = None,
    ):
        """
        Add a JSON object to the SDIF file.

        Args:
            object_name: A unique name for the object
            json_data: The data to store (will be converted to JSON string)
            source_id: The source_id reference
            description: Optional description
            schema_hint: Optional JSON schema (as dict, will be stored as JSON string)
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")

        # Validate source_id exists
        cursor = self.conn.execute(
            "SELECT 1 FROM sdif_sources WHERE source_id = ?", (source_id,)
        )
        if not cursor.fetchone():
            raise ValueError(
                f"Invalid source_id: {source_id} does not exist in sdif_sources."
            )

        try:
            json_str = json.dumps(json_data)
        except TypeError as e:
            raise TypeError(
                f"Data for object '{object_name}' is not JSON serializable: {e}"
            ) from e

        schema_str = None
        if schema_hint is not None:
            try:
                schema_str = json.dumps(schema_hint)
            except TypeError as e:
                raise TypeError(
                    f"Schema hint for object '{object_name}' is not JSON serializable: {e}"
                ) from e

        try:
            with self.conn:  # Transaction
                self.conn.execute(
                    """
                    INSERT INTO sdif_objects
                    (object_name, source_id, json_data, description, schema_hint)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (object_name, source_id, json_str, description, schema_str),
                )
        except sqlite3.IntegrityError as e:
            # Likely duplicate object_name
            log.error(
                f"Integrity error adding object '{object_name}'. Does it already exist? Error: {e}"
            )
            raise ValueError(
                f"Object with name '{object_name}' may already exist."
            ) from e
        except sqlite3.Error as e:
            log.error(f"Error adding object '{object_name}': {e}")
            raise  # Re-raise

    def add_media(
        self,
        media_name: str,
        media_data: bytes,
        media_type: str,
        source_id: int,
        description: Optional[str] = None,
        original_format: Optional[str] = None,
        technical_metadata: Optional[Dict] = None,
    ):
        """
        Add binary media data to the SDIF file.

        Args:
            media_name: A unique name for the media
            media_data: The binary data (must be bytes)
            media_type: The type of media (image, audio, video, binary)
            source_id: The source_id reference
            description: Optional description
            original_format: Optional format (png, jpeg, etc.)
            technical_metadata: Optional technical metadata (as dict, stored as JSON string)
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        if not isinstance(media_data, bytes):
            raise TypeError("media_data must be of type bytes.")

        # Validate source_id exists
        cursor = self.conn.execute(
            "SELECT 1 FROM sdif_sources WHERE source_id = ?", (source_id,)
        )
        if not cursor.fetchone():
            raise ValueError(
                f"Invalid source_id: {source_id} does not exist in sdif_sources."
            )

        tech_meta_str = None
        if technical_metadata is not None:
            try:
                tech_meta_str = json.dumps(technical_metadata)
            except TypeError as e:
                raise TypeError(
                    f"Technical metadata for media '{media_name}' is not JSON serializable: {e}"
                ) from e

        try:
            with self.conn:  # Transaction
                self.conn.execute(
                    """
                    INSERT INTO sdif_media
                    (media_name, source_id, media_type, media_data, description, original_format, technical_metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        media_name,
                        source_id,
                        media_type,
                        media_data,  # Pass bytes directly for BLOB
                        description,
                        original_format,
                        tech_meta_str,
                    ),
                )
        except sqlite3.IntegrityError as e:
            log.error(
                f"Integrity error adding media '{media_name}'. Does it already exist? Error: {e}"
            )
            raise ValueError(
                f"Media with name '{media_name}' may already exist."
            ) from e
        except sqlite3.Error as e:
            log.error(f"Error adding media '{media_name}': {e}")
            raise

    def add_semantic_link(
        self,
        link_type: str,
        from_element_type: str,
        from_element_spec: Dict,
        to_element_type: str,
        to_element_spec: Dict,
        description: Optional[str] = None,
    ):
        """
        Add a semantic link between elements.

        Args:
            link_type: The type of link (annotation, reference, logical_foreign_key)
            from_element_type: Type of source element ('table', 'column', 'object', 'media', 'json_path', 'source')
            from_element_spec: Specification of the source element (as dict, stored as JSON string)
            to_element_type: Type of target element ('table', 'column', 'object', 'media', 'json_path', 'source')
            to_element_spec: Specification of the target element (as dict, stored as JSON string)
            description: Optional description
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")

        valid_element_types = {
            "table",
            "column",
            "object",
            "media",
            "json_path",
            "source",
        }
        if from_element_type not in valid_element_types:
            raise ValueError(
                f"Invalid from_element_type: '{from_element_type}'. Must be one of {valid_element_types}"
            )
        if to_element_type not in valid_element_types:
            raise ValueError(
                f"Invalid to_element_type: '{to_element_type}'. Must be one of {valid_element_types}"
            )

        try:
            from_spec_str = json.dumps(from_element_spec)
            to_spec_str = json.dumps(to_element_spec)
        except TypeError as e:
            raise TypeError(
                f"Element specification for semantic link is not JSON serializable: {e}"
            ) from e

        try:
            with self.conn:  # Transaction
                self.conn.execute(
                    """
                    INSERT INTO sdif_semantic_links
                    (link_type, description, from_element_type, from_element_spec, to_element_type, to_element_spec)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        link_type,
                        description,
                        from_element_type,
                        from_spec_str,
                        to_element_type,
                        to_spec_str,
                    ),
                )
        except sqlite3.Error as e:
            log.error(f"Error adding semantic link of type '{link_type}': {e}")
            # Check constraints might fail if types are wrong, but handled by initial checks
            raise

    def close(self):
        """Close the database connection."""
        if self.conn:
            try:
                # Optional: Commit any pending transaction explicitly, although closing usually does this unless rollback occurred.
                # self.conn.commit()
                self.conn.close()
                log.info(f"Closed connection to SDIF database: {self.path}")
            except sqlite3.Error as e:
                log.error(f"Error closing database connection {self.path}: {e}")
            finally:
                self.conn = None  # Ensure it's marked as closed

    def __enter__(self):
        """
        Context manager enter method.

        Returns:
            self: The database object
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit method.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self.close()

    def __del__(self):
        """
        Ensure connection is closed when object is garbage collected
        Note: __del__ can be unreliable, using context manager is better.
        """
        if hasattr(self, "conn") and self.conn:
            log.debug(f"Closing connection for {self.path} from __del__")
            self.close()

    # --- Reading Methods ---

    def get_properties(self) -> Optional[Dict[str, Any]]:
        """Get the global properties from sdif_properties."""
        self._validate_connection()
        try:
            cursor = self.conn.execute("SELECT * FROM sdif_properties LIMIT 1")
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            log.error(f"Error reading sdif_properties: {e}")
            return None  # Or re-raise? Returning None indicates properties couldn't be read.

    def list_sources(self) -> List[Dict[str, Any]]:
        """List all sources from sdif_sources."""
        self._validate_connection()
        try:
            cursor = self.conn.execute("SELECT * FROM sdif_sources ORDER BY source_id")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            log.error(f"Error listing sources: {e}")
            return []  # Return empty list on error

    def list_tables(self) -> List[str]:
        """List the names of all user data tables registered in metadata."""
        self._validate_connection()
        try:
            cursor = self.conn.execute(
                "SELECT table_name FROM sdif_tables_metadata ORDER BY table_name"
            )
            return [row["table_name"] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            log.error(f"Error listing tables from metadata: {e}")
            return []

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific user table from SDIF metadata tables."""
        self._validate_connection()
        try:
            cursor = self.conn.execute(
                "SELECT * FROM sdif_tables_metadata WHERE table_name = ?", (table_name,)
            )
            table_meta = cursor.fetchone()
            if not table_meta:
                return None

            cursor = self.conn.execute(
                "SELECT * FROM sdif_columns_metadata WHERE table_name = ? ORDER BY column_name",
                (table_name,),  # Order for consistency
            )
            columns_meta = [dict(row) for row in cursor.fetchall()]
            result = dict(table_meta)
            result["columns"] = columns_meta
            return result
        except sqlite3.Error as e:
            log.error(f"Error getting metadata for table '{table_name}': {e}")
            return None

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Read a user data table into a pandas DataFrame.

        Args:
            table_name: The name of the user data table to read.

        Returns:
            A pandas DataFrame containing the table data.

        Raises:
            ValueError: If the table does not exist physically in the database.
            sqlite3.Error: If there's an issue reading from the database.
        """
        self._validate_connection()

        # Check if table physically exists first
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if not cursor.fetchone():
            raise ValueError(f"Table '{table_name}' not found in the database file.")

        # Optional: Check if it's also in SDIF metadata, log if not
        if table_name not in self.list_tables():
            log.warning(
                f"Table '{table_name}' exists in the database but is not registered in sdif_tables_metadata."
            )

        # Use pandas read_sql for efficient table reading
        # Wrap table name in double quotes for safety
        try:
            # Ensure connection object is passed correctly
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', self.conn)
            return df
        except pd.errors.DatabaseError as e:
            # This might happen for various reasons, e.g., permissions, corruption
            log.error(f"Pandas failed to read table '{table_name}': {e}")
            raise ValueError(
                f"Error reading table '{table_name}' using pandas: {e}"
            ) from e
        except sqlite3.Error as e:  # Catch other potential SQLite errors during read
            log.error(f"SQLite error reading table '{table_name}': {e}")
            raise

    def drop_table(self, table_name: str):
        """Drops a table and its associated metadata."""
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        log.warning(f"Dropping table '{table_name}' and its metadata.")
        try:
            with self.conn:
                # Drop physical table
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                # Delete metadata
                self.conn.execute(
                    "DELETE FROM sdif_columns_metadata WHERE table_name = ?",
                    (table_name,),
                )
                self.conn.execute(
                    "DELETE FROM sdif_tables_metadata WHERE table_name = ?",
                    (table_name,),
                )
                # TODO: Should we delete related semantic links? Requires parsing specs.
        except sqlite3.Error as e:
            log.error(f"Error dropping table '{table_name}': {e}")
            raise

    def list_objects(self) -> List[str]:
        """List the names of all stored JSON objects."""
        self._validate_connection()
        try:
            cursor = self.conn.execute(
                "SELECT object_name FROM sdif_objects ORDER BY object_name"
            )
            return [row["object_name"] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            log.error(f"Error listing objects: {e}")
            return []

    def get_object(
        self, object_name: str, parse_json: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a stored JSON object and its metadata.

        Args:
            object_name: The name of the object to retrieve.
            parse_json: If True (default), parse json_data and schema_hint strings into Python objects.
                        If False, return them as raw strings.

        Returns:
            A dictionary containing the object data and metadata, or None if the object doesn't exist.
            'json_data' and 'schema_hint' keys will contain parsed objects or strings based on parse_json flag.

        Raises:
            ValueError: If parsing fails when parse_json is True.
        """
        self._validate_connection()
        try:
            cursor = self.conn.execute(
                "SELECT * FROM sdif_objects WHERE object_name = ?", (object_name,)
            )
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)

            if parse_json:
                try:
                    result["json_data"] = json.loads(result["json_data"])
                except json.JSONDecodeError as e:
                    log.error(
                        f"Failed to parse JSON data for object '{object_name}': {e}"
                    )
                    raise ValueError(
                        f"Invalid JSON data stored for object '{object_name}'"
                    ) from e

                if result.get("schema_hint"):
                    try:
                        result["schema_hint"] = json.loads(result["schema_hint"])
                    except json.JSONDecodeError as e:
                        log.error(
                            f"Failed to parse schema_hint JSON for object '{object_name}': {e}"
                        )
                        raise ValueError(
                            f"Invalid schema_hint JSON stored for object '{object_name}'"
                        ) from e
            return result
        except sqlite3.Error as e:
            log.error(f"Error retrieving object '{object_name}': {e}")
            return None  # Or re-raise?

    def list_media(self) -> List[str]:
        """List the names of all stored media items."""
        self._validate_connection()
        try:
            cursor = self.conn.execute(
                "SELECT media_name FROM sdif_media ORDER BY media_name"
            )
            return [row["media_name"] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            log.error(f"Error listing media: {e}")
            return []

    def get_media(
        self, media_name: str, parse_json: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve stored media data and its metadata.

        Args:
            media_name: The name of the media item to retrieve.
            parse_json: If True (default), parse technical_metadata string into Python object.
                        If False, return it as a raw string.

        Returns:
            A dictionary containing the media data ('media_data' key as bytes)
            and its metadata, or None if the media item doesn't exist.
            'technical_metadata' key will contain parsed object or string based on parse_json flag.

        Raises:
            ValueError: If parsing fails when parse_json is True.
        """
        self._validate_connection()
        try:
            # Select all columns except the potentially large BLOB initially?
            # No, get all at once is simpler.
            cursor = self.conn.execute(
                "SELECT * FROM sdif_media WHERE media_name = ?", (media_name,)
            )
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)
            # Ensure media_data is bytes (it should be from BLOB)
            if not isinstance(result.get("media_data"), bytes):
                # This case should ideally not happen with SQLite BLOBs from this class
                log.warning(
                    f"media_data for '{media_name}' was not retrieved as bytes (type: {type(result.get('media_data'))})."
                )
                # Attempt conversion? Best to assume it's correct unless proven otherwise.

            if parse_json and result.get("technical_metadata"):
                try:
                    result["technical_metadata"] = json.loads(
                        result["technical_metadata"]
                    )
                except json.JSONDecodeError as e:
                    log.error(
                        f"Failed to parse technical_metadata JSON for media '{media_name}': {e}"
                    )
                    raise ValueError(
                        f"Invalid technical_metadata JSON stored for media '{media_name}'"
                    ) from e
            return result
        except sqlite3.Error as e:
            log.error(f"Error retrieving media '{media_name}': {e}")
            return None  # Or re-raise?

    def list_semantic_links(self, parse_json: bool = True) -> List[Dict[str, Any]]:
        """
        List all semantic links.

        Args:
            parse_json: If True (default), parse from/to_element_spec strings into Python objects.
                        If False, return them as raw strings.

        Returns:
            A list of dictionaries, each representing a semantic link.
            'from_element_spec' and 'to_element_spec' keys will contain parsed objects or strings.

        Raises:
            ValueError: If parsing fails when parse_json is True.
        """
        self._validate_connection()
        links = []
        try:
            cursor = self.conn.execute(
                "SELECT * FROM sdif_semantic_links ORDER BY link_id"
            )
            for row in cursor.fetchall():
                link = dict(row)
                if parse_json:
                    try:
                        link["from_element_spec"] = json.loads(
                            link["from_element_spec"]
                        )
                        link["to_element_spec"] = json.loads(link["to_element_spec"])
                    except json.JSONDecodeError as e:
                        link_id = link.get("link_id", "N/A")
                        log.error(
                            f"Failed to parse element spec JSON for link_id {link_id}: {e}"
                        )
                        raise ValueError(
                            f"Invalid element spec JSON stored for link_id {link_id}"
                        ) from e
                links.append(link)
            return links
        except sqlite3.Error as e:
            log.error(f"Error listing semantic links: {e}")
            return []  # Return empty on error

    def get_schema(self) -> Dict[str, Any]:
        """
        Retrieves the complete structural schema of the SDIF database.

        This includes global properties, sources, detailed table schemas (columns, types,
        constraints, metadata), object metadata (excluding data), media metadata
        (excluding data), and semantic links.

        Returns:
            A dictionary representing the database schema. Structure:
            {
                "sdif_properties": { ... },
                "sources": [ { ... } ],
                "tables": {
                    "table_name": {
                        "metadata": { ... }, // From sdif_tables_metadata
                        "columns": [ { name, sqlite_type, not_null, primary_key, // From PRAGMA
                                        description, original_data_format // From sdif_columns_metadata
                                     }, ... ],
                        "foreign_keys": [ { from_column, target_table, target_column, on_update, on_delete, match }, ... ] // From PRAGMA
                    }, ...
                },
                "objects": {
                    "object_name": { source_id, description, schema_hint }, ... // Parsed schema_hint
                },
                "media": {
                    "media_name": { source_id, media_type, description, original_format, technical_metadata }, ... // Parsed tech meta
                },
                "semantic_links": [ { ... } ] // Parsed specs
            }

        Raises:
            sqlite3.Error: If there are issues querying the database.
            ValueError: If inconsistencies are found (e.g., invalid JSON in metadata).
        """
        self._validate_connection()

        schema_info: Dict[str, Any] = {}

        # 1. Get Global Properties
        schema_info["sdif_properties"] = self.get_properties()

        # 2. Get Sources
        schema_info["sources"] = self.list_sources()

        # 3. Get Tables Schema (Detailed)
        schema_info["tables"] = {}
        table_names = self.list_tables()
        table_column_metadata: Dict[
            str, Dict[str, Dict[str, Any]]
        ] = {}  # Cache column metadata for efficiency

        # Pre-fetch all column metadata
        try:
            cursor = self.conn.execute(
                "SELECT table_name, column_name, description, original_column_name FROM sdif_columns_metadata"
            )
            for row in cursor.fetchall():
                if row["table_name"] not in table_column_metadata:
                    table_column_metadata[row["table_name"]] = {}
                table_column_metadata[row["table_name"]][row["column_name"]] = {
                    "description": row["description"],
                    "original_column_name": row["original_column_name"],
                }
        except sqlite3.Error as e:
            log.error(f"Error pre-fetching column metadata: {e}")
            raise  # Re-raise critical error

        for table_name in table_names:
            table_schema: Dict[str, Any] = {}

            # Get Table Metadata (from sdif_tables_metadata)
            table_meta = self.get_table_metadata(table_name)
            if table_meta:
                # Exclude the redundant 'columns' list from get_table_metadata result
                table_schema["metadata"] = {
                    k: v for k, v in table_meta.items() if k != "columns"
                }
            else:
                # This shouldn't happen if list_tables() is based on the same metadata table
                log.warning(
                    f"Could not retrieve metadata for table '{table_name}' listed in sdif_tables_metadata."
                )
                table_schema["metadata"] = {
                    "table_name": table_name,
                    "error": "Metadata not found",
                }
                # Continue processing other parts if possible

            # Get Column Definitions (from PRAGMA table_info and sdif_columns_metadata)
            table_schema["columns"] = []
            try:
                # Use PRAGMA for actual SQLite schema details
                pragma_cursor = self.conn.execute(f'PRAGMA table_info("{table_name}")')
                pragma_cols = pragma_cursor.fetchall()
                if not pragma_cols:
                    log.warning(
                        f"PRAGMA table_info returned no columns for table '{table_name}', though it exists in metadata."
                    )
                    table_schema["columns"] = [
                        {"error": "Could not retrieve column definitions via PRAGMA"}
                    ]
                else:
                    col_meta_for_table = table_column_metadata.get(table_name, {})
                    for col_row in pragma_cols:
                        col_info = dict(col_row)  # Convert sqlite3.Row to dict
                        # Combine PRAGMA info with SDIF column metadata
                        sdif_col_meta = col_meta_for_table.get(col_info["name"], {})
                        table_schema["columns"].append(
                            {
                                "name": col_info["name"],
                                "sqlite_type": col_info["type"],
                                "not_null": bool(
                                    col_info["notnull"]
                                ),  # PRAGMA uses 0/1
                                "default_value": col_info["dflt_value"],
                                "primary_key": bool(
                                    col_info["pk"] > 0
                                ),  # PK column number (1-based) or 0
                                # Add info from sdif_columns_metadata
                                "description": sdif_col_meta.get("description"),
                                "original_column_name": sdif_col_meta.get(
                                    "original_column_name"
                                ),
                            }
                        )
            except sqlite3.Error as e:
                log.error(f"Error getting PRAGMA table_info for '{table_name}': {e}")
                table_schema["columns"] = [
                    {"error": f"Failed to get PRAGMA table_info: {e}"}
                ]
                # Continue if possible

            # Get Foreign Key Constraints (from PRAGMA foreign_key_list)
            table_schema["foreign_keys"] = []
            try:
                fk_cursor = self.conn.execute(
                    f'PRAGMA foreign_key_list("{table_name}")'
                )
                for fk_row in fk_cursor.fetchall():
                    fk_info = dict(fk_row)
                    table_schema["foreign_keys"].append(
                        {
                            "id": fk_info["id"],  # ID of the FK constraint
                            "seq": fk_info[
                                "seq"
                            ],  # Column sequence number for multi-column FKs
                            "from_column": fk_info["from"],
                            "target_table": fk_info["table"],
                            "target_column": fk_info["to"],
                            "on_update": fk_info["on_update"],
                            "on_delete": fk_info["on_delete"],
                            "match": fk_info["match"],
                        }
                    )
            except sqlite3.Error as e:
                log.error(
                    f"Error getting PRAGMA foreign_key_list for '{table_name}': {e}"
                )
                table_schema["foreign_keys"] = [
                    {"error": f"Failed to get PRAGMA foreign_key_list: {e}"}
                ]
                # Continue if possible

            schema_info["tables"][table_name] = table_schema

        # 4. Get Objects Metadata (excluding data)
        schema_info["objects"] = {}
        object_names = self.list_objects()
        try:
            # Query only needed columns to avoid loading large json_data
            cursor = self.conn.execute(
                "SELECT object_name, source_id, description, schema_hint FROM sdif_objects"
            )
            for row in cursor.fetchall():
                obj_meta = dict(row)
                obj_name = obj_meta["object_name"]
                if obj_name not in object_names:
                    continue  # Should not happen if lists are consistent

                schema_str = obj_meta.get("schema_hint")
                parsed_schema = None
                if schema_str:
                    try:
                        parsed_schema = json.loads(schema_str)
                    except json.JSONDecodeError as e:
                        log.warning(
                            f"Invalid JSON in schema_hint for object '{obj_name}': {e}"
                        )
                        parsed_schema = {"error": f"Invalid JSON: {e}"}
                schema_info["objects"][obj_name] = {
                    "source_id": obj_meta["source_id"],
                    "description": obj_meta["description"],
                    "schema_hint": parsed_schema,  # Store parsed schema or error
                }
        except sqlite3.Error as e:
            log.error(f"Error reading object metadata: {e}")
            schema_info["objects"]["error"] = f"Failed to retrieve object metadata: {e}"

        # 5. Get Media Metadata (excluding data)
        schema_info["media"] = {}
        media_names = self.list_media()
        try:
            # Query only needed columns, excluding media_data BLOB
            cursor = self.conn.execute("""
                SELECT media_name, source_id, media_type, description, original_format, technical_metadata
                FROM sdif_media
            """)
            for row in cursor.fetchall():
                media_meta = dict(row)
                media_name = media_meta["media_name"]
                if media_name not in media_names:
                    continue  # Consistency check

                tech_meta_str = media_meta.get("technical_metadata")
                parsed_tech_meta = None
                if tech_meta_str:
                    try:
                        parsed_tech_meta = json.loads(tech_meta_str)
                    except json.JSONDecodeError as e:
                        log.warning(
                            f"Invalid JSON in technical_metadata for media '{media_name}': {e}"
                        )
                        parsed_tech_meta = {"error": f"Invalid JSON: {e}"}

                schema_info["media"][media_name] = {
                    "source_id": media_meta["source_id"],
                    "media_type": media_meta["media_type"],
                    "description": media_meta["description"],
                    "original_format": media_meta["original_format"],
                    "technical_metadata": parsed_tech_meta,  # Store parsed meta or error
                }
        except sqlite3.Error as e:
            log.error(f"Error reading media metadata: {e}")
            schema_info["media"]["error"] = f"Failed to retrieve media metadata: {e}"

        # 6. Get Semantic Links (parsed)
        try:
            # Use the existing method which handles parsing
            schema_info["semantic_links"] = self.list_semantic_links(parse_json=True)
        except ValueError as e:  # Catch parsing errors from list_semantic_links
            log.error(f"Error parsing semantic link specifications: {e}")
            schema_info["semantic_links"] = [
                {"error": f"Failed to parse link specifications: {e}"}
            ]
            # Optionally retrieve raw strings here as fallback
            # schema_info["semantic_links"] = self.list_semantic_links(parse_json=False)
        except sqlite3.Error as e:
            log.error(f"Error listing semantic links: {e}")
            schema_info["semantic_links"] = [
                {"error": f"Failed to list semantic links: {e}"}
            ]

        return schema_info

    def get_sample_analysis(
        self,
        num_sample_rows: int = 5,
        top_n_common_values: int = 10,
        include_objects: bool = False,  # Option to include basic object info
        include_media: bool = False,  # Option to include basic media info
    ) -> Dict[str, Any]:
        """
        Provides a sample of data and basic statistical analysis for tables
        within the SDIF database, intended to give a better understanding
        of the data content beyond just the schema.

        Args:
            num_sample_rows: The number of random rows to sample from each table.
            top_n_common_values: The number of most frequent distinct values to report per column.
            include_objects: If True, includes a list of object names and descriptions.
            include_media: If True, includes a list of media names and descriptions.

        Returns:
            A dictionary containing samples and analysis. Structure:
            {
                "tables": {
                    "table_name": {
                        "row_count": int, // From metadata
                        "sample_rows": [ {col1: val1, ...}, ... ], // List of sample row dicts
                        "column_analysis": {
                            "column_name": {
                                "sqlite_type": str, // From PRAGMA in get_schema (or re-query)
                                "inferred_type": str, // Pandas inferred type
                                "null_percentage": float,
                                "distinct_count": int,
                                "is_numeric": bool,
                                "is_datetime": bool, # Basic check
                                "most_common_values": [ [value, count], ... ], // Top N
                                "numeric_summary": { // Only if is_numeric
                                    "min": float/int,
                                    "max": float/int,
                                    "mean": float,
                                    "median": float,
                                    "std_dev": float,
                                    "q25": float, # 25th percentile
                                    "q75": float, # 75th percentile
                                }
                            }, ...
                        }
                    }, ...
                },
                "objects": { // Optional, based on include_objects
                     "count": int,
                     "items": [ {"name": str, "description": str, "source_id": int }, ... ]
                 },
                "media": { // Optional, based on include_media
                     "count": int,
                     "items": [ {"name": str, "description": str, "media_type": str, "source_id": int }, ... ]
                 }
            }

        Raises:
            sqlite3.Error: If database querying fails.
            ValueError: If table listed in metadata cannot be read.
            Exception: For unexpected errors during analysis.
        """
        self._validate_connection()
        log.info(
            f"Starting data sampling and analysis (sample_rows={num_sample_rows}, top_n={top_n_common_values})"
        )

        results: Dict[str, Any] = {"tables": {}}
        table_names = self.list_tables()

        # Get schema once to access PRAGMA info efficiently if needed
        # This avoids repeated PRAGMA calls inside the loop
        try:
            full_schema = self.get_schema()
            table_schemas = full_schema.get("tables", {})
        except (sqlite3.Error, ValueError) as e:
            log.error(f"Failed to retrieve base schema for sampling: {e}")
            # Decide whether to proceed without schema or raise error
            # For now, let's raise, as schema context is useful
            raise RuntimeError(
                f"Cannot perform sampling without base schema: {e}"
            ) from e

        for table_name in table_names:
            log.debug(f"Analyzing table: {table_name}")
            table_results: Dict[str, Any] = {
                "sample_rows": [],
                "column_analysis": {},
                "row_count": table_schemas.get(table_name, {})
                .get("metadata", {})
                .get("row_count", 0),  # Get row count from schema
            }

            try:
                # Read the entire table for analysis.
                # Note: For VERY large tables, this could be memory-intensive.
                # Alternatives: SQL-based sampling/stats, chunking.
                # For typical use cases, reading into pandas is feasible and easier.
                df = self.read_table(table_name)

                if df.empty:
                    log.info(f"Table '{table_name}' is empty, skipping analysis.")
                    results["tables"][table_name] = table_results  # Store empty results
                    continue

                # --- Sampling ---
                actual_sample_size = min(num_sample_rows, len(df))
                if actual_sample_size > 0:
                    # Use a fixed random state for reproducibility if desired, or remove for true randomness
                    sample_df = df.sample(n=actual_sample_size, random_state=42)
                    # Convert sample to list of dicts, handling potential NaNs
                    table_results["sample_rows"] = sample_df.replace(
                        {np.nan: None}
                    ).to_dict("records")
                else:
                    table_results["sample_rows"] = []

                # --- Column Analysis ---
                table_schema_cols = {
                    col["name"]: col
                    for col in table_schemas.get(table_name, {}).get("columns", [])
                }

                for col_name in df.columns:
                    col_analysis: Dict[str, Any] = {}
                    col_series = df[col_name]
                    col_schema_info = table_schema_cols.get(col_name, {})

                    # Basic Info
                    col_analysis["sqlite_type"] = col_schema_info.get(
                        "sqlite_type", "N/A"
                    )
                    try:
                        # Infer more specific type if possible
                        col_analysis["inferred_type"] = pd.api.types.infer_dtype(
                            col_series, skipna=True
                        )
                    except Exception:  # Broad catch for safety on inference
                        col_analysis["inferred_type"] = str(
                            col_series.dtype
                        )  # Fallback to numpy dtype

                    # Nulls
                    null_count = col_series.isnull().sum()
                    total_count = len(col_series)
                    col_analysis["null_percentage"] = (
                        round((null_count / total_count) * 100, 2)
                        if total_count > 0
                        else 0
                    )

                    # Distinct Values
                    try:
                        col_analysis["distinct_count"] = col_series.nunique()
                    except TypeError:  # Handle unhashable types if they somehow occur
                        col_analysis[
                            "distinct_count"
                        ] = -1  # Indicate error or inability to count
                        log.warning(
                            f"Could not count distinct values for column '{col_name}' in table '{table_name}' (possibly unhashable type)."
                        )

                    # Most Common Values
                    try:
                        # Use Counter for potentially better performance with many distinct values
                        # value_counts() handles NaN implicitly (drops by default)
                        common_values = col_series.value_counts(dropna=True).head(
                            top_n_common_values
                        )
                        col_analysis["most_common_values"] = [
                            [val, int(count)] for val, count in common_values.items()
                        ]
                    except TypeError:  # Handle unhashable types
                        col_analysis["most_common_values"] = []
                        log.warning(
                            f"Could not get value counts for column '{col_name}' in table '{table_name}' (possibly unhashable type)."
                        )
                    except Exception as e:  # Catch other potential errors
                        col_analysis["most_common_values"] = []
                        log.error(
                            f"Error getting value counts for column '{col_name}' in table '{table_name}': {e}"
                        )

                    # Numeric Analysis
                    col_analysis["is_numeric"] = pd.api.types.is_numeric_dtype(
                        col_series
                    )
                    col_analysis["is_datetime"] = pd.api.types.is_datetime64_any_dtype(
                        col_series
                    ) or col_analysis["inferred_type"] in (
                        "datetime",
                        "date",
                    )  # Basic check

                    if col_analysis["is_numeric"]:
                        # Drop NaNs for describe to work correctly on numeric stats
                        numeric_series = col_series.dropna()
                        if not numeric_series.empty:
                            try:
                                stats = numeric_series.describe()
                                col_analysis["numeric_summary"] = {
                                    "min": float(stats.get("min", np.nan)),
                                    "max": float(stats.get("max", np.nan)),
                                    "mean": float(stats.get("mean", np.nan)),
                                    "median": float(
                                        stats.get("50%", np.nan)
                                    ),  # describe uses '50%' for median
                                    "std_dev": float(stats.get("std", np.nan)),
                                    "q25": float(stats.get("25%", np.nan)),
                                    "q75": float(stats.get("75%", np.nan)),
                                }
                            except Exception as e:
                                log.error(
                                    f"Error calculating numeric summary for column '{col_name}' in table '{table_name}': {e}"
                                )
                                col_analysis["numeric_summary"] = {
                                    "error": f"Calculation failed: {e}"
                                }
                        else:
                            col_analysis["numeric_summary"] = {
                                "note": "Column is numeric but contains only null values."
                            }

                    table_results["column_analysis"][col_name] = col_analysis

            except (ValueError, sqlite3.Error, pd.errors.DatabaseError) as e:
                log.error(f"Failed to read or analyze table '{table_name}': {e}")
                results["tables"][table_name] = {
                    "error": f"Failed to process table: {e}"
                }
                continue  # Move to the next table
            except Exception as e:  # Catch unexpected errors during analysis
                log.exception(
                    f"Unexpected error analyzing table '{table_name}': {e}"
                )  # Use log.exception to include traceback
                results["tables"][table_name] = {
                    "error": f"Unexpected analysis error: {e}"
                }
                continue

            results["tables"][table_name] = table_results

        # --- Object and Media Info (Optional) ---
        if include_objects:
            try:
                objects_list = []
                # Reuse schema info if available and reliable
                if (
                    "objects" in full_schema
                    and isinstance(full_schema["objects"], dict)
                    and "error" not in full_schema["objects"]
                ):
                    objects_list = [
                        {
                            "name": name,
                            "description": meta.get("description"),
                            "source_id": meta.get("source_id"),
                        }
                        for name, meta in full_schema["objects"].items()
                    ]
                else:  # Fallback query if schema had issues or format changed
                    cursor = self.conn.execute(
                        "SELECT object_name, description, source_id FROM sdif_objects ORDER BY object_name"
                    )
                    objects_list = [dict(row) for row in cursor.fetchall()]

                results["objects"] = {"count": len(objects_list), "items": objects_list}
            except Exception as e:
                log.error(f"Failed to retrieve object list for sampling summary: {e}")
                results["objects"] = {"error": f"Failed to retrieve object list: {e}"}

        if include_media:
            try:
                media_list = []
                # Reuse schema info if available and reliable
                if (
                    "media" in full_schema
                    and isinstance(full_schema["media"], dict)
                    and "error" not in full_schema["media"]
                ):
                    media_list = [
                        {
                            "name": name,
                            "description": meta.get("description"),
                            "media_type": meta.get("media_type"),
                            "source_id": meta.get("source_id"),
                        }
                        for name, meta in full_schema["media"].items()
                    ]
                else:  # Fallback query
                    cursor = self.conn.execute(
                        "SELECT media_name, description, media_type, source_id FROM sdif_media ORDER BY media_name"
                    )
                    media_list = [dict(row) for row in cursor.fetchall()]

                results["media"] = {"count": len(media_list), "items": media_list}
            except Exception as e:
                log.error(f"Failed to retrieve media list for sampling summary: {e}")
                results["media"] = {"error": f"Failed to retrieve media list: {e}"}

        log.info("Finished data sampling and analysis.")
        return results

    def query(
        self,
        plain_sql: str,
        return_format: str = "dataframe",
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Executes a read-only SQL query string against the SDIF database.

        This method allows flexible querying using raw SQL SELECT statements.
        It includes checks to prevent modification queries (INSERT, UPDATE, DELETE, etc.)
        and potentially harmful PRAGMA/ATTACH commands, ensuring the database
        state is not altered by the query. This is suitable for use cases
        where an automated agent (like an AI) generates queries for analysis
        or transformation planning, but should not modify the source data.

        Args:
            plain_sql: The raw SQL SELECT query string to execute.
                       No parameter binding is performed; the string is executed as is.
            return_format: The desired format for the results.
                           Options:
                           - "dataframe" (default): Returns a pandas DataFrame.
                           - "dict": Returns a list of dictionaries (one per row).

        Returns:
            The query results in the specified format (pandas DataFrame or list of dicts).

        Raises:
            PermissionError: If the query appears to be non-SELECT or contains
                             disallowed keywords (e.g., UPDATE, PRAGMA, ATTACH, INSERT).
            sqlite3.Error: If any database error occurs during query execution.
            ValueError: If an invalid `return_format` is specified.

        Safety Note:
            This method executes the provided SQL string directly after performing
            keyword checks to prevent modifications. While these checks block common
            modification commands, using the SDIFDatabase in read-only mode
            (`read_only=True`) provides the strongest guarantee against unintended
            data changes at the database level.
        """
        self._validate_connection()

        # --- Security Checks ---
        cleaned_sql = plain_sql.strip()
        lower_sql = cleaned_sql.lower()

        # 1. Check if it starts with SELECT or WITH (for Common Table Expressions)
        #    Allow EXPLAIN as well, as it's read-only and useful for analysis.
        if (
            not lower_sql.startswith("select")
            and not lower_sql.startswith("with")
            and not lower_sql.startswith("explain")
        ):
            raise PermissionError(
                "Query execution failed: Only SELECT, WITH...SELECT, or EXPLAIN queries are allowed."
            )

        # 2. Check for disallowed keywords that modify data or structure, or attach other DBs.
        #    Adding spaces helps prevent accidental matches within identifiers.
        #    Keeping PRAGMA disallowed as it can change connection state (e.g., foreign_keys).
        disallowed_keywords = [
            "insert ",
            "update ",
            "delete ",
            "replace ",
            "drop ",
            "create ",
            "alter ",
            "attach ",
            "detach ",
            "pragma ",  # Still disallow PRAGMA to prevent state changes
            "vacuum ",
            "reindex ",
        ]
        # Perform check on the lowercased query for simplicity
        for keyword in disallowed_keywords:
            # Check for keyword followed by space or end of string/semicolon
            # to be slightly more robust against keywords appearing in names.
            # Example: `keyword ` or `keyword;` or `keyword\n`
            if keyword in lower_sql or lower_sql.endswith(keyword.strip()):
                # Check more carefully: ensure it's not part of a longer word
                # This is still heuristic, not perfect parsing.
                import re

                # Match keyword at word boundary
                pattern = r"\b" + re.escape(keyword.strip()) + r"\b"
                if re.search(pattern, lower_sql):
                    raise PermissionError(
                        f"Query execution failed: Contains disallowed keyword '{keyword.strip()}'."
                    )

        # 3. Log a warning if executing on a writeable connection
        if not self.read_only:
            log.warning(
                "Executing custom query on a writeable database connection. "
                "Ensure the query is safe and does not have unintended side effects. "
                "Using read_only=True is strongly recommended for querying."
            )

        # --- Execution ---
        try:
            # Execute the plain SQL string directly
            cursor = self.conn.execute(plain_sql)

            # --- Format Results ---
            if return_format == "dataframe":
                rows = cursor.fetchall()
                # Get column names from cursor.description even if no rows returned
                if cursor.description:
                    column_names = [
                        description[0] for description in cursor.description
                    ]
                    # Create DataFrame (will be empty with correct columns if no rows)
                    return pd.DataFrame(rows, columns=column_names)
                else:
                    # This can happen with EXPLAIN or potentially other non-row-returning statements
                    # that might pass the initial checks. Return empty DataFrame.
                    log.info(
                        f"Query executed but returned no columns (e.g., EXPLAIN): {cleaned_sql[:100]}..."
                    )
                    return pd.DataFrame()

            elif return_format == "dict":
                # self.conn.row_factory = sqlite3.Row is set in __init__
                # so fetchall() returns Row objects which behave like dicts
                rows = cursor.fetchall()
                if cursor.description:
                    return [dict(row) for row in rows]
                else:
                    # Handle cases like EXPLAIN where there are no columns/rows in the usual sense
                    log.info(
                        f"Query executed but returned no columns (e.g., EXPLAIN): {cleaned_sql[:100]}..."
                    )
                    # Return the raw fetchall result if it contains anything (e.g., EXPLAIN output)
                    # or an empty list otherwise.
                    return rows if rows else []

            else:
                # Invalid return format specified
                raise ValueError(
                    f"Invalid return_format: '{return_format}'. Choose 'dataframe' or 'dict'."
                )

        except sqlite3.Error as e:
            log.error(f"Error executing query:\nSQL: {plain_sql}\nError: {e}")
            # Re-raise the original SQLite error for the caller to handle
            raise

    # TODO: maybe we can simplify by now using the "if_exists" from create_table
    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        source_id: int,
        description: Optional[str] = None,
        original_identifier: Optional[str] = None,
        if_exists: str = "fail",  # Options: 'fail', 'replace', 'append'
        columns_metadata: Optional[Dict[str, Dict[str, Any]]] = None,  # New argument
    ):
        """
        Writes a pandas DataFrame to a new table in the SDIF database.
        Handles table creation, metadata registration, and data insertion.

        Args:
            df: The pandas DataFrame to write.
            table_name: The name for the new table.
            source_id: The ID of the source for this data.
            description: Optional description for the table.
            original_identifier: Optional original identifier for the table (e.g., sheet name).
            if_exists: Behavior if the table already exists ('fail', 'replace', 'append').
            columns_metadata: Optional. A dictionary where keys are final column names
                              and values are dicts like {"description": "...", "original_column_name": "..."}.
                              This metadata is used during table creation.
        """
        self._validate_connection()
        if self.read_only:
            raise PermissionError("Database is open in read-only mode.")
        if table_name.startswith("sdif_"):
            raise ValueError("User data tables must not start with 'sdif_'")

        table_exists = False
        try:
            # Check metadata first for existence
            cursor = self.conn.execute(
                "SELECT 1 FROM sdif_tables_metadata WHERE table_name = ?", (table_name,)
            )
            if cursor.fetchone():
                table_exists = True
                # Verify physical table existence too, log warning if inconsistent
                cursor = self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                )
                if not cursor.fetchone():
                    log.warning(
                        f"Table '{table_name}' exists in SDIF metadata but not physically in the database. Will attempt to recreate."
                    )
                    table_exists = (
                        False  # Treat as if it doesn't exist for creation logic
                    )
            else:  # Not in metadata, ensure it also doesn't exist physically if strict
                cursor = self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                )
                if cursor.fetchone() and if_exists == "fail":
                    raise ValueError(
                        f"Table '{table_name}' exists physically but not in SDIF metadata, and if_exists='fail'. "
                        "Consider 'replace' or manual cleanup."
                    )

        except sqlite3.Error as e:
            log.error(f"Error checking existence of table {table_name}: {e}")
            raise

        if table_exists:
            if if_exists == "fail":
                raise ValueError(
                    f"Table '{table_name}' already exists. Set if_exists='replace' or 'append'."
                )
            elif if_exists == "replace":
                log.warning(f"Replacing existing table '{table_name}'.")
                try:
                    # Use a transaction for dropping and metadata deletion
                    with self.conn:
                        # Drop physical table first
                        self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        # Delete metadata (FKs on sdif_columns_metadata should cascade)
                        # Explicitly delete from sdif_columns_metadata first just in case,
                        # though ON DELETE CASCADE on its FK to sdif_tables_metadata should handle it.
                        self.conn.execute(
                            "DELETE FROM sdif_columns_metadata WHERE table_name = ?",
                            (table_name,),
                        )
                        self.conn.execute(
                            "DELETE FROM sdif_tables_metadata WHERE table_name = ?",
                            (table_name,),
                        )
                        # Any related semantic links are not automatically handled here by default.
                except sqlite3.Error as e:
                    log.error(
                        f"Error dropping existing table '{table_name}' for replacement: {e}"
                    )
                    raise
                table_exists = False  # Reset flag so creation logic proceeds
            elif if_exists == "append":
                log.info(f"Appending data to existing table '{table_name}'.")
                # Schema check for append: Ensure df columns match existing table.
                # This is crucial to prevent errors during insert_data.
                # For simplicity here, we assume column compatibility for append.
                # A more robust implementation would query table_info and compare.
                raise NotImplementedError(
                    "Appending data to existing table is not yet implemented."
                )
            else:
                raise ValueError(
                    f"Invalid value for if_exists: '{if_exists}'. Choose 'fail', 'replace', or 'append'."
                )

        if (
            not table_exists or if_exists == "replace"
        ):  # Create table if it didn't exist or was replaced
            columns_def = {}
            for col_name_str in df.columns:
                # Ensure col_name is a string, as pandas columns can sometimes be other types (e.g., int)
                col_name = str(col_name_str)
                dtype = df[col_name].dtype
                col_type = "TEXT"  # Default type
                if pd.api.types.is_integer_dtype(dtype):
                    col_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = "REAL"
                elif pd.api.types.is_bool_dtype(dtype):
                    # SQLite doesn't have a true BOOLEAN type, store as INTEGER 0 or 1
                    col_type = "INTEGER"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    # Store datetimes as TEXT in ISO 8601 format
                    col_type = "TEXT"
                elif pd.api.types.is_timedelta64_dtype(dtype):
                    # Store timedeltas as TEXT (e.g., "X days HH:MM:SS.ffffff") or REAL (total seconds)
                    col_type = "TEXT"  # Or REAL if preferred
                elif pd.api.types.is_object_dtype(dtype):
                    # Could be mixed, actual strings, or other Python objects.
                    # Forcing to TEXT is safest for general objects.
                    # Consider inspecting a sample if more specific typing is needed.
                    col_type = "TEXT"
                # NUMERIC can be used for columns that might store various numeric forms

                current_col_metadata = (columns_metadata or {}).get(col_name, {})
                columns_def[col_name] = {
                    "type": col_type,
                    "description": current_col_metadata.get(
                        "description"
                    ),  # Use provided or None
                    "original_column_name": current_col_metadata.get(
                        "original_column_name"
                    ),  # Use provided or None
                }

            try:
                self.create_table(
                    table_name,
                    columns_def,
                    source_id,
                    description=description,
                    original_identifier=original_identifier,
                )
            except (sqlite3.Error, ValueError) as e:
                log.error(f"Failed to create table '{table_name}': {e}")
                raise

        # Insert data if DataFrame is not empty
        if not df.empty:
            try:
                # Prepare data for insertion: handle NaT, NaN
                # Convert boolean columns to 0/1 if they exist and are meant for INTEGER
                df_copy = df.copy()
                for col_name in df_copy.columns:
                    if pd.api.types.is_bool_dtype(df_copy[col_name].dtype):
                        df_copy[col_name] = df_copy[col_name].astype(int)
                    elif pd.api.types.is_datetime64_any_dtype(df_copy[col_name].dtype):
                        # Convert datetimes/timestamps to ISO format strings
                        # NaT values will become None due to pd.notnull(NaT) being False
                        df_copy[col_name] = df_copy[col_name].apply(
                            lambda x: x.isoformat() if pd.notnull(x) else None
                        )
                    elif pd.api.types.is_timedelta64_dtype(df_copy[col_name].dtype):
                        # Convert timedelta to string representation
                        df_copy[col_name] = df_copy[col_name].astype(str)

                # Convert to list of dicts, replacing pandas-specific nulls with Python None
                data_list = df_copy.replace({pd.NaT: None, np.nan: None}).to_dict(
                    "records"
                )
                self.insert_data(table_name, cast(List[Dict[str, Any]], data_list))
            except (sqlite3.Error, ValueError) as e:
                log.error(f"Failed to insert data into table '{table_name}': {e}")
                raise
        elif (
            not table_exists or if_exists == "replace"
        ):  # Table was newly created and df is empty
            log.info(
                f"Created empty table '{table_name}' as input DataFrame was empty."
            )
        # If if_exists == 'append' and df is empty, no data is inserted, which is correct.
