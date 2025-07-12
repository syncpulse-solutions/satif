import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from sdif_db.utils import (
    DBConnectionError,
    cleanup_db_connection,
    create_db_connection,
)


@pytest.fixture
def temp_sqlite_file(request) -> Path:
    """Creates a temporary minimal SQLite file for testing."""
    # Suffix is added to allow multiple instances of this fixture if needed directly
    # though typically pytest handles one instance per test function needing it.
    suffix = getattr(request, "param", "")
    if suffix and not suffix.startswith("."):
        suffix = f"_{suffix}"

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=f"{suffix}.sqlite", prefix="sdif_test_"
    ) as tmp_file:
        file_path = Path(tmp_file.name)

    # Create a minimal SQLite database at this path
    conn = sqlite3.connect(file_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dummy_table (id INTEGER PRIMARY KEY, data TEXT);"
    )
    conn.execute("INSERT INTO dummy_table (data) VALUES ('test_data');")
    conn.commit()
    conn.close()

    yield file_path

    # Cleanup: remove the file after the test
    if file_path.exists():
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error removing temp file {file_path}: {e}")  # Or use logger


@pytest.fixture
def temp_sqlite_files(request):
    """Creates a specified number of temporary minimal SQLite files."""
    count = getattr(request, "param", 2)  # Default to 2 files if not specified
    files = []
    temp_file_objects = []  # To keep tempfile objects alive

    for i in range(count):
        tmp_file = tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_multi_{i}.sqlite", prefix="sdif_test_"
        )
        temp_file_objects.append(tmp_file)
        file_path = Path(tmp_file.name)

        conn = sqlite3.connect(file_path)
        conn.execute(f"CREATE TABLE IF NOT EXISTS t{i} (id INTEGER);")
        conn.commit()
        conn.close()
        files.append(file_path)

    yield files

    for file_path in files:
        if file_path.exists():
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error removing temp file {file_path}: {e}")


# --- Tests for create_db_connection ---


def test_create_db_connection_single_source(temp_sqlite_file):
    sdif_sources = {"my_db": temp_sqlite_file.resolve()}
    conn, attached_schemas = create_db_connection(sdif_sources)
    conn.row_factory = sqlite3.Row

    assert isinstance(conn, sqlite3.Connection)
    assert attached_schemas == {"my_db": temp_sqlite_file.resolve()}

    # Verify that the main connection is to the file itself
    cursor = conn.execute("PRAGMA database_list;")
    databases = {
        row["name"]: Path(row["file"]).resolve() if row["file"] else ""
        for row in cursor.fetchall()
    }
    assert databases["main"] == temp_sqlite_file.resolve()
    assert databases["my_db"] == temp_sqlite_file.resolve()

    # Check if dummy table is accessible via schema name
    try:
        cursor = conn.execute("SELECT data FROM my_db.dummy_table WHERE id = 1;")
        row = cursor.fetchone()
        assert row["data"] == "test_data"
    except sqlite3.OperationalError as e:
        pytest.fail(f"Failed to query attached schema: {e}")

    cleanup_db_connection(conn, attached_schemas)  # Also tests basic cleanup


@pytest.mark.parametrize("temp_sqlite_files", [2], indirect=True)
def test_create_db_connection_multiple_sources(temp_sqlite_files):
    file1, file2 = temp_sqlite_files
    file1_resolved = file1.resolve()
    file2_resolved = file2.resolve()
    sdif_sources = {"db_one": file1_resolved, "db_two": file2_resolved}
    conn, attached_schemas = create_db_connection(sdif_sources)
    conn.row_factory = sqlite3.Row

    assert isinstance(conn, sqlite3.Connection)
    assert attached_schemas == {"db_one": file1_resolved, "db_two": file2_resolved}

    cursor = conn.execute("PRAGMA database_list;")
    databases = {
        row["name"]: Path(row["file"]).resolve() if row["file"] else ""
        for row in cursor.fetchall()
    }

    assert databases["main"] == ""  # In-memory for multiple sources
    assert databases["db_one"] == file1_resolved
    assert databases["db_two"] == file2_resolved

    try:
        conn.execute("SELECT * FROM db_one.t0;").fetchall()
        conn.execute("SELECT * FROM db_two.t1;").fetchall()
    except sqlite3.OperationalError as e:
        pytest.fail(f"Failed to query attached schemas: {e}")

    cleanup_db_connection(conn, attached_schemas)

    if temp_sqlite_files[0].exists():
        reopened_conn = sqlite3.connect(temp_sqlite_files[0])
        reopened_conn.row_factory = sqlite3.Row
        attached_dbs_after_cleanup = {
            row["name"]
            for row in reopened_conn.execute("PRAGMA database_list;").fetchall()
        }
        assert "db_one" not in attached_dbs_after_cleanup
        assert "db_two" not in attached_dbs_after_cleanup
        reopened_conn.close()


def test_create_db_connection_no_sources():
    with pytest.raises(DBConnectionError, match="No SDIF sources provided"):
        create_db_connection({})


def test_create_db_connection_file_not_found():
    non_existent_path = Path(tempfile.gettempdir()) / "non_existent_sdif_db.sqlite"
    assert not non_existent_path.exists()  # Ensure it doesn't exist
    with pytest.raises(
        DBConnectionError,
        match=f"Input SDIF file for schema 's1' not found or is not a file: {non_existent_path}",
    ):
        create_db_connection({"s1": non_existent_path})


def test_create_db_connection_path_is_directory():
    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_path = Path(tmp_dir)
        with pytest.raises(
            DBConnectionError,
            match=f"Input SDIF file for schema 's1' not found or is not a file: {dir_path}",
        ):
            create_db_connection({"s1": dir_path})


# --- Tests for cleanup_db_connection ---


def test_cleanup_db_connection_closes_and_detaches(temp_sqlite_file):
    conn, attached = create_db_connection({"test_db": temp_sqlite_file})
    conn.row_factory = sqlite3.Row
    assert "test_db" in {
        row["name"] for row in conn.execute("PRAGMA database_list;").fetchall()
    }

    cleanup_db_connection(conn, attached, should_close=True)

    # Check if connection is closed
    with pytest.raises(
        sqlite3.ProgrammingError, match="Cannot operate on a closed database."
    ):
        conn.execute("SELECT 1;")

    # If it was a file DB, reconnect and check if schema is detached
    # (This is a bit indirect for in-memory, but good for file-based single source)
    if temp_sqlite_file.exists():
        reopened_conn = sqlite3.connect(temp_sqlite_file)
        reopened_conn.row_factory = sqlite3.Row
        attached_dbs_after_cleanup = {
            row["name"]
            for row in reopened_conn.execute("PRAGMA database_list;").fetchall()
        }
        assert "test_db" not in attached_dbs_after_cleanup
        reopened_conn.close()


@pytest.mark.parametrize("temp_sqlite_files", [1], indirect=True)
def test_cleanup_db_connection_no_close_detaches(temp_sqlite_files):
    the_file = temp_sqlite_files[0]
    # For this test, force a single file scenario to ensure 'main' and schema attach
    conn, attached = create_db_connection({"my_schema": the_file.resolve()})
    conn.row_factory = sqlite3.Row

    # Check schema is attached
    initial_databases = {
        row["name"]: Path(row["file"]).resolve() if row["file"] else ""
        for row in conn.execute("PRAGMA database_list;").fetchall()
    }
    assert "my_schema" in initial_databases
    assert initial_databases["my_schema"] == the_file.resolve()

    cleanup_db_connection(conn, attached, should_close=False)

    # Check connection is still open
    try:
        conn.execute("SELECT 1;").fetchone()
    except sqlite3.ProgrammingError:
        pytest.fail("Connection was closed when should_close=False")

    # Check schema is detached
    databases_after_detach = {
        row["name"]: Path(row["file"]).resolve() if row["file"] else ""
        for row in conn.execute("PRAGMA database_list;").fetchall()
    }
    assert "my_schema" not in databases_after_detach
    # The 'main' database should still be the file itself, as it wasn't an in-memory scenario
    assert databases_after_detach["main"] == the_file.resolve()

    conn.close()  # Manual cleanup


def test_cleanup_db_connection_conn_is_none():
    cleanup_db_connection(None, {"s1": Path("dummy.sqlite")})
    # No assertion needed, just ensure no error is raised


def test_cleanup_db_connection_empty_attached_schemas():
    # Create a simple in-memory connection directly for this test
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INT);")  # Make sure it's a valid connection

    cleanup_db_connection(conn, {}, should_close=True)

    with pytest.raises(
        sqlite3.ProgrammingError, match="Cannot operate on a closed database."
    ):
        conn.execute("SELECT 1;")


def test_cleanup_db_connection_handles_detach_error_gracefully(
    mocker, temp_sqlite_file, caplog
):
    # The schema name we expect to cause a detach error
    error_schema_name = "error_db"
    sdif_sources = {error_schema_name: temp_sqlite_file.resolve()}

    # Mock sqlite3.connect to return a connection whose execute method will fail on DETACH
    mock_conn_instance = mocker.MagicMock(spec=sqlite3.Connection)

    def execute_for_detach_error(sql, params=None):
        if isinstance(sql, str) and sql.upper().startswith(
            f"DETACH DATABASE {error_schema_name.upper()}"
        ):
            raise sqlite3.OperationalError(
                f"Simulated detach error for {error_schema_name}"
            )
        # For ATTACH and other operations during create_db_connection, let them pass
        # by returning a mock cursor or another MagicMock for cursor operations.
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        return mock_cursor

    mock_conn_instance.execute.side_effect = execute_for_detach_error
    # Mock close to do nothing to prevent errors if it's called on the mock before test ends
    mock_conn_instance.close.return_value = None

    # Patch sqlite3.connect globally to return our controlled mock connection
    # This needs to be active when create_db_connection is called.
    mocker.patch("sqlite3.connect", return_value=mock_conn_instance)

    # Call the function under test
    # create_db_connection will now use our mock_conn_instance
    conn_returned, attached_schemas = create_db_connection(sdif_sources)

    # Ensure the connection returned IS our mock, so its execute is the one we patched
    assert conn_returned is mock_conn_instance

    with caplog.at_level("ERROR"):
        cleanup_db_connection(conn_returned, attached_schemas, should_close=True)

    assert any(
        f"Error detaching database '{error_schema_name}'" in record.message
        for record in caplog.records
    )
    assert any(
        f"Simulated detach error for {error_schema_name}" in record.message
        for record in caplog.records
    )

    # Check that conn_returned.close() was called by cleanup_db_connection
    mock_conn_instance.close.assert_called_once()


@pytest.mark.parametrize("temp_sqlite_files", [1], indirect=True)
def test_create_db_connection_attach_failure_single_source(temp_sqlite_files, mocker):
    file1 = temp_sqlite_files[0].resolve()
    schema_name_to_fail = "db1"
    sdif_sources = {schema_name_to_fail: file1}

    mock_conn_instance = mocker.MagicMock(spec=sqlite3.Connection)

    def execute_for_single_attach_fail(sql, params=None):
        if isinstance(sql, str) and sql.startswith(
            f"ATTACH DATABASE ? AS {schema_name_to_fail}"
        ):
            raise sqlite3.OperationalError(
                f"Simulated ATTACH error for {schema_name_to_fail} single source"
            )
        # Allow other execute calls (e.g., PRAGMAs if any during connect)
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = []  # e.g. for PRAGMA database_list
        return mock_cursor

    mock_conn_instance.execute.side_effect = execute_for_single_attach_fail
    mock_conn_instance.close.return_value = None  # Prevent errors on close

    mocker.patch("sqlite3.connect", return_value=mock_conn_instance)

    with pytest.raises(
        DBConnectionError,
        match=f"Failed to attach single database '{str(file1)}' as schema '{schema_name_to_fail}': Simulated ATTACH error for {schema_name_to_fail} single source",
    ):
        create_db_connection(sdif_sources)

    mock_conn_instance.close.assert_called_once()  # Ensure connection was closed during error handling


@pytest.mark.parametrize("temp_sqlite_files", [2], indirect=True)
def test_create_db_connection_attach_failure_multi_source(temp_sqlite_files, mocker):
    file1_resolved = temp_sqlite_files[0].resolve()

    schema_to_succeed = "db1"
    schema_to_fail = "db2"

    mock_path_db2 = mocker.MagicMock(spec=Path)
    mock_path_db2.exists.return_value = True
    mock_path_db2.is_file.return_value = True
    mock_path_db2.resolve.return_value = mock_path_db2
    mock_path_db2.__str__.return_value = "mocked/db2.sqlite"

    sdif_sources_mocked = {
        schema_to_succeed: file1_resolved,
        schema_to_fail: mock_path_db2,
    }

    mock_conn_instance = mocker.MagicMock(spec=sqlite3.Connection)
    attached_successfully = {}

    def execute_for_multi_attach_fail(sql, params=None):
        nonlocal attached_successfully
        # Let the first ATTACH (for db1) succeed by returning a mock cursor
        if isinstance(sql, str) and sql.startswith(
            f"ATTACH DATABASE ? AS {schema_to_succeed}"
        ):
            # Simulate successful attach for the first one
            attached_successfully[schema_to_succeed] = params[
                0
            ]  # Record path for assertion if needed
            mock_cursor = mocker.MagicMock()
            return mock_cursor
        # Make the second ATTACH (for db2) fail
        if isinstance(sql, str) and sql.startswith(
            f"ATTACH DATABASE ? AS {schema_to_fail}"
        ):
            raise sqlite3.OperationalError(
                f"Simulated ATTACH error for {schema_to_fail} multi-source"
            )
        # Default for other calls (e.g. initial :memory: connection might do pragmas)
        mock_cursor = mocker.MagicMock()
        return mock_cursor

    mock_conn_instance.execute.side_effect = execute_for_multi_attach_fail
    mock_conn_instance.close.return_value = None  # To prevent errors on close

    mocker.patch("sqlite3.connect", return_value=mock_conn_instance)

    with pytest.raises(
        DBConnectionError,
        match=f"Failed to attach database 'mocked/db2.sqlite' as schema '{schema_to_fail}': Simulated ATTACH error for {schema_to_fail} multi-source",
    ):
        create_db_connection(sdif_sources_mocked)

    # Ensure the failing ATTACH was attempted
    # Check calls to mock_conn_instance.execute
    attach_db1_call = mocker.call(
        f"ATTACH DATABASE ? AS {schema_to_succeed}", (str(file1_resolved),)
    )
    attach_db2_call = mocker.call(
        f"ATTACH DATABASE ? AS {schema_to_fail}", (str(mock_path_db2),)
    )

    mock_conn_instance.execute.assert_any_call(
        *attach_db1_call[1], **attach_db1_call[2]
    )
    mock_conn_instance.execute.assert_any_call(
        *attach_db2_call[1], **attach_db2_call[2]
    )

    # Ensure close was called on the mock connection during error handling
    mock_conn_instance.close.assert_called_once()
