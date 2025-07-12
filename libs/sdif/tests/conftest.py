from pathlib import Path

import pytest

from sdif_db.database import SDIFDatabase

# --- Fixtures ---


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Provides a path for a temporary database file."""
    return tmp_path / "test_sdif.db"


@pytest.fixture
def empty_db(tmp_db_path: Path) -> SDIFDatabase:
    """Provides a new, empty SDIFDatabase instance, closed after test."""
    db = SDIFDatabase(tmp_db_path)
    yield db
    db.close()


@pytest.fixture
def readonly_db(tmp_db_path: Path) -> SDIFDatabase:
    """Provides a read-only SDIFDatabase instance, closed after test."""
    # Create a dummy DB file first
    SDIFDatabase(tmp_db_path).close()

    db = SDIFDatabase(tmp_db_path, read_only=True)
    yield db
    db.close()


@pytest.fixture
def db_with_source(empty_db: SDIFDatabase) -> tuple[SDIFDatabase, int]:
    """Provides an SDIFDatabase with one source added, and the source_id."""
    source_id = empty_db.add_source("test_file.csv", "csv", "Test source")
    return empty_db, source_id


@pytest.fixture
def db_with_simple_table(
    db_with_source: tuple[SDIFDatabase, int],
) -> tuple[SDIFDatabase, int, str]:
    """Provides an SDIFDatabase with a source and a simple table added."""
    db, source_id = db_with_source
    table_name = "simple_test_table"
    columns = {
        "id": {"type": "INTEGER", "primary_key": True},
        "name": {"type": "TEXT", "not_null": True},
        "value": {"type": "REAL"},
    }
    db.create_table(table_name, columns, source_id, description="A simple test table")
    return db, source_id, table_name
