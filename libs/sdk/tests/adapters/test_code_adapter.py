from pathlib import Path
from typing import Any, Callable, Dict

import pytest
from sdif_db.database import SDIFDatabase

from satif_sdk.adapters.code import AdapterError, CodeAdapter

# --- Fixtures ---


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Provides a path for a temporary database file."""
    return tmp_path / "test_sdif.db"


@pytest.fixture
def sample_db(tmp_db_path: Path) -> Path:
    """Creates a sample database with a simple table."""
    db = SDIFDatabase(tmp_db_path)

    # Create a simple table with test data
    table_name = "test_table"
    columns = {
        "id": {"type": "INTEGER", "primary_key": True},
        "name": {"type": "TEXT", "not_null": True},
        "value": {"type": "REAL"},
    }
    source_id = db.add_source("test_source.csv", "csv", "Test source")
    db.create_table(table_name, columns, source_id, description="Test table")

    # Insert some test data
    db.insert_data(
        table_name,
        [
            {"id": 1, "name": "test1", "value": 10.5},
            {"id": 2, "name": "test2", "value": 20.5},
        ],
    )

    db.close()  # Close the DB connection
    return tmp_db_path


@pytest.fixture
def simple_adapt_function() -> Callable:
    """Returns a simple adaptation function that adds a column."""

    def adapt(db: SDIFDatabase) -> None:
        db.conn.execute("ALTER TABLE test_table ADD COLUMN new_col TEXT")
        db.conn.execute("UPDATE test_table SET new_col = 'added'")
        db.conn.commit()

    return adapt


@pytest.fixture
def adapt_function_with_context() -> Callable:
    """Returns an adaptation function that uses context values."""

    def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:
        value = context.get("value", "default")
        db.conn.execute("ALTER TABLE test_table ADD COLUMN context_col TEXT")
        db.conn.execute(f"UPDATE test_table SET context_col = '{value}'")
        db.conn.commit()

    return adapt


@pytest.fixture
def adapt_code_string() -> str:
    """Returns a string containing adaptation code."""
    return """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase
import sqlite3 # Required for db.conn

def adapt(db: SDIFDatabase) -> Dict[str, Any]:
    db.conn.execute("ALTER TABLE test_table ADD COLUMN from_string TEXT")
    db.conn.execute("UPDATE test_table SET from_string = 'string_code'")
    db.conn.commit()
    return {}
"""


@pytest.fixture
def adapt_code_with_context_string() -> str:
    """Returns a string containing adaptation code that uses context."""
    return """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase
import sqlite3 # Required for db.conn

def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> Dict[str, Any]:
    value = context.get('value', 'default')
    db.conn.execute("ALTER TABLE test_table ADD COLUMN context_from_string TEXT")
    db.conn.execute(f"UPDATE test_table SET context_from_string = '{value}'")
    db.conn.commit()
    return {}
"""


@pytest.fixture
def adapt_code_db_param_string() -> str:
    """Returns a string containing adaptation code that uses db: SDIFDatabase."""
    return """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase
import sqlite3 # Required for db.conn

def adapt(db: SDIFDatabase) -> Dict[str, Any]:
    db.conn.execute("ALTER TABLE test_table ADD COLUMN from_db_param TEXT")
    db.conn.execute("UPDATE test_table SET from_db_param = 'db_param_code'")
    db.conn.commit()
    return {}
"""


@pytest.fixture
def adapt_code_db_param_with_context_string() -> str:
    """Returns a string containing adaptation code that uses db: SDIFDatabase and context."""
    return """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase
import sqlite3 # Required for db.conn

def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> Dict[str, Any]:
    value = context.get('db_value', 'db_default')
    db.conn.execute("ALTER TABLE test_table ADD COLUMN context_from_db_param TEXT")
    db.conn.execute(f"UPDATE test_table SET context_from_db_param = '{value}'")
    db.conn.commit()
    return {}
"""


@pytest.fixture
def code_file_path(tmp_path: Path, adapt_code_string: str) -> Path:
    """Creates a temporary file with adaptation code."""
    file_path = tmp_path / "adapt_script.py"
    with open(file_path, "w") as f:
        f.write(adapt_code_string)
    return file_path


# --- Tests ---


def test_direct_callable(sample_db: Path, simple_adapt_function: Callable):
    """Test adapter with a direct callable function."""
    adapter = CodeAdapter(simple_adapt_function)

    # Verify function name is set correctly
    assert adapter.function_name == "adapt"

    # Execute adaptation
    output_path = adapter.adapt(sample_db)

    # Verify output path and existence
    assert output_path.exists()
    assert output_path.name == f"{sample_db.stem}_adapted{sample_db.suffix}"

    # Check if the adaptation worked by opening the result
    with SDIFDatabase(output_path) as db:
        # Check if the new column exists and has the expected value
        result = db.query(
            "SELECT new_col FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["new_col"] == "added"


def test_direct_callable_with_context(
    sample_db: Path, adapt_function_with_context: Callable
):
    """Test adapter with a direct callable that uses context."""
    context_value = "context_test_value"
    adapter = CodeAdapter(
        adapt_function_with_context, extra_context={"value": context_value}
    )

    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT context_col FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["context_col"] == context_value


def test_code_string(sample_db: Path, adapt_code_string: str):
    """Test adapter with a code string."""
    adapter = CodeAdapter(adapt_code_string, disable_security_warning=True)
    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT from_string FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["from_string"] == "string_code"


def test_code_string_with_context(sample_db: Path, adapt_code_with_context_string: str):
    """Test adapter with a code string that uses context."""
    context_value = "context_string_value"
    adapter = CodeAdapter(
        adapt_code_with_context_string,
        extra_context={"value": context_value},
        disable_security_warning=True,
    )

    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT context_from_string FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["context_from_string"] == context_value


def test_code_file(sample_db: Path, code_file_path: Path):
    """Test adapter with a path to a code file."""
    adapter = CodeAdapter(code_file_path, disable_security_warning=True)
    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT from_string FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["from_string"] == "string_code"


def test_custom_function_name(sample_db: Path):
    """Test adapter with a custom function name."""
    code = """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase # Import SDIFDatabase
import sqlite3 # Required for db.conn

def custom_adapt(db: SDIFDatabase) -> Dict[str, Any]: # Changed to db: SDIFDatabase
    db.conn.execute("ALTER TABLE test_table ADD COLUMN custom_col TEXT")
    db.conn.execute("UPDATE test_table SET custom_col = 'custom'")
    db.conn.commit() # Ensure changes are committed
    return {}
"""
    adapter = CodeAdapter(
        code, function_name="custom_adapt", disable_security_warning=True
    )
    output_path = adapter.adapt(sample_db)
    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT custom_col FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["custom_col"] == "custom"


def test_code_string_db_param(sample_db: Path, adapt_code_db_param_string: str):
    """Test adapter with a code string using db: SDIFDatabase parameter."""
    adapter = CodeAdapter(adapt_code_db_param_string, disable_security_warning=True)
    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT from_db_param FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["from_db_param"] == "db_param_code"


def test_code_string_db_param_with_context(
    sample_db: Path, adapt_code_db_param_with_context_string: str
):
    """Test adapter with a code string using db: SDIFDatabase and context."""
    context_value = "db_context_test_value"
    adapter = CodeAdapter(
        adapt_code_db_param_with_context_string,
        extra_context={"db_value": context_value},
        disable_security_warning=True,
    )
    output_path = adapter.adapt(sample_db)

    with SDIFDatabase(output_path) as db:
        result = db.query(
            "SELECT context_from_db_param FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
        assert result[0]["context_from_db_param"] == context_value


def test_custom_output_suffix(sample_db: Path, simple_adapt_function: Callable):
    """Test adapter with a custom output suffix."""
    custom_suffix = "_custom_suffix"
    adapter = CodeAdapter(simple_adapt_function, output_suffix=custom_suffix)
    output_path = adapter.adapt(sample_db)

    assert output_path.exists()
    assert output_path.name == f"{sample_db.stem}{custom_suffix}{sample_db.suffix}"


def test_input_file_not_found():
    """Test adapter with a nonexistent input file."""
    adapter = CodeAdapter(lambda db: None)

    with pytest.raises(FileNotFoundError):
        adapter.adapt("nonexistent_file.db")


def test_invalid_function_signature(sample_db: Path):
    """Test adapter with an invalid function signature."""

    def invalid_adapt(invalid_param: str) -> None:
        pass

    adapter = CodeAdapter(invalid_adapt)

    with pytest.raises(AdapterError, match="must accept 'db' as its first parameter"):
        adapter.adapt(sample_db)


def test_function_with_return_value(sample_db: Path, caplog):
    """Test adapter with a function that returns a value (should warn)."""

    def adapt_with_return(db: SDIFDatabase) -> str:
        db.conn.execute("ALTER TABLE test_table ADD COLUMN test_col TEXT")
        db.conn.commit()
        return "This should trigger a warning"

    adapter = CodeAdapter(adapt_with_return)
    adapter.adapt(sample_db)

    assert "returned a value" in caplog.text
    assert (
        "should modify the SDIFDatabase instance in place and return None"
        in caplog.text
    )


def test_invalid_input_type():
    """Test adapter with an invalid input type."""
    with pytest.raises(TypeError, match="must be a callable, a string"):
        CodeAdapter(123)  # type: ignore


def test_overwrite_existing_output(
    sample_db: Path, simple_adapt_function: Callable, tmp_path: Path
):
    """Test overwriting an existing output file."""
    adapter = CodeAdapter(simple_adapt_function)

    # Create a dummy file at the expected output path
    output_filename = f"{sample_db.stem}_adapted{sample_db.suffix}"
    output_path = sample_db.parent / output_filename
    with open(output_path, "w") as f:
        f.write("dummy content")

    # Execute adaptation (should overwrite)
    adapter.adapt(sample_db)

    # Verify the file was overwritten
    with SDIFDatabase(output_path) as db:
        # If we can open it as SDIF, it was overwritten
        result = db.query(
            "SELECT new_col FROM test_table LIMIT 1", return_format="dict"
        )
        assert result
