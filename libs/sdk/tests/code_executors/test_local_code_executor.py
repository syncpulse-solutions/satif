from pathlib import Path

import pytest
from satif_core.exceptions import CodeExecutionError
from sdif_db.database import SDIFDatabase as ConcreteSDIFDatabase

from satif_sdk.code_executors.local_executor import LocalCodeExecutor

# --- Fixtures ---


@pytest.fixture
def executor() -> LocalCodeExecutor:
    """Returns a LocalCodeExecutor instance with security warnings disabled."""
    return LocalCodeExecutor(disable_security_warning=True)


@pytest.fixture
def executor_with_warning() -> LocalCodeExecutor:
    """Returns a LocalCodeExecutor instance with security warnings enabled."""
    return LocalCodeExecutor(disable_security_warning=False)


@pytest.fixture
def sample_sdif_path(tmp_path: Path) -> Path:
    """Creates a simple SDIF file and returns its path."""
    db_path = tmp_path / "sample.sdif"
    db = ConcreteSDIFDatabase(db_path)
    source_id = db.add_source("test_source.csv", "csv", "Test source data")
    db.create_table(
        "my_data",
        {"id": {"type": "INTEGER"}, "value": {"type": "TEXT"}},
        source_id,
        description="A test table",
    )
    db.insert_data("my_data", [{"id": 1, "value": "alpha"}, {"id": 2, "value": "beta"}])
    db.close()
    return db_path


@pytest.fixture
def another_sample_sdif_path(tmp_path: Path) -> Path:
    """Creates another simple SDIF file for multi-source tests."""
    db_path = tmp_path / "another_sample.sdif"
    db = ConcreteSDIFDatabase(db_path)
    source_id = db.add_source("other_source.txt", "txt", "Other source")
    db.create_table(
        "other_table", {"key": {"type": "TEXT"}, "data": {"type": "INTEGER"}}, source_id
    )
    db.insert_data(
        "other_table", [{"key": "X", "data": 100}, {"key": "Y", "data": 200}]
    )
    db.close()
    return db_path


# --- Test Code Snippets ---

CODE_CONN_ONLY = """
from typing import Dict, Any
import sqlite3

def process_data(conn: sqlite3.Connection) -> Dict[str, Any]:
    cursor = conn.execute("SELECT COUNT(*) FROM db.my_data")
    count = cursor.fetchone()[0]
    conn.commit() # Important for attached DBs if modifications were made
    return {"count": count}
"""

CODE_CONN_CONTEXT = """
from typing import Dict, Any
import sqlite3

def process_data_ctx(conn: sqlite3.Connection, context: Dict[str, Any]) -> Dict[str, Any]:
    prefix = context.get("prefix", "default")
    cursor = conn.execute("SELECT value FROM db.my_data WHERE id = 1")
    value = cursor.fetchone()[0]
    conn.commit()
    return {f"{prefix}_value": value}
"""

CODE_DB_ONLY = """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase # Executor provides this in globals

def process_data_db(db: SDIFDatabase) -> Dict[str, Any]:
    # SDIFDatabase.query returns a DataFrame or List[Dict]
    # For simplicity, let's assume it can execute and fetch directly or use db.conn
    # Using db.conn to mimic previous conn tests but via SDIFDatabase instance
    cursor = db.conn.execute("SELECT COUNT(*) FROM my_data") # No 'db.' prefix needed
    count = cursor.fetchone()[0]
    db.conn.commit()
    return {"db_count": count}
"""

CODE_DB_CONTEXT = """
from typing import Dict, Any
from sdif_db.database import SDIFDatabase

def process_data_db_ctx(db: SDIFDatabase, context: Dict[str, Any]) -> Dict[str, Any]:
    multiplier = context.get("multiplier", 1)
    # Using query method of SDIFDatabase
    result = db.query("SELECT id FROM my_data WHERE value = 'beta'", return_format='dict')
    original_id = result[0]['id']
    # db.conn.commit() # query is read-only, no commit needed unless modifying through db.conn
    return {"multiplied_id": original_id * multiplier}
"""

CODE_GLOBAL_CONTEXT_CHECK = """
from typing import Dict, Any
import sqlite3

test_global_var_value = test_global # Comes from extra_context into globals

def check_global(conn: sqlite3.Connection) -> Dict[str, Any]:
    return {"global_check": test_global_var_value}
"""

CODE_RETURN_NOT_DICT = """
def not_a_dict_return(conn: sqlite3.Connection) -> str:
    return "hello"
"""

CODE_SYNTAX_ERROR = "def func_with_syntax_error(conn: sqlite3.Connection) -> Dict[str, Any]:\n  retun {}"

CODE_RUNTIME_ERROR_INSIDE = """
from typing import Dict, Any
def func_with_runtime_error(conn: sqlite3.Connection) -> Dict[str, Any]:
    a = 1 / 0
    return {"result": a}
"""

CODE_WRONG_SIGNATURE_NO_DB_CONN = """
from typing import Dict, Any
def wrong_sig_no_db_conn(some_other_param: int) -> Dict[str, Any]:
    return {"val": some_other_param}
"""

CODE_WRONG_SIGNATURE_MISSING_REQUIRED = """
from typing import Dict, Any
import sqlite3
def wrong_sig_missing_req(conn: sqlite3.Connection, mandatory_param: str) -> Dict[str, Any]:
    return {"val": mandatory_param}
"""

CODE_CONN_ONLY_NO_PREFIX = """
from typing import Dict, Any
import sqlite3

def process_data_no_prefix(conn: sqlite3.Connection) -> Dict[str, Any]:
    # Querying 'my_data' without any schema prefix, relying on it being in 'main'
    cursor = conn.execute("SELECT COUNT(*) FROM my_data")
    count = cursor.fetchone()[0]
    # conn.commit() # Not strictly necessary for a SELECT, but doesn't hurt
    return {"count_no_prefix": count}
"""

# --- Basic Tests ---


def test_execute_conn_only(executor: LocalCodeExecutor, sample_sdif_path: Path):
    sdif_sources = {"db": sample_sdif_path}
    result = executor.execute(CODE_CONN_ONLY, "process_data", sdif_sources, {})
    assert isinstance(result, dict)
    assert result == {"count": 2}


def test_execute_conn_context(executor: LocalCodeExecutor, sample_sdif_path: Path):
    sdif_sources = {"db": sample_sdif_path}
    extra_context = {"prefix": "test"}
    result = executor.execute(
        CODE_CONN_CONTEXT, "process_data_ctx", sdif_sources, extra_context
    )
    assert result == {"test_value": "alpha"}


def test_execute_db_only(executor: LocalCodeExecutor, sample_sdif_path: Path):
    sdif_sources = {"db": sample_sdif_path}
    result = executor.execute(CODE_DB_ONLY, "process_data_db", sdif_sources, {})
    assert result == {"db_count": 2}


def test_execute_db_context(executor: LocalCodeExecutor, sample_sdif_path: Path):
    sdif_sources = {"db": sample_sdif_path}
    extra_context = {"multiplier": 3}
    result = executor.execute(
        CODE_DB_CONTEXT, "process_data_db_ctx", sdif_sources, extra_context
    )
    assert result == {"multiplied_id": 6}  # id for 'beta' is 2, 2*3=6


def test_global_context_injection(executor: LocalCodeExecutor, sample_sdif_path: Path):
    sdif_sources = {"db": sample_sdif_path}
    extra_context = {"test_global": "hello_global"}
    result = executor.execute(
        CODE_GLOBAL_CONTEXT_CHECK, "check_global", sdif_sources, extra_context
    )
    assert result == {"global_check": "hello_global"}


def test_execute_multiple_sources_conn(
    executor: LocalCodeExecutor, sample_sdif_path: Path, another_sample_sdif_path: Path
):
    sdif_sources = {"db1": sample_sdif_path, "aux_db": another_sample_sdif_path}
    code = """
from typing import Dict, Any
import sqlite3
def process_multi(conn: sqlite3.Connection) -> Dict[str, Any]:
    c1 = conn.execute("SELECT COUNT(*) FROM db1.my_data").fetchone()[0]
    c2 = conn.execute("SELECT COUNT(*) FROM aux_db.other_table").fetchone()[0]
    conn.commit()
    return {"db1_count": c1, "aux_db_count": c2}
"""
    result = executor.execute(code, "process_multi", sdif_sources, {})
    assert result == {"db1_count": 2, "aux_db_count": 2}


def test_execute_conn_only_no_prefix(
    executor: LocalCodeExecutor, sample_sdif_path: Path
):
    sdif_sources = {"arbitrary_schema_for_attach": sample_sdif_path}
    result = executor.execute(
        CODE_CONN_ONLY_NO_PREFIX, "process_data_no_prefix", sdif_sources, {}
    )
    assert isinstance(result, dict)
    assert result == {"count_no_prefix": 2}


# --- Error Handling Tests ---


def test_error_function_not_found(executor: LocalCodeExecutor, sample_sdif_path: Path):
    with pytest.raises(
        CodeExecutionError, match="Function 'non_existent_func' not found"
    ):
        executor.execute(
            CODE_CONN_ONLY, "non_existent_func", {"db": sample_sdif_path}, {}
        )


def test_error_not_callable(executor: LocalCodeExecutor, sample_sdif_path: Path):
    code = "my_var = 123"
    with pytest.raises(
        CodeExecutionError, match="'my_var' defined in code is not a callable function"
    ):
        executor.execute(code, "my_var", {"db": sample_sdif_path}, {})


def test_error_return_not_dict(executor: LocalCodeExecutor, sample_sdif_path: Path):
    with pytest.raises(
        CodeExecutionError, match="must return a Dict. Got <class 'str'>"
    ):
        executor.execute(
            CODE_RETURN_NOT_DICT, "not_a_dict_return", {"db": sample_sdif_path}, {}
        )


def test_error_syntax_error_in_code(
    executor: LocalCodeExecutor, sample_sdif_path: Path
):
    with pytest.raises(
        CodeExecutionError
    ) as excinfo:  # Check for wrapped original error
        executor.execute(
            CODE_SYNTAX_ERROR, "func_with_syntax_error", {"db": sample_sdif_path}, {}
        )
    assert isinstance(excinfo.value.__cause__, SyntaxError)


def test_error_runtime_error_inside_func(
    executor: LocalCodeExecutor, sample_sdif_path: Path
):
    with pytest.raises(CodeExecutionError) as excinfo:
        executor.execute(
            CODE_RUNTIME_ERROR_INSIDE,
            "func_with_runtime_error",
            {"db": sample_sdif_path},
            {},
        )
    assert isinstance(excinfo.value.__cause__, ZeroDivisionError)


def test_error_wrong_signature_no_db_conn(
    executor: LocalCodeExecutor, sample_sdif_path: Path
):
    with pytest.raises(
        CodeExecutionError,
        match=r"Transformation function 'wrong_sig_no_db_conn's first required parameter must be 'db' or 'conn' if other arguments are expected. Got 'some_other_param'. Signature: \(some_other_param: int\) -> Dict\[str, Any\]",
    ):
        executor.execute(
            CODE_WRONG_SIGNATURE_NO_DB_CONN,
            "wrong_sig_no_db_conn",
            {"db": sample_sdif_path},
            {},
        )


def test_error_wrong_signature_missing_required(
    executor: LocalCodeExecutor, sample_sdif_path: Path
):
    with pytest.raises(
        CodeExecutionError, match="is missing required argument 'mandatory_param'"
    ):
        executor.execute(
            CODE_WRONG_SIGNATURE_MISSING_REQUIRED,
            "wrong_sig_missing_req",
            {"db": sample_sdif_path},
            {},
        )


def test_error_db_param_multiple_sources(
    executor: LocalCodeExecutor, sample_sdif_path: Path, another_sample_sdif_path: Path
):
    sdif_sources = {"s1": sample_sdif_path, "s2": another_sample_sdif_path}
    with pytest.raises(
        CodeExecutionError, match="'db' parameter requires exactly one SDIF source file"
    ):
        executor.execute(CODE_DB_ONLY, "process_data_db", sdif_sources, {})
