from pathlib import Path
from typing import Any, Dict

import pytest

from satif_core.code_executors.base import CodeExecutor


class SimpleCodeExecutor(CodeExecutor):
    """Simple concrete implementation of the CodeExecutor abstract class."""

    def execute(
        self,
        code: str,
        function_name: str,
        sdif_sources: Dict[str, Path],
        extra_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Simple implementation that mocks code execution.
        """
        # Validate inputs
        assert isinstance(code, str)
        assert isinstance(function_name, str)
        assert isinstance(sdif_sources, dict)
        assert isinstance(extra_context, dict)

        # Return mock execution result
        return {
            "output.csv": "mock_dataframe_content",
            "summary.json": {
                "source_schemas": list(sdif_sources.keys()),
                "row_count": 10,
            },
        }


@pytest.fixture
def simple_code_executor():
    """Fixture providing a SimpleCodeExecutor instance."""
    return SimpleCodeExecutor()


def test_code_executor_interface(simple_code_executor):
    """Test that the CodeExecutor interface works as expected."""
    # Setup test data
    test_code = "def transform(conn): return {'output.csv': 'data'}"
    function_name = "transform"
    sdif_sources = {"db1": Path("/path/to/sdif1.db"), "db2": Path("/path/to/sdif2.db")}
    extra_context = {"param1": "value1", "param2": "value2"}

    # Execute the code
    result = simple_code_executor.execute(
        code=test_code,
        function_name=function_name,
        sdif_sources=sdif_sources,
        extra_context=extra_context,
    )

    # Check result type and structure
    assert isinstance(result, dict)
    assert "output.csv" in result
    assert "summary.json" in result

    # Check the source schemas in the result
    summary = result["summary.json"]
    assert isinstance(summary, dict)
    assert "source_schemas" in summary
    assert set(summary["source_schemas"]) == {"db1", "db2"}


def test_code_executor_subclass_abstractness():
    """Test that CodeExecutor can't be instantiated directly."""
    with pytest.raises(TypeError):
        CodeExecutor()
