from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from satif_core.comparators.base import Comparator


class SimpleComparator(Comparator):
    """Simple concrete implementation of the Comparator abstract class."""

    def compare(
        self,
        file_path1: Path,
        file_path2: Path,
        file_config: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Simple implementation that returns a mock comparison result."""
        result = {
            "files": {"file1": str(file_path1), "file2": str(file_path2)},
            "comparison_params": kwargs,
            "are_equivalent": True,
            "summary": ["Files are equivalent"],
            "details": {},
        }

        if file_config:
            result["file_config"] = file_config

        return result


@pytest.fixture
def simple_comparator():
    """Fixture providing a SimpleComparator instance."""
    return SimpleComparator()


def test_comparator_interface(simple_comparator, tmp_path):
    """Test that the Comparator interface works as expected."""
    # Create test files
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.touch()
    file2.touch()

    # Test with basic parameters
    result = simple_comparator.compare(file1, file2)

    # Check result structure
    assert isinstance(result, dict)
    assert "files" in result
    assert "are_equivalent" in result
    assert result["files"]["file1"] == str(file1)
    assert result["files"]["file2"] == str(file2)

    # Test with additional kwargs
    result_with_kwargs = simple_comparator.compare(
        file1, file2, ignore_whitespace=True, ignore_case=True
    )
    assert result_with_kwargs["comparison_params"] == {
        "ignore_whitespace": True,
        "ignore_case": True,
    }

    # Test with file_config
    result_with_config = simple_comparator.compare(
        file1, file2, file_config={"encoding": "utf-8"}
    )
    assert result_with_config["file_config"] == {"encoding": "utf-8"}


def test_comparator_subclass_abstractness():
    """Test that Comparator can't be instantiated directly."""
    with pytest.raises(TypeError):
        Comparator()
