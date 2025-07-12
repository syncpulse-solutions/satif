from pathlib import Path
from typing import Any, Dict, Tuple, Union

import pytest

from satif_core.representers.base import Representer


class SimpleRepresenter(Representer):
    """Simple concrete implementation of the Representer abstract class."""

    def represent(
        self,
        file_path: Union[str, Path],
        num_rows: int = 10,
        **kwargs: Any,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Simple implementation that returns a mock representation.
        """
        file_path = Path(file_path)
        representation = f"Representation of {file_path.name} with {num_rows} rows"
        params_used = {"num_rows": num_rows, **kwargs}

        return representation, params_used

    def as_base64_image(self, file_path: Union[str, Path], **kwargs: Any) -> str:
        """Return a mock base64 image."""
        return "base64_encoded_string"

    def as_text(self, file_path: Union[str, Path], **kwargs: Any) -> str:
        """Return a mock text representation."""
        return f"Text representation of {Path(file_path).name}"


@pytest.fixture
def simple_representer():
    """Fixture providing a SimpleRepresenter instance."""
    return SimpleRepresenter()


def test_representer_interface(simple_representer, tmp_path):
    """Test that the Representer interface works as expected."""
    # Create a test file
    test_file = tmp_path / "test_file.csv"
    test_file.touch()

    # Test with default parameters
    representation, params = simple_representer.represent(test_file)

    # Check result types
    assert isinstance(representation, str)
    assert isinstance(params, dict)

    # Check default params
    assert params["num_rows"] == 10

    # Test with custom parameters
    representation, params = simple_representer.represent(
        test_file, num_rows=5, encoding="utf-8"
    )

    assert "Representation of test_file.csv with 5 rows" in representation
    assert params["num_rows"] == 5
    assert params["encoding"] == "utf-8"


def test_representer_helper_methods(simple_representer, tmp_path):
    """Test the helper methods of Representer."""
    test_file = tmp_path / "test_file.png"
    test_file.touch()

    # Test base64 image representation
    base64_result = simple_representer.as_base64_image(test_file)
    assert isinstance(base64_result, str)

    # Test text representation
    text_result = simple_representer.as_text(test_file)
    assert isinstance(text_result, str)
    assert "test_file.png" in text_result


def test_representer_subclass_abstractness():
    """Test that Representer can't be instantiated directly."""
    with pytest.raises(TypeError):
        Representer()
