from pathlib import Path
from typing import Any, Dict, List, Union
from unittest.mock import patch

import pandas as pd
import pytest

from satif_core.sdif_db import SDIFDatabase
from satif_core.transformers.base import Transformer
from satif_core.types import SDIFPath


class SimpleTransformer(Transformer):
    """Simple concrete implementation of the Transformer abstract class."""

    def transform(
        self, sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]]
    ) -> Dict[str, Any]:
        """
        Simple implementation that returns a mock transformation result.
        """
        # For the simple implementation, just return mock data
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        return {"output.csv": df, "summary.json": {"count": 3}}

    def _export_data(
        self,
        data: Dict[str, Any],
        output_path: Union[str, Path] = Path("."),
        zip_archive: bool = False,
    ) -> Path:
        """
        Simple implementation that returns a mock output path.
        """
        resolved_path = Path(output_path).resolve()
        # Just pretend we wrote the files
        return resolved_path


@pytest.fixture
def simple_transformer():
    """Fixture providing a SimpleTransformer instance."""
    return SimpleTransformer()


def test_transformer_transform_method(simple_transformer):
    """Test that the transform method works as expected."""
    # Test with a single SDIF path
    result = simple_transformer.transform("input.sdif")

    # Check result type and content
    assert isinstance(result, dict)
    assert "output.csv" in result
    assert "summary.json" in result
    assert isinstance(result["output.csv"], pd.DataFrame)
    assert isinstance(result["summary.json"], dict)


def test_transformer_export_method(simple_transformer, tmp_path):
    """Test that the export method works as expected."""
    with (
        patch.object(SimpleTransformer, "transform") as mock_transform,
        patch.object(
            SimpleTransformer, "_export_data", return_value=tmp_path / "output"
        ) as mock_export,
    ):
        # Setup mock return value
        mock_data = {"output.csv": pd.DataFrame()}
        mock_transform.return_value = mock_data

        # Call export method
        result = simple_transformer.export("input.sdif", output_path=tmp_path)

        # Verify transform was called
        mock_transform.assert_called_once_with("input.sdif")

        # Verify _export_data was called with correct parameters
        mock_export.assert_called_once_with(
            data=mock_data, output_path=tmp_path, zip_archive=False
        )

        # Check result
        assert result == tmp_path / "output"


def test_transformer_subclass_abstractness():
    """Test that Transformer can't be instantiated directly."""
    with pytest.raises(TypeError):
        Transformer()
