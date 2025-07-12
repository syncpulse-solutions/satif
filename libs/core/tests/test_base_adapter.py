from pathlib import Path
from unittest.mock import patch

import pytest

from satif_core.adapters.base import Adapter


class SimpleAdapter(Adapter):
    """Simple concrete implementation of the Adapter abstract class."""

    def adapt(self, sdif):
        """
        Simple implementation that returns a mock output path.
        """
        # Just return a dummy path
        return Path("/tmp/adapted_sdif.db")


@pytest.fixture
def simple_adapter():
    """Fixture providing a SimpleAdapter instance."""
    return SimpleAdapter()


def test_adapter_interface(simple_adapter):
    """Test that the Adapter interface works as expected."""
    with patch.object(
        SimpleAdapter, "adapt", return_value=Path("/tmp/test.db")
    ) as mock_adapt:
        # Call the adapt method
        result = simple_adapter.adapt("input.sdif")

        # Check that the method was called with the right parameters
        mock_adapt.assert_called_once_with("input.sdif")

        # Check the return value is a Path
        assert isinstance(result, Path)
        assert str(result) == "/tmp/test.db"


def test_adapter_subclass_abstractness():
    """Test that Adapter can't be instantiated directly."""
    with pytest.raises(TypeError):
        Adapter()
