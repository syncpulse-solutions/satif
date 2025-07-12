from pathlib import Path

import pytest

from satif_core.standardizers.base import AsyncStandardizer, Standardizer
from satif_core.types import StandardizationResult


class SimpleStandardizer(Standardizer):
    """Simple concrete implementation of the Standardizer abstract class."""

    def standardize(
        self, datasource, output_path, *, overwrite=False, config=None, **kwargs
    ):
        """
        Simple implementation that returns a mock StandardizationResult.
        """
        resolved_path = Path(output_path).resolve()
        file_configs = None

        if isinstance(datasource, (str, Path)):
            file_configs = {str(datasource): {"type": "csv", "encoding": "utf-8"}}
        elif isinstance(datasource, list):
            file_configs = {
                str(path): {"type": "csv", "encoding": "utf-8"} for path in datasource
            }

        return StandardizationResult(
            output_path=resolved_path, file_configs=file_configs
        )


class SimpleAsyncStandardizer(AsyncStandardizer):
    """Simple concrete implementation of the AsyncStandardizer abstract class."""

    async def standardize(
        self, datasource, output_path, *, overwrite=False, config=None, **kwargs
    ):
        """
        Simple implementation that returns a mock StandardizationResult.
        """
        resolved_path = Path(output_path).resolve()
        file_configs = None

        if isinstance(datasource, (str, Path)):
            file_configs = {str(datasource): {"type": "csv", "encoding": "utf-8"}}
        elif isinstance(datasource, list):
            file_configs = {
                str(path): {"type": "csv", "encoding": "utf-8"} for path in datasource
            }

        return StandardizationResult(
            output_path=resolved_path, file_configs=file_configs
        )


@pytest.fixture
def simple_standardizer():
    """Fixture providing a SimpleStandardizer instance."""
    return SimpleStandardizer()


@pytest.fixture
def simple_async_standardizer():
    """Fixture providing a SimpleAsyncStandardizer instance."""
    return SimpleAsyncStandardizer()


def test_standardizer_interface(simple_standardizer, tmp_path):
    """Test that the Standardizer interface works as expected."""
    # Setup test paths
    input_file = tmp_path / "input.csv"
    input_file.touch()
    output_path = tmp_path / "output.sdif"

    # Test with a single file datasource
    result = simple_standardizer.standardize(input_file, output_path)

    # Check result type and properties
    assert isinstance(result, StandardizationResult)
    assert result.output_path == output_path.resolve()
    assert isinstance(result.file_configs, dict)
    assert str(input_file) in result.file_configs

    # Test with a list of files
    input_files = [input_file, tmp_path / "input2.csv"]
    input_files[1].touch()

    result = simple_standardizer.standardize(input_files, output_path)
    assert len(result.file_configs) == 2
    assert all(str(path) in result.file_configs for path in input_files)


@pytest.mark.asyncio
async def test_async_standardizer_interface(simple_async_standardizer, tmp_path):
    """Test that the AsyncStandardizer interface works as expected."""
    # Setup test paths
    input_file = tmp_path / "input.csv"
    input_file.touch()
    output_path = tmp_path / "output.sdif"

    # Test with a single file datasource
    result = await simple_async_standardizer.standardize(input_file, output_path)

    # Check result type and properties
    assert isinstance(result, StandardizationResult)
    assert result.output_path == output_path.resolve()
    assert isinstance(result.file_configs, dict)
    assert str(input_file) in result.file_configs


def test_standardizer_subclass_abstractness():
    """Test that Standardizer can't be instantiated directly."""
    with pytest.raises(TypeError):
        Standardizer()


def test_async_standardizer_subclass_abstractness():
    """Test that AsyncStandardizer can't be instantiated directly."""
    with pytest.raises(TypeError):
        AsyncStandardizer()
