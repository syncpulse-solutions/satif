import asyncio
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from satif_core.standardizers.base import AsyncStandardizer
from satif_core.types import StandardizationResult

from satif_ai.standardizers.ai import AIStandardizer


class MockAsyncStandardizer(AsyncStandardizer):
    """Mock standardizer for testing"""

    def __init__(self, **kwargs):
        self.mcp_server = kwargs.get("mcp_server")
        self.mcp_session = kwargs.get("mcp_session")
        self.llm_model = kwargs.get("llm_model")

    async def standardize(self, datasource, output_path, *, overwrite=False, **kwargs):
        # Create a real output file to simulate successful standardization
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.touch()
        return StandardizationResult(output_path=output_path)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir).resolve()


@pytest.fixture
def mock_csv_file(temp_dir):
    """Create a mock CSV file."""
    file_path = temp_dir / "test.csv"
    file_path.write_text("col1,col2\nval1,val2\n")
    return file_path.resolve()


@pytest.fixture
def mock_xlsx_file(temp_dir):
    """Create a mock XLSX file."""
    file_path = temp_dir / "test.xlsx"
    file_path.touch()
    return file_path.resolve()


@pytest.fixture
def mock_unsupported_file(temp_dir):
    """Create a mock unsupported file."""
    file_path = temp_dir / "test.txt"
    file_path.write_text("some content")
    return file_path.resolve()


@pytest.fixture
def mock_output_path(temp_dir):
    """Create a mock output path."""
    return (temp_dir / "output.sdif").resolve()


@pytest.fixture
def ai_standardizer():
    """Create an instance of AIStandardizer with mock standardizers."""
    with (
        patch("satif_ai.standardizers.ai.AICSVStandardizer", MockAsyncStandardizer),
        patch("satif_ai.standardizers.ai.AIXLSXStandardizer", MockAsyncStandardizer),
    ):
        standardizer = AIStandardizer(
            mcp_server=MagicMock(), mcp_session=MagicMock(), llm_model="mock-model"
        )
        yield standardizer


class TestAIStandardizer:
    def test_init(self):
        """Test initialization of AIStandardizer."""
        standardizer = AIStandardizer(
            mcp_server="mock_server",
            mcp_session="mock_session",
            llm_model="mock_model",
        )

        assert standardizer.mcp_server == "mock_server"
        assert standardizer.mcp_session == "mock_session"
        assert standardizer.llm_model == "mock_model"
        assert ".csv" in standardizer.ai_standardizer_map
        assert ".xlsx" in standardizer.ai_standardizer_map

    def test_get_ai_standardizer_class(self):
        """Test _get_ai_standardizer_class method."""
        standardizer = AIStandardizer()

        from satif_ai.standardizers.ai_csv import AICSVStandardizer
        from satif_ai.standardizers.ai_xlsx import AIXLSXStandardizer

        assert standardizer._get_ai_standardizer_class(".csv") is AICSVStandardizer
        assert standardizer._get_ai_standardizer_class(".xlsx") is AIXLSXStandardizer
        assert (
            standardizer._get_ai_standardizer_class(".CSV") is AICSVStandardizer
        )  # Case insensitive
        assert standardizer._get_ai_standardizer_class(".unknown") is None

    def test_resolve_file_path_file(self, ai_standardizer, mock_csv_file, temp_dir):
        """Test _resolve_file_path with a file."""
        result = ai_standardizer._resolve_file_path(mock_csv_file, temp_dir)
        assert len(result) == 1
        assert result[0].resolve() == mock_csv_file.resolve()

    def test_resolve_file_path_directory(self, ai_standardizer, temp_dir):
        """Test _resolve_file_path with a directory."""
        # Create some test files in the directory
        (temp_dir / "test1.csv").touch()
        (temp_dir / "test2.xlsx").touch()
        (temp_dir / "readme.txt").touch()

        result = ai_standardizer._resolve_file_path(temp_dir, temp_dir)
        assert len(result) == 3  # All files in directory
        assert all(isinstance(p, Path) for p in result)

    def test_resolve_file_path_not_found(self, ai_standardizer, temp_dir):
        """Test _resolve_file_path with a non-existent file."""
        with pytest.raises(FileNotFoundError):
            ai_standardizer._resolve_file_path(temp_dir / "nonexistent.file", temp_dir)

    def test_setup_workspace_real_implementation(self, ai_standardizer, temp_dir):
        """Test _setup_workspace method with the real implementation."""
        output_path = temp_dir / "output.sdif"

        # Call the real method (synchronously since it's not async anymore)
        final_path, intermediate_dir, processing_dir = ai_standardizer._setup_workspace(
            output_path, False
        )

        # Verify the paths
        assert final_path.resolve() == output_path.resolve()
        assert intermediate_dir.exists()
        assert processing_dir.exists()
        assert intermediate_dir.name == "intermediate_sdif_files"
        assert processing_dir.name == "file_processing_temp"
        assert intermediate_dir.parent == processing_dir.parent  # Same temp run dir

    def test_setup_workspace_directory_output_error(self, ai_standardizer, temp_dir):
        """Test _setup_workspace raises error when output is a directory."""
        with pytest.raises(ValueError, match="Target output_path .* is a directory"):
            ai_standardizer._setup_workspace(temp_dir, False)

    def test_setup_workspace_no_extension_warning(
        self, ai_standardizer, temp_dir, caplog
    ):
        """Test _setup_workspace logs warning for files without extension."""
        output_path = temp_dir / "output_no_ext"

        ai_standardizer._setup_workspace(output_path, False)

        assert "has no file extension" in caplog.text

    @pytest.mark.asyncio
    async def test_resolve_input_files_single_file(
        self, ai_standardizer, mock_csv_file, temp_dir
    ):
        """Test _resolve_input_files with a single file."""
        result = await ai_standardizer._resolve_input_files(mock_csv_file, temp_dir)
        assert len(result) == 1
        assert result[0].resolve() == mock_csv_file.resolve()

    @pytest.mark.asyncio
    async def test_resolve_input_files_file_list(
        self, ai_standardizer, mock_csv_file, mock_xlsx_file, temp_dir
    ):
        """Test _resolve_input_files with a list of files."""
        result = await ai_standardizer._resolve_input_files(
            [mock_csv_file, mock_xlsx_file], temp_dir
        )
        assert len(result) == 2
        result_paths = {p.resolve() for p in result}
        assert {mock_csv_file.resolve(), mock_xlsx_file.resolve()} == result_paths

    @pytest.mark.asyncio
    async def test_resolve_input_files_invalid_input(self, ai_standardizer, temp_dir):
        """Test _resolve_input_files with invalid input."""
        with pytest.raises(
            ValueError, match="Datasource must be a non-empty file path"
        ):
            await ai_standardizer._resolve_input_files({}, temp_dir)

        with pytest.raises(ValueError, match="No input datasource paths provided"):
            await ai_standardizer._resolve_input_files([], temp_dir)

    def test_group_files_by_standardizer(
        self, ai_standardizer, mock_csv_file, mock_xlsx_file, mock_unsupported_file
    ):
        """Test _group_files_by_standardizer."""
        files = [mock_csv_file, mock_xlsx_file, mock_unsupported_file]
        grouped, unsupported = ai_standardizer._group_files_by_standardizer(files)

        # Should have MockAsyncStandardizer for both csv and xlsx since we mocked them
        assert len(grouped) == 1
        assert MockAsyncStandardizer in grouped
        assert len(grouped[MockAsyncStandardizer]) == 2
        assert len(unsupported) == 1
        assert unsupported[0].resolve() == mock_unsupported_file.resolve()

    @pytest.mark.asyncio
    async def test_process_file_groups_success(
        self, ai_standardizer, mock_csv_file, temp_dir
    ):
        """Test _process_file_groups with successful processing."""
        grouped_files = {MockAsyncStandardizer: [mock_csv_file]}

        intermediate_files, file_configs = await ai_standardizer._process_file_groups(
            grouped_files, temp_dir, None
        )

        assert len(intermediate_files) == 1
        assert intermediate_files[0].exists()  # File should actually exist
        assert intermediate_files[0].suffix == ".sdif"
        assert file_configs == []

    @pytest.mark.asyncio
    async def test_cleanup_workspace_real_implementation(self, ai_standardizer):
        """Test _cleanup_workspace with real implementation."""
        # Create a real temporary directory to clean up
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_dir = Path(tmp_dir) / "test_cleanup"
            test_dir.mkdir()
            test_file = test_dir / "test_file.txt"
            test_file.write_text("test content")

            # Verify directory exists before cleanup
            assert test_dir.exists()
            assert test_file.exists()

            # Call the real cleanup method
            await ai_standardizer._cleanup_workspace(test_dir)

            # Directory should be gone
            assert not test_dir.exists()

    def test_consolidate_results_single_file(self, ai_standardizer, temp_dir):
        """Test _consolidate_results with a single file."""
        # Create a real intermediate file
        intermediate_file = temp_dir / "intermediate.sdif"
        intermediate_file.write_text("test sdif content")

        output_path = temp_dir / "final_output.sdif"

        result = ai_standardizer._consolidate_results(
            [intermediate_file], None, output_path, True
        )

        assert result.output_path.resolve() == output_path.resolve()
        assert result.file_configs is None
        assert output_path.exists()
        # Intermediate file should be moved (no longer exists)
        assert not intermediate_file.exists()

    def test_consolidate_results_no_files_error(self, ai_standardizer, temp_dir):
        """Test _consolidate_results raises error with no files."""
        output_path = temp_dir / "output.sdif"

        with pytest.raises(
            RuntimeError, match="No intermediate SDIF files were successfully generated"
        ):
            ai_standardizer._consolidate_results([], None, output_path, True)

    @pytest.mark.asyncio
    async def test_standardize_integration(
        self, ai_standardizer, mock_csv_file, mock_output_path
    ):
        """Test the full standardize method integration without excessive mocking."""
        # This tests the real integration between methods
        result = await ai_standardizer.standardize(
            mock_csv_file, mock_output_path, overwrite=True
        )

        assert isinstance(result, StandardizationResult)
        assert result.output_path.resolve() == mock_output_path.resolve()
        assert mock_output_path.exists()

    @pytest.mark.asyncio
    async def test_standardize_no_supported_files_error(
        self, ai_standardizer, mock_unsupported_file, mock_output_path
    ):
        """Test standardize raises error when no files are supported."""
        with pytest.raises(
            ValueError,
            match="No files found that can be handled by configured AI standardizers",
        ):
            await ai_standardizer.standardize(mock_unsupported_file, mock_output_path)

    @pytest.mark.asyncio
    async def test_standardize_file_not_found_error(
        self, ai_standardizer, temp_dir, mock_output_path
    ):
        """Test standardize raises error for non-existent files."""
        nonexistent_file = temp_dir / "does_not_exist.csv"

        with pytest.raises(FileNotFoundError):
            await ai_standardizer.standardize(nonexistent_file, mock_output_path)

    @pytest.mark.asyncio
    async def test_standardize_overwrite_false_existing_file(
        self, ai_standardizer, mock_csv_file, mock_output_path
    ):
        """Test standardize raises error when output exists and overwrite=False."""
        # Create existing output file
        mock_output_path.touch()

        with pytest.raises(
            FileExistsError, match="already exists and overwrite is False"
        ):
            await ai_standardizer.standardize(
                mock_csv_file, mock_output_path, overwrite=False
            )

    @pytest.mark.asyncio
    async def test_asyncio_to_thread_integration(
        self, ai_standardizer, mock_csv_file, mock_output_path
    ):
        """Test that asyncio.to_thread calls work correctly with the real methods."""
        # This test specifically checks that our sync methods work with asyncio.to_thread
        output_path = mock_output_path

        # Test _setup_workspace through asyncio.to_thread (as used in real code)
        final_path, intermediate_dir, processing_dir = await asyncio.to_thread(
            ai_standardizer._setup_workspace, output_path, False
        )

        assert final_path.resolve() == output_path.resolve()
        assert intermediate_dir.exists()
        assert processing_dir.exists()

        # Test _consolidate_results through asyncio.to_thread
        intermediate_file = intermediate_dir / "test.sdif"
        intermediate_file.write_text("test content")

        result = await asyncio.to_thread(
            ai_standardizer._consolidate_results,
            [intermediate_file],
            None,
            output_path,
            True,
        )

        assert result.output_path.resolve() == output_path.resolve()
        assert output_path.exists()
