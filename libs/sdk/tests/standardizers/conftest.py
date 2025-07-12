import csv
from pathlib import Path
from typing import List, Optional, Union

import pytest


@pytest.fixture
def create_csv_file(tmp_path: Path):
    """
    Fixture to create a temporary CSV file with specified content.
    """

    def _create_csv_file(
        file_name: str,
        data: List[List[Union[str, int, float]]],
        sub_dir: Optional[str] = None,
    ) -> Path:
        if sub_dir:
            dir_path = tmp_path / sub_dir
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / file_name
        else:
            file_path = tmp_path / file_name

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        return file_path

    return _create_csv_file


@pytest.fixture
def create_text_file(tmp_path: Path):
    """
    Fixture to create a temporary text file with specified content.
    Useful for testing encoding detection with non-UTF-8 files.
    """

    def _create_text_file(
        file_name: str,
        content: str,
        encoding: str = "utf-8",
        sub_dir: Optional[str] = None,
    ) -> Path:
        if sub_dir:
            dir_path = tmp_path / sub_dir
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / file_name
        else:
            file_path = tmp_path / file_name

        with open(file_path, "w", encoding=encoding) as f:
            f.write(content)
        return file_path

    return _create_text_file


@pytest.fixture
def create_excel_file(tmp_path: Path):
    """
    Fixture to create a temporary Excel file with specified content.
    Requires pandas and openpyxl to be installed.
    """

    def _create_excel_file(
        file_name: str,
        data_dict: dict,
        sub_dir: Optional[str] = None,
    ) -> Path:
        """
        Create an Excel file with multiple sheets.

        Args:
            file_name: Name of the Excel file to create
            data_dict: Dict mapping sheet names to data lists (list of lists)
            sub_dir: Optional subdirectory to create the file in

        Returns:
            Path to the created Excel file
        """
        import pandas as pd

        if sub_dir:
            dir_path = tmp_path / sub_dir
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / file_name
        else:
            file_path = tmp_path / file_name

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            for sheet_name, sheet_data in data_dict.items():
                # Convert list of lists to DataFrame
                if (
                    isinstance(sheet_data, list)
                    and sheet_data
                    and isinstance(sheet_data[0], list)
                ):
                    # If first row should be headers
                    headers = sheet_data[0]
                    data = sheet_data[1:]
                    df = pd.DataFrame(data, columns=headers)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                elif isinstance(sheet_data, pd.DataFrame):
                    # If directly provided as DataFrame
                    sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    raise ValueError(
                        f"Data for sheet {sheet_name} must be a list of lists or DataFrame"
                    )

        return file_path

    return _create_excel_file
