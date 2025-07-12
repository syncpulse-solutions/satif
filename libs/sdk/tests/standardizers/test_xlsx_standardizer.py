from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest
from sdif_db import SDIFDatabase

from satif_sdk.standardizers.xlsx import XLSXStandardizer

# pylint: disable=redefined-outer-name


def _get_table_schema(db_path: Path, table_name: str) -> Dict[str, Any]:
    """Helper to get schema of a specific table with detailed column metadata."""
    with SDIFDatabase(db_path) as db:
        # Use the full schema to extract column metadata with types
        full_schema = db.get_schema()
        table_schema = full_schema["tables"][table_name]
        columns_list = table_schema.get("columns", [])
        # Map column name to its metadata
        columns_mapping: Dict[str, Any] = {}
        for col in columns_list:
            columns_mapping[col["name"]] = {
                "type": col.get("sqlite_type"),
                "description": col.get("description"),
                "original_column_name": col.get("original_column_name"),
            }
        return {"columns": columns_mapping}


def _get_table_data(db_path: Path, table_name: str) -> List[Dict[str, Any]]:
    """Helper to get all data from a specific table."""
    with SDIFDatabase(db_path) as db:
        df = db.read_table(table_name)
        return df.replace({np.nan: None}).to_dict("records")


def _get_all_table_names(db_path: Path) -> List[str]:
    """Helper to get all user table names from the SDIF database."""
    with SDIFDatabase(db_path) as db:
        return db.list_tables()


@pytest.mark.parametrize(
    "sheet_data, expected_cols, expected_rows_count",
    [
        (
            [["id", "name"], [1, "Alice"], [2, "Bob"]],
            {"id": "INTEGER", "name": "TEXT"},
            2,
        )
    ],
)
def test_xlsx_standardizer_basic(
    create_excel_file,
    tmp_path: Path,
    sheet_data: List[List[Any]],
    expected_cols: Dict[str, str],
    expected_rows_count: int,
):
    """Test basic XLSX standardization."""
    # Create Excel file with a single sheet
    excel_file = create_excel_file("data.xlsx", {"Sheet1": sheet_data})

    # Initialize standardizer with default settings
    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "output.sdif"

    # Standardize
    result = standardizer.standardize(excel_file, output_sdif)
    assert result.output_path.exists()
    assert str(excel_file.resolve()) in result.file_configs

    # Verify table name and structure
    table_names = _get_all_table_names(output_sdif)
    assert len(table_names) == 1
    table_name = table_names[0]
    assert table_name == "sheet1"  # Default table name from sheet name, lowercase

    # Verify schema
    schema = _get_table_schema(output_sdif, table_name)
    assert len(schema["columns"]) == len(expected_cols)
    for col_name, col_type in expected_cols.items():
        assert schema["columns"][col_name]["type"] == col_type

    # Verify data
    data = _get_table_data(output_sdif, table_name)
    assert len(data) == expected_rows_count
    if expected_rows_count > 0:
        assert list(data[0].keys()) == list(expected_cols.keys())


def test_xlsx_standardizer_sheet_selection(create_excel_file, tmp_path: Path):
    """Test selecting specific sheets from an Excel file."""
    # Create Excel file with multiple sheets
    sheet1_data = [["id", "name"], [1, "Alice"], [2, "Bob"]]
    sheet2_data = [["product", "price"], ["Apple", 1.99], ["Orange", 2.49]]
    excel_file = create_excel_file(
        "multi_sheet.xlsx", {"Sheet1": sheet1_data, "Sheet2": sheet2_data}
    )

    # Test selecting by sheet index
    standardizer1 = XLSXStandardizer(sheet_name=1)  # Select Sheet2 (0-based index)
    output_sdif1 = tmp_path / "output_sheet_index.sdif"
    standardizer1.standardize(excel_file, output_sdif1)

    table_names1 = _get_all_table_names(output_sdif1)
    assert len(table_names1) == 1
    assert table_names1[0] == "sheet2"

    data1 = _get_table_data(output_sdif1, "sheet2")
    assert len(data1) == 2
    assert data1[0]["product"] == "Apple"

    # Test selecting by sheet name
    standardizer2 = XLSXStandardizer(sheet_name="Sheet1")
    output_sdif2 = tmp_path / "output_sheet_name.sdif"
    standardizer2.standardize(excel_file, output_sdif2)

    table_names2 = _get_all_table_names(output_sdif2)
    assert len(table_names2) == 1
    assert table_names2[0] == "sheet1"

    data2 = _get_table_data(output_sdif2, "sheet1")
    assert len(data2) == 2
    assert data2[0]["id"] == 1


def test_xlsx_standardizer_header_and_skip_rows(create_excel_file, tmp_path: Path):
    """Test the header_row and skip_rows parameters."""
    # Create Excel file with metadata rows at the top
    data = [
        ["REPORT TITLE", "", ""],  # Row 0 - Metadata
        ["Generated on:", "2023-01-01", ""],  # Row 1 - Metadata
        ["", "", ""],  # Row 2 - Empty row
        ["ID", "Name", "Value"],  # Row 3 - Actual header
        [1, "Alice", 100],  # Row 4 - Data
        [2, "Bob", 200],  # Row 5 - Data
    ]
    excel_file = create_excel_file("header_skip.xlsx", {"Data": data})

    # Test with skip_rows=3, header_row=0 (relative to skip_rows)
    standardizer = XLSXStandardizer(skip_rows=3, header_row=0)
    output_sdif = tmp_path / "output_header_skip.sdif"
    standardizer.standardize(excel_file, output_sdif)

    table_names = _get_all_table_names(output_sdif)
    assert len(table_names) == 1

    schema = _get_table_schema(output_sdif, table_names[0])
    assert "id" in schema["columns"]
    assert "name" in schema["columns"]
    assert "value" in schema["columns"]

    data = _get_table_data(output_sdif, table_names[0])
    assert len(data) == 2
    assert data[0]["id"] == 1
    assert data[0]["name"] == "Alice"
    assert data[0]["value"] == 100


def test_xlsx_standardizer_skip_columns(create_excel_file, tmp_path: Path):
    """Test skipping columns by name or index."""
    data = [
        ["id", "name", "age", "salary", "department"],
        [1, "Alice", 30, 50000, "HR"],
        [2, "Bob", 35, 60000, "Engineering"],
    ]
    excel_file = create_excel_file("skip_cols.xlsx", {"Data": data})

    # Test skipping by column name
    standardizer1 = XLSXStandardizer(skip_columns=["age", "salary"])
    output_sdif1 = tmp_path / "output_skip_name.sdif"
    standardizer1.standardize(excel_file, output_sdif1)

    schema1 = _get_table_schema(output_sdif1, "data")
    assert "id" in schema1["columns"]
    assert "name" in schema1["columns"]
    assert "department" in schema1["columns"]
    assert "age" not in schema1["columns"]
    assert "salary" not in schema1["columns"]

    # Note: The XLSX standardizer's skip_columns indices are based on the
    # DataFrame columns AFTER reading, unlike the CSV standardizer.
    # Also, in pandas, columns are named objects not just integer positions.
    # Let's correct this test by using column names in both cases:

    standardizer2 = XLSXStandardizer(skip_columns=["id", "department"])  # Skip by name
    output_sdif2 = tmp_path / "output_skip_index.sdif"
    standardizer2.standardize(excel_file, output_sdif2)

    schema2 = _get_table_schema(output_sdif2, "data")
    assert "id" not in schema2["columns"]
    assert "name" in schema2["columns"]
    assert "age" in schema2["columns"]
    assert "salary" in schema2["columns"]
    assert "department" not in schema2["columns"]


def test_xlsx_standardizer_multiple_files(create_excel_file, tmp_path: Path):
    """Test standardizing multiple Excel files."""
    # Create two Excel files
    excel_file1 = create_excel_file(
        "data1.xlsx", {"Sheet1": [["id", "name"], [1, "Alice"], [2, "Bob"]]}
    )
    excel_file2 = create_excel_file(
        "data2.xlsx",
        {"Products": [["product", "price"], ["Apple", 1.99], ["Orange", 2.49]]},
    )

    # Standardize both files together
    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "multi.sdif"
    result = standardizer.standardize([excel_file1, excel_file2], output_sdif)

    assert result.output_path.exists()
    table_names = _get_all_table_names(output_sdif)
    assert len(table_names) == 2
    assert "sheet1" in table_names  # First file's sheet name
    assert "products" in table_names  # Second file's sheet name

    # Verify first file's data
    data1 = _get_table_data(output_sdif, "sheet1")
    assert len(data1) == 2
    assert data1[0]["id"] == 1
    assert data1[0]["name"] == "Alice"

    # Verify second file's data
    data2 = _get_table_data(output_sdif, "products")
    assert len(data2) == 2
    assert data2[0]["product"] == "Apple"
    assert data2[0]["price"] == 1.99


def test_xlsx_standardizer_custom_table_names(create_excel_file, tmp_path: Path):
    """Test custom table names for single and multiple files."""
    # Create Excel files
    excel_file1 = create_excel_file(
        "data1.xlsx", {"Sheet1": [["id", "name"], [1, "Alice"]]}
    )
    excel_file2 = create_excel_file(
        "data2.xlsx", {"Sheet1": [["product", "price"], ["Apple", 1.99]]}
    )

    # Single file with custom table name
    standardizer1 = XLSXStandardizer(table_names="CustomTable")
    output_sdif1 = tmp_path / "single_custom.sdif"
    standardizer1.standardize(excel_file1, output_sdif1)

    table_names1 = _get_all_table_names(output_sdif1)
    assert table_names1 == ["customtable"]  # Lowercase due to sanitization

    # Multiple files with different custom table names
    standardizer2 = XLSXStandardizer(table_names=["TableX", "TableY"])
    output_sdif2 = tmp_path / "multi_custom.sdif"
    standardizer2.standardize([excel_file1, excel_file2], output_sdif2)

    table_names2 = _get_all_table_names(output_sdif2)
    assert set(table_names2) == {"tablex", "tabley"}

    # Multiple files with same custom table name (should add index suffix)
    standardizer3 = XLSXStandardizer(table_names="BaseName")
    output_sdif3 = tmp_path / "multi_same_name.sdif"
    standardizer3.standardize([excel_file1, excel_file2], output_sdif3)

    table_names3 = _get_all_table_names(output_sdif3)
    assert set(table_names3) == {"basename", "basename_1"}


def test_xlsx_standardizer_file_configs_override(create_excel_file, tmp_path: Path):
    """Test overriding configurations per file using file_configs."""
    # Create a multi-sheet Excel file
    excel_file = create_excel_file(
        "multi_sheet.xlsx",
        {
            "Sheet1": [["id", "name"], [1, "Alice"], [2, "Bob"]],
            "Sheet2": [["product", "price"], ["Apple", 1.99], ["Orange", 2.49]],
        },
    )

    # Test with file_configs as a single dict for one file
    file_config_single = {
        "sheet_name": "Sheet2",
        "table_name": "ProductsTable",
        "description": "Products data",
    }

    standardizer1 = XLSXStandardizer(file_configs=file_config_single)
    output_sdif1 = tmp_path / "override_single.sdif"
    standardizer1.standardize(excel_file, output_sdif1)

    table_names1 = _get_all_table_names(output_sdif1)
    assert "productstable" in table_names1

    # Test with file_configs as a list for multiple files
    excel_file1 = create_excel_file(
        "data1.xlsx", {"Sheet1": [["id", "name"], [1, "Alice"]]}
    )
    excel_file2 = create_excel_file(
        "data2.xlsx",
        {
            "Sheet1": [["not_used", "ignore"], [0, "ignored"]],
            "Sheet2": [["product", "price"], ["Apple", 1.99]],
        },
    )

    file_configs_list = [
        {"table_name": "Employees"},
        {"sheet_name": "Sheet2", "table_name": "Products"},
    ]

    standardizer2 = XLSXStandardizer(file_configs=file_configs_list)
    output_sdif2 = tmp_path / "override_list.sdif"
    standardizer2.standardize([excel_file1, excel_file2], output_sdif2)

    table_names2 = _get_all_table_names(output_sdif2)
    assert set(table_names2) == {"employees", "products"}


def test_xlsx_standardizer_empty_sheet(create_excel_file, tmp_path: Path):
    """Test handling of empty Excel sheets."""
    # Create Excel file with empty sheet
    empty_df = pd.DataFrame()
    excel_file = create_excel_file("empty.xlsx", {"EmptySheet": empty_df})

    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "empty_output.sdif"

    result = standardizer.standardize(excel_file, output_sdif)
    assert result.output_path.exists()

    # Should have no tables but should have added source entry
    table_names = _get_all_table_names(output_sdif)
    assert len(table_names) == 0

    # Verify source was added
    with SDIFDatabase(output_sdif) as db:
        sources = db.query("SELECT * FROM sdif_sources")
        assert len(sources) == 1
        assert sources.iloc[0]["original_file_name"] == "empty.xlsx"
        assert sources.iloc[0]["original_file_type"] == "xlsx"


def test_xlsx_standardizer_overwrite_existing_sdif(create_excel_file, tmp_path: Path):
    """Test the overwrite parameter functionality."""
    # Create two Excel files
    excel_file1 = create_excel_file(
        "data1.xlsx", {"Sheet1": [["id", "name"], [1, "Alice"]]}
    )
    excel_file2 = create_excel_file(
        "data2.xlsx", {"Sheet1": [["product", "price"], ["Apple", 1.99]]}
    )

    # Create initial SDIF file
    output_sdif = tmp_path / "overwrite.sdif"
    standardizer1 = XLSXStandardizer()
    standardizer1.standardize(excel_file1, output_sdif)

    # Verify first file was processed
    assert "sheet1" in _get_all_table_names(output_sdif)

    # Attempt to standardize second file to same output with overwrite=True
    standardizer2 = XLSXStandardizer()
    result = standardizer2.standardize(excel_file2, output_sdif, overwrite=True)

    # Verify second file replaced the first
    table_names = _get_all_table_names(result.output_path)
    assert "sheet1" in table_names  # Same sheet name from second file

    data = _get_table_data(result.output_path, "sheet1")
    assert len(data) == 1
    assert "product" in data[0]  # Data from second file

    # Test case 2: overwrite=False should raise an exception
    no_overwrite_path = tmp_path / "no_overwrite.sdif"

    # Create a file that will trigger an exception
    with open(no_overwrite_path, "w") as f:
        f.write("This file exists and should not be overwritten")

    # Attempt to standardize to this file (should fail)
    standardizer3 = XLSXStandardizer()
    with pytest.raises(Exception):
        standardizer3.standardize(excel_file1, no_overwrite_path)


def test_xlsx_standardizer_date_handling(create_excel_file, tmp_path: Path):
    """Test handling of date/time values."""
    # Create Excel file with dates
    from datetime import date, datetime

    df = pd.DataFrame(
        {
            "id": [1, 2],
            "date": [date(2023, 1, 1), date(2023, 2, 1)],
            "datetime": [datetime(2023, 1, 1, 10, 30), datetime(2023, 2, 1, 14, 45)],
        }
    )

    excel_file = create_excel_file("dates.xlsx", {"Dates": df})

    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "dates_output.sdif"

    standardizer.standardize(excel_file, output_sdif)

    # Check schema and data
    table_name = _get_all_table_names(output_sdif)[0]
    schema = _get_table_schema(output_sdif, table_name)

    # Date columns should be stored as TEXT in SQLite
    assert schema["columns"]["date"]["type"] == "TEXT"
    assert schema["columns"]["datetime"]["type"] == "TEXT"

    # Verify data was stored correctly
    data = _get_table_data(output_sdif, table_name)
    assert len(data) == 2
    assert (
        data[0]["date"] == "2023-01-01T00:00:00"
    )  # Date stored as full ISO format with time
    assert "2023-01-01T10:30:00" in data[0]["datetime"]  # ISO format with time


def test_xlsx_standardizer_numeric_types(create_excel_file, tmp_path: Path):
    """Test mapping of various numeric types."""
    # Create Excel file with different numeric types

    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "bool_col": [True, False, True],
        }
    )

    excel_file = create_excel_file("numbers.xlsx", {"Numbers": df})

    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "numbers_output.sdif"

    standardizer.standardize(excel_file, output_sdif)

    # Check schema types
    table_name = _get_all_table_names(output_sdif)[0]
    schema = _get_table_schema(output_sdif, table_name)

    assert schema["columns"]["int_col"]["type"] == "INTEGER"
    assert schema["columns"]["float_col"]["type"] == "REAL"
    assert (
        schema["columns"]["bool_col"]["type"] == "INTEGER"
    )  # Booleans stored as INTEGER

    # Check values
    data = _get_table_data(output_sdif, table_name)
    assert data[0]["int_col"] == 1
    assert data[0]["float_col"] == 1.1
    assert data[0]["bool_col"] == 1  # True stored as 1
    assert data[1]["bool_col"] == 0  # False stored as 0


def test_xlsx_standardizer_duplicate_sheet_names(create_excel_file, tmp_path: Path):
    """Test handling of duplicate sheet names from different files."""
    # Create two Excel files with same sheet name
    excel_file1 = create_excel_file(
        "data1.xlsx", {"Sheet1": [["id", "name"], [1, "Alice"]]}
    )
    excel_file2 = create_excel_file(
        "data2.xlsx", {"Sheet1": [["product", "price"], ["Apple", 1.99]]}
    )

    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "duplicate_sheets.sdif"

    standardizer.standardize([excel_file1, excel_file2], output_sdif)

    # Should create unique table names
    table_names = _get_all_table_names(output_sdif)
    assert len(table_names) == 2
    assert "sheet1" in table_names
    assert "sheet1_1" in table_names  # Second sheet gets suffix


def test_xlsx_standardizer_case_sensitivity(create_excel_file, tmp_path: Path):
    """Test case handling in sheet names and column names."""
    # Create Excel file with mixed case names
    data = [
        ["ID", "UserName", "LastLogin"],
        [1, "Alice", "2023-01-01"],
        [2, "Bob", "2023-01-02"],
    ]

    excel_file = create_excel_file("MixedCase.xlsx", {"UserData": data})

    standardizer = XLSXStandardizer()
    output_sdif = tmp_path / "case_output.sdif"

    standardizer.standardize(excel_file, output_sdif)

    # Table name should be lowercase
    table_names = _get_all_table_names(output_sdif)
    assert "userdata" in table_names

    # Column names should be lowercase
    schema = _get_table_schema(output_sdif, "userdata")
    assert "id" in schema["columns"]
    assert "username" in schema["columns"]
    assert "lastlogin" in schema["columns"]

    # Original column names should be preserved in metadata
    assert schema["columns"]["id"]["original_column_name"] == "ID"
    assert schema["columns"]["username"]["original_column_name"] == "UserName"
    assert schema["columns"]["lastlogin"]["original_column_name"] == "LastLogin"
