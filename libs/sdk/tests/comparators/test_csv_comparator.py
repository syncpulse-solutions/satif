import csv
from pathlib import Path
from typing import Any, List, Optional

import pytest

from satif_sdk.comparators.csv import CSVComparator


@pytest.fixture
def comparator() -> CSVComparator:
    return CSVComparator()


def create_csv_file(
    tmp_path: Path,
    file_name: str,
    header: Optional[List[str]],
    rows: Optional[List[List[Any]]],
    delimiter: str = ",",
) -> Path:
    file_path = tmp_path / file_name
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        if header:
            writer.writerow(header)
        if rows:
            writer.writerows(rows)
    return file_path


# --- Basic Equivalence Tests ---


def test_compare_identical_files(tmp_path: Path, comparator: CSVComparator):
    header = ["ID", "Name", "Value"]
    rows = [
        [1, "Alice", 100],
        [2, "Bob", 200],
    ]
    file1_path = create_csv_file(tmp_path, "file1.csv", header, rows)
    file2_path = create_csv_file(tmp_path, "file2.csv", header, rows)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is True
    assert "Files are considered equivalent" in result["summary"][0]
    assert result["details"]["header_comparison"]["result"] == "Identical"
    assert (
        result["details"]["row_comparison"]["result"]
        == f"Identical content (within {result['comparison_params']['decimal_places']} decimal places)"
    )


def test_compare_empty_files(tmp_path: Path, comparator: CSVComparator):
    file1_path = create_csv_file(tmp_path, "empty1.csv", None, None)
    file2_path = create_csv_file(tmp_path, "empty2.csv", None, None)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is True
    assert "Files are considered equivalent" in result["summary"][0]
    assert (
        result["details"]["row_comparison"]["result"]
        == f"Identical content (within {result['comparison_params']['decimal_places']} decimal places)"
    )  # Both have 0 rows


def test_compare_header_only_files_identical(tmp_path: Path, comparator: CSVComparator):
    header = ["ColA", "ColB"]
    file1_path = create_csv_file(tmp_path, "header1.csv", header, None)
    file2_path = create_csv_file(tmp_path, "header2.csv", header, None)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is True
    assert "Files are considered equivalent" in result["summary"][0]
    assert result["details"]["header_comparison"]["result"] == "Identical"
    assert (
        "Identical" in result["details"]["row_comparison"]["result"]
    )  # Both have 0 rows


# --- Tests for Comparison Parameters ---


def test_compare_ignore_row_order(tmp_path: Path, comparator: CSVComparator):
    header = ["ID", "Name"]
    rows1 = [[1, "Alice"], [2, "Bob"]]
    rows2 = [[2, "Bob"], [1, "Alice"]]
    file1_path = create_csv_file(tmp_path, "file1_order.csv", header, rows1)
    file2_path = create_csv_file(tmp_path, "file2_order.csv", header, rows2)

    # Default: ignore_row_order=True
    result_ignored = comparator.compare(file1_path, file2_path)
    assert result_ignored["are_equivalent"] is True
    assert (
        result_ignored["details"]["row_comparison"]["result"]
        == f"Identical content (within {result_ignored['comparison_params']['decimal_places']} decimal places)"
    )

    # Explicitly False: ignore_row_order=False (should fail due to NotImplementedError)
    with pytest.raises(
        NotImplementedError,
        match="Ordered comparison beyond count matching not fully implemented",
    ):
        comparator.compare(file1_path, file2_path, ignore_row_order=False)


def test_compare_check_header_order(tmp_path: Path, comparator: CSVComparator):
    header1 = ["Name", "ID"]
    header2 = ["ID", "Name"]
    rows = [[1, "Alice"]]
    file1_path = create_csv_file(tmp_path, "h_order1.csv", header1, rows)
    file2_path = create_csv_file(tmp_path, "h_order2.csv", header2, rows)

    # Default: check_header_order=True
    result_ordered = comparator.compare(file1_path, file2_path)
    assert result_ordered["are_equivalent"] is False
    assert (
        "Different names or order"
        in result_ordered["details"]["header_comparison"]["result"]
    )

    # Explicitly False: check_header_order=False
    result_unordered = comparator.compare(
        file1_path, file2_path, check_header_order=False
    )
    assert (
        result_unordered["are_equivalent"] is True
    )  # Rows are the same, headers have same names diff order
    assert (
        "Identical names (different order)"
        in result_unordered["details"]["header_comparison"]["result"]
    )


def test_compare_check_header_case(tmp_path: Path, comparator: CSVComparator):
    header1 = ["Name", "ID"]
    header2 = ["name", "id"]
    rows = [["Alice", 1]]
    file1_path = create_csv_file(tmp_path, "h_case1.csv", header1, rows)
    file2_path = create_csv_file(tmp_path, "h_case2.csv", header2, rows)

    # Default: check_header_case=True
    result_cs = comparator.compare(file1_path, file2_path)
    assert result_cs["are_equivalent"] is False
    assert (
        "Different names or order"
        in result_cs["details"]["header_comparison"]["result"]
    )

    # Explicitly False: check_header_case=False
    result_ci = comparator.compare(file1_path, file2_path, check_header_case=False)
    assert result_ci["are_equivalent"] is True
    assert (
        "Identical names/order (differs only by case)"
        in result_ci["details"]["header_comparison"]["result"]
    )


def test_compare_strip_whitespace(tmp_path: Path, comparator: CSVComparator):
    header1 = ["  ID  ", "Name"]
    rows1 = [["  1  ", "  Alice  "]]
    header2 = ["ID", "Name"]
    rows2 = [["1", "Alice"]]

    file1_path = create_csv_file(tmp_path, "ws1.csv", header1, rows1)
    file2_path = create_csv_file(tmp_path, "ws2.csv", header2, rows2)

    # Default: strip_whitespace=True
    result_stripped = comparator.compare(file1_path, file2_path)
    assert result_stripped["are_equivalent"] is True
    assert (
        result_stripped["details"]["row_comparison"]["result"]
        == f"Identical content (within {result_stripped['comparison_params']['decimal_places']} decimal places)"
    )

    # Explicitly False: strip_whitespace=False
    result_not_stripped = comparator.compare(
        file1_path, file2_path, strip_whitespace=False
    )
    assert result_not_stripped["are_equivalent"] is False
    assert (
        "Different names or order"
        in result_not_stripped["details"]["header_comparison"]["result"]
    )
    assert (
        result_not_stripped["details"]["row_comparison"]["result"]
        == "Not compared (header mismatch)"
    )
    # Row counts should still be available
    assert result_not_stripped["details"]["row_comparison"]["row_count1"] == 1
    assert result_not_stripped["details"]["row_comparison"]["row_count2"] == 1


def test_compare_decimal_places(tmp_path: Path, comparator: CSVComparator):
    header = ["Value1", "Value2"]
    rows1 = [[1.234, 5.678]]
    rows2 = [[1.23, 5.68]]  # Rounded versions
    file1_path = create_csv_file(tmp_path, "dec1.csv", header, rows1)
    file2_path = create_csv_file(tmp_path, "dec2.csv", header, rows2)

    # Default decimal_places=2
    result_default_dp = comparator.compare(file1_path, file2_path)
    assert result_default_dp["are_equivalent"] is True
    assert (
        "Identical content (within 2 decimal places)"
        in result_default_dp["details"]["row_comparison"]["result"]
    )

    # No rounding (decimal_places=None)
    result_no_dp = comparator.compare(file1_path, file2_path, decimal_places=None)
    assert result_no_dp["are_equivalent"] is False
    assert "Different content" in result_no_dp["details"]["row_comparison"]["result"]

    # Different decimal_places
    result_dp3 = comparator.compare(file1_path, file2_path, decimal_places=3)
    assert result_dp3["are_equivalent"] is False  # 1.234 vs 1.230 (when file2 is read)
    assert (
        "Different content (within 3 decimal places)"
        in result_dp3["details"]["row_comparison"]["result"]
    )

    # Test with non-numeric data (should not error, just compare as strings)
    rows_non_numeric1 = [["abc", "def"]]
    rows_non_numeric2 = [["abc", "def"]]
    file_nn1 = create_csv_file(tmp_path, "nn1.csv", ["ColA"], rows_non_numeric1)
    file_nn2 = create_csv_file(tmp_path, "nn2.csv", ["ColA"], rows_non_numeric2)
    result_nn = comparator.compare(file_nn1, file_nn2, decimal_places=2)
    assert result_nn["are_equivalent"] is True
    assert (
        "Identical content (within 2 decimal places)"
        in result_nn["details"]["row_comparison"]["result"]
    )


def test_compare_check_structure_only(tmp_path: Path, comparator: CSVComparator):
    header1 = ["ID", "Name"]
    rows1 = [[1, "Alice"]]
    header2 = ["ID", "Name"]
    rows2 = [[2, "Bob"]]
    header3 = ["ID", "Value"]

    file1_path = create_csv_file(tmp_path, "struct1.csv", header1, rows1)
    file2_path = create_csv_file(
        tmp_path, "struct2.csv", header2, rows2
    )  # Same header, diff rows
    file3_path = create_csv_file(tmp_path, "struct3.csv", header3, rows1)  # Diff header

    # Equivalent structure, different rows
    result_eq_struct = comparator.compare(
        file1_path, file2_path, check_structure_only=True
    )
    assert result_eq_struct["are_equivalent"] is True
    assert (
        "Skipped (check_structure_only enabled)"
        in result_eq_struct["details"]["row_comparison"]["result"]
    )

    # Different structure
    result_diff_struct = comparator.compare(
        file1_path, file3_path, check_structure_only=True
    )
    assert result_diff_struct["are_equivalent"] is False
    assert (
        "Different names or order"
        in result_diff_struct["details"]["header_comparison"]["result"]
    )
    assert (
        "Skipped (check_structure_only enabled)"
        in result_diff_struct["details"]["row_comparison"]["result"]
    )


def test_compare_different_delimiters(tmp_path: Path, comparator: CSVComparator):
    header = ["A", "B"]
    rows = [[1, 2]]
    file_comma_path = create_csv_file(
        tmp_path, "comma.csv", header, rows, delimiter=","
    )
    file_semi_path = create_csv_file(tmp_path, "semi.csv", header, rows, delimiter=";")

    # Auto-detect (should work)
    result_auto = comparator.compare(
        file_comma_path, file_semi_path
    )  # Delimiter is None by default
    assert result_auto["are_equivalent"] is True
    assert result_auto["comparison_params"]["delimiter"] is None  # Shows it was auto

    # Explicitly wrong delimiter for one (e.g. trying to read semi with comma)
    # This is tricky as _read_data has fallback. The test for _read_data should be more specific.
    # For compare, if sniffing fails and defaults to comma, it might misinterpret.
    # Let's assume sniffing works or the default matches one file.

    # If we force a delimiter that only works for one file, they will likely appear different
    # because the other file will be parsed as a single column (or incorrectly).
    header_single_col_expected = ["A;B"]
    rows_single_col_expected = [["1;2"]]

    # Create a scenario where forcing comma on semicolon file makes it look different
    result_forced_comma_on_semi = comparator.compare(
        file_comma_path, file_semi_path, delimiter=","
    )
    assert (
        result_forced_comma_on_semi["are_equivalent"] is False
    )  # file_semi will be read as one col
    assert (
        "Different column count"
        in result_forced_comma_on_semi["details"]["header_comparison"]["result"]
    )


# --- Basic Non-Equivalence Tests ---


def test_compare_different_header_names(tmp_path: Path, comparator: CSVComparator):
    header1 = ["ID", "Name"]
    header2 = ["ID", "Value"]
    rows = [[1, "Alice"]]
    file1_path = create_csv_file(tmp_path, "h_diff1.csv", header1, rows)
    file2_path = create_csv_file(tmp_path, "h_diff2.csv", header2, rows)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is False
    assert (
        "Different names or order" in result["details"]["header_comparison"]["result"]
    )
    assert (
        "File 1 'Name' != File 2 'Value'"
        in result["details"]["header_comparison"]["diff"][0]
    )


def test_compare_different_column_count(tmp_path: Path, comparator: CSVComparator):
    header1 = ["ID", "Name"]
    header2 = ["ID", "Name", "Age"]
    rows = [[1, "Alice"]]
    file1_path = create_csv_file(tmp_path, "cc1.csv", header1, rows)
    file2_path = create_csv_file(tmp_path, "cc2.csv", header2, rows)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is False
    assert "Different column count" in result["details"]["header_comparison"]["result"]


def test_compare_different_row_content_unique_rows(
    tmp_path: Path, comparator: CSVComparator
):
    header = ["ID", "Name"]
    rows1 = [[1, "Alice"], [2, "Bob"]]
    rows2 = [[1, "Alice"], [3, "Charlie"]]
    file1_path = create_csv_file(tmp_path, "rc1.csv", header, rows1)
    file2_path = create_csv_file(tmp_path, "rc2.csv", header, rows2)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is False
    assert "Different content" in result["details"]["row_comparison"]["result"]
    assert len(result["details"]["row_comparison"]["unique_rows1"]) == 1
    assert result["details"]["row_comparison"]["unique_rows1"][0] == [2, "Bob"]
    assert len(result["details"]["row_comparison"]["unique_rows2"]) == 1
    assert result["details"]["row_comparison"]["unique_rows2"][0] == [3, "Charlie"]


def test_compare_different_row_counts(tmp_path: Path, comparator: CSVComparator):
    header = ["ID"]
    rows1 = [[1], [2]]
    rows2 = [[1], [2], [3]]
    file1_path = create_csv_file(tmp_path, "rcount1.csv", header, rows1)
    file2_path = create_csv_file(tmp_path, "rcount2.csv", header, rows2)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is False
    assert "Different content" in result["details"]["row_comparison"]["result"]
    assert result["details"]["row_comparison"]["row_count1"] == 2
    assert result["details"]["row_comparison"]["row_count2"] == 3
    assert any("Total row counts differ" in s for s in result["summary"])


def test_compare_different_row_occurrence_counts(
    tmp_path: Path, comparator: CSVComparator
):
    header = ["ID"]
    rows1 = [[1], [1], [2]]  # ID 1 appears twice
    rows2 = [[1], [2], [2]]  # ID 2 appears twice
    file1_path = create_csv_file(tmp_path, "rocc1.csv", header, rows1)
    file2_path = create_csv_file(tmp_path, "rocc2.csv", header, rows2)

    result = comparator.compare(file1_path, file2_path)
    assert result["are_equivalent"] is False
    assert "Different content" in result["details"]["row_comparison"]["result"]
    assert len(result["details"]["row_comparison"]["count_diffs"]) > 0

    found_id1_diff = any(
        d["row"] == [1] and d["count1"] == 2 and d["count2"] == 1
        for d in result["details"]["row_comparison"]["count_diffs"]
    )
    found_id2_diff = any(
        d["row"] == [2] and d["count1"] == 1 and d["count2"] == 2
        for d in result["details"]["row_comparison"]["count_diffs"]
    )
    assert (
        found_id1_diff or found_id2_diff
    )  # At least one should be identified this way by the current logic


# --- Error Handling and Edge Cases ---


def test_compare_file_not_found(tmp_path: Path, comparator: CSVComparator):
    header = ["ID"]
    rows = [[1]]
    file1_path = create_csv_file(tmp_path, "exists.csv", header, rows)
    non_existent_path = tmp_path / "not_exists.csv"

    result = comparator.compare(file1_path, non_existent_path)
    assert result["are_equivalent"] is False
    assert "Files are considered different" in result["summary"][0]
    assert len(result["details"]["errors"]) == 1
    assert "File not found" in result["details"]["errors"][0]


def test_compare_one_file_empty_one_not(tmp_path: Path, comparator: CSVComparator):
    header = ["ID"]
    rows = [[1]]
    file_empty_path = create_csv_file(tmp_path, "empty_comp.csv", None, None)
    file_content_path = create_csv_file(tmp_path, "content_comp.csv", header, rows)

    result = comparator.compare(file_empty_path, file_content_path)
    assert result["are_equivalent"] is False
    assert (
        "Headers differ" in result["summary"][1]
    )  # summary[0] is about overall difference
    assert "File 1 has no header" in result["details"]["header_comparison"]["diff"][0]


def test_compare_ragged_rows_adapted(tmp_path: Path, comparator: CSVComparator):
    # File1 has a consistent structure
    header1 = ["ID", "Name", "Value"]
    rows1 = [
        ["1", "Alice", "100"],
        ["2", "Bob", "200"],
    ]
    file1_path = create_csv_file(tmp_path, "ragged_base.csv", header1, rows1)

    # File2 has a ragged row (fewer columns) and one with more (should be truncated)
    file_path_ragged = tmp_path / "ragged.csv"
    with open(file_path_ragged, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Name", "Value"])  # Header
        writer.writerow(["1", "Alice", "100"])  # Correct row
        writer.writerow(["2", "Bob"])  # Ragged row (missing one)
        writer.writerow(["3", "Charlie", "300", "Extra"])  # Ragged row (extra one)

    # Default comparison (ignore_row_order=True)
    # The comparator should adapt ragged rows: pad missing, truncate extra.
    # So, row ["2", "Bob"] becomes ["2", "Bob", ""], row ["3", "Charlie", "300", "Extra"] becomes ["3", "Charlie", "300"]
    # File1: (1, Alice, 100), (2, Bob, 200)
    # File2: (1, Alice, 100), (2, Bob, ""), (3, Charlie, 300)
    result = comparator.compare(file1_path, file_path_ragged)
    assert result["are_equivalent"] is False
    assert "Different content" in result["details"]["row_comparison"]["result"]

    # Check unique rows based on adaptation
    # file1 has (2,Bob,200) which is not in adapted file2
    # file2 has (2,Bob,"") and (3,Charlie,300) which are not in file1
    assert any(
        row == [2.0, "Bob", 200.0]
        for row in result["details"]["row_comparison"]["unique_rows1"]
    )
    assert any(
        row == [2.0, "Bob", ""]
        for row in result["details"]["row_comparison"]["unique_rows2"]
    )
    assert any(
        row == [3.0, "Charlie", 300.0]
        for row in result["details"]["row_comparison"]["unique_rows2"]
    )
    assert result["details"]["row_comparison"]["row_count1"] == 2
    assert result["details"]["row_comparison"]["row_count2"] == 3
