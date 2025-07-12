import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from aspose.cells import (
    AutoFillType,
    CellArea,
    CellsHelper,
    CellValueType,
    FindOptions,
    LookAtType,
    PasteOptions,
    PasteType,
    ReplaceOptions,
    Workbook,
    Worksheet,
)
from typing_extensions import Literal

from xlsx_to_sdif.spreadsheet.base import SpreadsheetManager

logger = logging.getLogger(__name__)


class _WorkbookManager:
    """Manages loading, caching, and closing of Aspose.Cells Workbook objects."""

    def __init__(self):
        self._cached_workbooks: Dict[str, Workbook] = {}

    def get_workbook(self, file_path: str) -> Workbook:
        """
        Retrieves a workbook from the cache or loads it from the file_path.
        Ensures the workbook's file_name attribute is set.
        """
        if file_path not in self._cached_workbooks:
            try:
                workbook = Workbook(file_path)
                # Ensure the workbook object knows its path, critical for saving
                if (
                    not workbook.file_name
                ):  # Workbook might be created in memory first then path associated
                    workbook.file_name = file_path
                self._cached_workbooks[file_path] = workbook
            except Exception as e:
                raise RuntimeError(f"Error loading workbook '{file_path}': {e}") from e
        else:
            # Ensure workbook.file_name is still correctly set if it was somehow cleared
            if not self._cached_workbooks[file_path].file_name:
                self._cached_workbooks[file_path].file_name = file_path
        return self._cached_workbooks[file_path]

    def close_workbook(self, file_path: str) -> None:
        """Closes a specific workbook and removes it from the cache."""
        if file_path in self._cached_workbooks:
            try:
                self._cached_workbooks[file_path].close()
                del self._cached_workbooks[file_path]
            except Exception as e:
                logger.error(f"Error removing workbook '{file_path}' from cache: {e}")

    def close_all_workbooks(self) -> None:
        """Closes all cached workbooks."""
        if not self._cached_workbooks:
            return

        paths_to_close = list(self._cached_workbooks.keys())
        for file_path in paths_to_close:
            self.close_workbook(file_path)


# Global instance of the WorkbookManager
workbook_manager = _WorkbookManager()


def get_workbook(file_path: str) -> Workbook:
    return workbook_manager.get_workbook(file_path)


class AsposeCellsManager(SpreadsheetManager):
    """Concrete implementation of SpreadsheetManager for Aspose.Cells."""

    def __init__(self, workbook: Workbook):
        """Initialize the Aspose.Cells manager.

        Args:
            workbook: The Aspose.Cells Workbook object.
        """
        if not isinstance(workbook, Workbook):
            raise TypeError("workbook must be an instance of aspose.cells.Workbook")
        self.workbook = workbook

    def _get_worksheet(self, sheet_name_or_index: Union[str, int]) -> Worksheet:
        """Helper to get a worksheet by name or index."""
        try:
            if isinstance(sheet_name_or_index, int):
                if 0 <= sheet_name_or_index < len(self.workbook.worksheets):
                    return self.workbook.worksheets[sheet_name_or_index]
                else:
                    raise IndexError(
                        f"Sheet index {sheet_name_or_index} out of bounds."
                    )
            elif isinstance(sheet_name_or_index, str):
                worksheet = self.workbook.worksheets.get(sheet_name_or_index)
                if worksheet is None:
                    raise ValueError(f"Sheet '{sheet_name_or_index}' not found.")
                return worksheet
            else:
                raise TypeError("sheet_name_or_index must be str or int.")
        except Exception as e:
            # Log or re-raise with more context if needed
            raise RuntimeError(
                f"Error accessing worksheet '{sheet_name_or_index}': {e}"
            ) from e

    def _parse_range_spec(
        self, range_spec: str
    ) -> Tuple[Worksheet, int, int, int, int]:
        """Parses range_spec like 'Sheet1!A1:B5' into worksheet and 0-based indices."""
        sheet_name = self.extract_sheet_name_from_range(range_spec)
        worksheet = self._get_worksheet(sheet_name)
        range_part = (
            range_spec.split("!")[1] if "!" in range_spec else range_spec
        )  # Handle ranges without sheet name

        start_row, start_col, end_row, end_col = -1, -1, -1, -1

        if ":" in range_part:
            start_cell_str, end_cell_str = range_part.split(":")
            start_col_letter, start_row_1based = self._parse_cell(start_cell_str)
            end_col_letter, end_row_1based = self._parse_cell(end_cell_str)
            start_row = start_row_1based - 1
            start_col = self.col_letter_to_index(start_col_letter)
            end_row = end_row_1based - 1
            end_col = self.col_letter_to_index(end_col_letter)
        else:  # Single cell
            col_letter, row_1based = self._parse_cell(range_part)
            start_row = row_1based - 1
            start_col = self.col_letter_to_index(col_letter)
            end_row = start_row
            end_col = start_col

        return worksheet, start_row, start_col, end_row, end_col

    def add_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Add values to a specified range in the spreadsheet."""
        try:
            worksheet, start_row, start_col, _, _ = self._parse_range_spec(range_spec)
            # import_object_array expects 0-based row/col, which _parse_range_spec provides
            worksheet.cells.import_object_array(values, start_row, start_col, False)
        except Exception as e:
            raise RuntimeError(
                f"Error adding values to range '{range_spec}': {e}"
            ) from e

    def update_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Update values in a specified range in the spreadsheet."""
        # In Aspose, add_values effectively overwrites, so it acts as update.
        self.add_values(range_spec, values)

    def delete_values(self, range_spec: str) -> None:
        """Delete values in a specified range in the spreadsheet."""
        try:
            worksheet, start_row, start_col, end_row, end_col = self._parse_range_spec(
                range_spec
            )
            worksheet.cells.clear_contents(start_row, start_col, end_row, end_col)
        except Exception as e:
            raise RuntimeError(
                f"Error deleting values from range '{range_spec}': {e}"
            ) from e

    def insert_rows(
        self,
        sheet_name: str,
        start_row: int,
        count: int,
        values: Optional[List[List[Any]]] = None,
    ) -> None:
        """Insert rows starting at the specified row index."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            worksheet.cells.insert_rows(start_row, count)
            if values:
                # Need to determine the starting column for inserting values.
                # Assuming insertion starts at column A (index 0).
                # If a different behavior is needed, the interface might need adjustment.
                start_col = 0
                worksheet.cells.import_object_array(values, start_row, start_col, False)
        except Exception as e:
            raise RuntimeError(
                f"Error inserting {count} rows at {start_row} in sheet '{sheet_name}': {e}"
            ) from e

    def delete_rows(self, sheet_name: str, start_row: int, count: int) -> None:
        """Delete rows starting at the specified row index."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            worksheet.cells.delete_rows(start_row, count)
        except Exception as e:
            raise RuntimeError(
                f"Error deleting {count} rows from {start_row} in sheet '{sheet_name}': {e}"
            ) from e

    def insert_columns(
        self,
        sheet_name: str,
        start_column: int,
        count: int,
        values: Optional[List[List[Any]]] = None,
    ) -> None:
        """Insert columns starting at the specified column index."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            worksheet.cells.insert_columns(start_column, count)
            if values:
                # Need to determine the starting row for inserting values.
                # Assuming insertion starts at row 1 (index 0).
                start_row = 0
                # import_object_array with is_vertical=True expects data where
                # the outer list represents columns and inner list represents rows.
                # If the input `values` is row-major (list of rows), we need to transpose.
                # Let's assume input `values` is row-major based on add_values.
                if values and values[0]:  # Check if values is not empty
                    num_rows_in_data = len(values)
                    num_cols_in_data = len(values[0])
                    # Transpose if necessary
                    if num_cols_in_data == count:  # Data likely row-major for columns
                        transposed_values = [[] for _ in range(num_cols_in_data)]
                        for r_idx in range(num_rows_in_data):
                            for c_idx in range(num_cols_in_data):
                                if c_idx < len(values[r_idx]):
                                    transposed_values[c_idx].append(
                                        values[r_idx][c_idx]
                                    )
                                else:
                                    transposed_values[c_idx].append(
                                        None
                                    )  # Handle jagged arrays
                        worksheet.cells.import_object_array(
                            transposed_values, start_row, start_column, True
                        )
                    elif num_rows_in_data == count:  # Data likely already column-major
                        worksheet.cells.import_object_array(
                            values, start_row, start_column, True
                        )
                    else:
                        logger.warning(
                            f"Warning: Dimensions of provided values ({num_rows_in_data}x{num_cols_in_data}) do not match inserted columns ({count}). Skipping value insertion."
                        )

        except Exception as e:
            raise RuntimeError(
                f"Error inserting {count} columns at {start_column} in sheet '{sheet_name}': {e}"
            ) from e

    def delete_columns(self, sheet_name: str, start_column: int, count: int) -> None:
        """Delete columns starting at the specified column index."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            worksheet.cells.delete_columns(start_column, count, update_reference=True)
        except Exception as e:
            raise RuntimeError(
                f"Error deleting {count} columns from {start_column} in sheet '{sheet_name}': {e}"
            ) from e

    def merge_cells(self, range_spec: str, sheet_name: str | None = None) -> None:
        """Merge cells in the specified range."""
        try:
            worksheet, start_row, start_col, end_row, end_col = self._parse_range_spec(
                range_spec
            )
            # Check if it's a single cell before merging
            if start_row == end_row and start_col == end_col:
                raise ValueError("Cannot merge a single cell.")

            total_rows = end_row - start_row + 1
            total_cols = end_col - start_col + 1
            worksheet.cells.merge(start_row, start_col, total_rows, total_cols)
        except Exception as e:
            raise RuntimeError(
                f"Error merging cells in range '{range_spec}': {e}"
            ) from e

    def unmerge_cells(self, range_spec: str, sheet_name: str | None = None) -> None:
        """Unmerge cells in the specified range."""
        try:
            # _parse_range_spec already handles sheet name extraction and validation
            worksheet, start_row, start_col, end_row, end_col = self._parse_range_spec(
                range_spec
            )
            # Check if it's a single cell before unmerging
            if start_row == end_row and start_col == end_col:
                raise ValueError("Cannot unmerge a single cell.")

            total_rows = end_row - start_row + 1
            total_cols = end_col - start_col + 1
            worksheet.cells.un_merge(start_row, start_col, total_rows, total_cols)
        except Exception as e:
            raise RuntimeError(
                f"Error unmerging cells in range '{range_spec}': {e}"
            ) from e

    def copy_paste(
        self,
        source_range: str,
        destination_range: str,
        paste_type: Literal[
            "PASTE_NORMAL", "PASTE_VALUES", "PASTE_FORMAT"
        ] = "PASTE_NORMAL",
    ) -> None:
        """Copy and paste content from source range to destination range."""
        try:
            src_sheet_name = self.extract_sheet_name_from_range(source_range)
            dest_sheet_name = self.extract_sheet_name_from_range(destination_range)
            src_worksheet = self._get_worksheet(src_sheet_name)
            dest_worksheet = self._get_worksheet(dest_sheet_name)

            # Use _parse_range_spec to get source range details for row/column count
            _, src_start_row, src_start_col, src_end_row, src_end_col = (
                self._parse_range_spec(source_range)
            )
            src_row_count = src_end_row - src_start_row + 1
            src_col_count = src_end_col - src_start_col + 1

            # Use _parse_range_spec for destination start cell indices only
            # Note: destination_range might be just a single cell like 'Sheet2!C3'
            _, dest_start_row, dest_start_col, _, _ = self._parse_range_spec(
                destination_range
            )

            # Create Aspose Range objects using indices and counts
            src_range_obj = src_worksheet.cells.create_range(
                src_start_row, src_start_col, src_row_count, src_col_count
            )
            dest_range_obj = dest_worksheet.cells.create_range(
                dest_start_row, dest_start_col, src_row_count, src_col_count
            )

            paste_options = PasteOptions()
            if paste_type == "PASTE_NORMAL":
                paste_options.paste_type = PasteType.ALL
            elif paste_type == "PASTE_VALUES":
                paste_options.paste_type = PasteType.VALUES
            elif paste_type == "PASTE_FORMAT":
                paste_options.paste_type = PasteType.FORMATS
            else:
                raise ValueError(f"Unsupported paste_type: {paste_type}")

            dest_range_obj.copy(src_range_obj, paste_options)
        except Exception as e:
            raise RuntimeError(
                f"Error copying from '{source_range}' to '{destination_range}': {e}"
            ) from e

    def find_replace(
        self,
        find: str,
        replace: str,
        range_spec: Optional[str] = None,
        match_case: bool = False,
        match_entire_cell: bool = False,
    ) -> int:
        """Find and replace content in the spreadsheet."""
        try:
            find_options = FindOptions()
            find_options.case_sensitive = match_case
            find_options.look_at_type = (
                LookAtType.ENTIRE_CONTENT if match_entire_cell else LookAtType.CONTAINS
            )

            replace_options = ReplaceOptions()
            replace_options.case_sensitive = match_case
            replace_options.match_entire_cell_contents = match_entire_cell

            total_replaced = 0
            if range_spec:
                worksheet, start_row, start_col, end_row, end_col = (
                    self._parse_range_spec(range_spec)
                )
                cell_area = CellArea()
                cell_area.start_row = start_row
                cell_area.start_column = start_col
                cell_area.end_row = end_row
                cell_area.end_column = end_col
                find_options.set_range(cell_area)
                total_replaced = worksheet.cells.replace(find, replace, replace_options)
            else:
                # Replace across all worksheets if no range is specified
                for worksheet in self.workbook.worksheets:
                    total_replaced += worksheet.cells.replace(
                        find, replace, replace_options
                    )
            return total_replaced
        except Exception as e:
            raise RuntimeError(
                f"Error finding '{find}' and replacing with '{replace}': {e}"
            ) from e

    def read_cells(
        self,
        ranges: Union[str, List[str]],
        major_dimension: Literal["ROWS", "COLUMNS"] = "ROWS",
        value_render_option: Literal[
            "FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"
        ] = "FORMATTED_VALUE",
        date_time_render_option: Literal[
            "SERIAL_NUMBER", "FORMATTED_STRING"
        ] = "SERIAL_NUMBER",
    ) -> Union[Dict[str, List[List[Any]]], List[List[Any]]]:
        """Read values from one or multiple cells/ranges in the spreadsheet."""
        single_range_input = isinstance(ranges, str)
        range_list = [ranges] if single_range_input else ranges
        results = {}

        try:
            for range_spec in range_list:
                worksheet, start_row, start_col, end_row, end_col = (
                    self._parse_range_spec(range_spec)
                )
                num_rows = end_row - start_row + 1
                num_cols = end_col - start_col + 1

                # Initialize based on major dimension
                if major_dimension == "ROWS":
                    range_data = [
                        [None for _ in range(num_cols)] for _ in range(num_rows)
                    ]
                else:  # COLUMNS
                    range_data = [
                        [None for _ in range(num_rows)] for _ in range(num_cols)
                    ]

                for r_offset in range(num_rows):
                    for c_offset in range(num_cols):
                        current_row = start_row + r_offset
                        current_col = start_col + c_offset
                        cell = worksheet.cells.check_cell(current_row, current_col)
                        cell_value = None

                        if cell:
                            if value_render_option == "FORMULA":
                                cell_value = (
                                    cell.formula if cell.is_formula else cell.value
                                )
                            elif value_render_option == "FORMATTED_VALUE":
                                cell_value = (
                                    cell.string_value
                                )  # Aspose's string_value is formatted
                                # Handle date/time specifically if needed for FORMATTED_VALUE
                                if (
                                    cell.type == CellValueType.IS_DATE_TIME
                                    and date_time_render_option == "SERIAL_NUMBER"
                                ):
                                    cell_value = cell.double_value  # Get serial number
                            elif value_render_option == "UNFORMATTED_VALUE":
                                cell_value = cell.value  # Raw value
                                # Handle date/time specifically for UNFORMATTED_VALUE
                                if (
                                    cell.type == CellValueType.IS_DATE_TIME
                                    and date_time_render_option == "FORMATTED_STRING"
                                ):
                                    cell_value = (
                                        cell.string_value
                                    )  # Get formatted string
                            else:  # Default to formatted value
                                cell_value = cell.string_value
                                if (
                                    cell.type == CellValueType.IS_DATE_TIME
                                    and date_time_render_option == "SERIAL_NUMBER"
                                ):
                                    cell_value = cell.double_value

                        # Assign to the correct position based on major dimension
                        if major_dimension == "ROWS":
                            range_data[r_offset][c_offset] = cell_value
                        else:  # COLUMNS
                            range_data[c_offset][r_offset] = cell_value

                results[range_spec] = range_data

            return results[ranges] if single_range_input else results

        except Exception as e:
            raise RuntimeError(
                f"Error reading cells from ranges '{ranges}': {e}"
            ) from e

    def move_columns(
        self,
        sheet_name: str,
        source_start_column: int,
        source_end_column: int,  # Exclusive in abstract class
        destination_index: int,
    ) -> None:
        """Move columns from one position to another."""
        # Aspose.Cells does not have a direct 'move dimension' function like Google Sheets.
        # Implementing this requires a complex insert/copy/delete sequence,
        # which is prone to errors with reference updates and overlapping ranges.
        # A full, robust implementation is complex and beyond a simple mapping.
        # Raising NotImplementedError is safer than providing a potentially buggy implementation.
        # If this functionality is critical, further investigation into Aspose's specific
        # best practices for this scenario (perhaps involving temporary sheets or specific copy flags)
        # would be needed.
        logger.warning(
            "Warning: move_columns is complex to implement reliably with Aspose.Cells and is not fully supported by this manager."
        )
        # Basic (potentially problematic) sketch:
        try:
            worksheet = self._get_worksheet(sheet_name)
            num_cols_to_move = source_end_column - source_start_column
            if num_cols_to_move <= 0:
                return  # Nothing to move

            # --- This logic needs careful review for index adjustments ---
            # 1. Insert space at destination
            # worksheet.cells.insert_columns(destination_index, num_cols_to_move)

            # # 2. Adjust source index if destination was before source
            # adjusted_source_start = source_start_column
            # if destination_index <= source_start_column:
            #     adjusted_source_start += num_cols_to_move

            # # 3. Copy columns
            # copy_options = CopyOptions() # Configure as needed
            # paste_options = PasteOptions(paste_type=PasteType.ALL) # Configure as needed
            # worksheet.cells.copy_columns(worksheet.cells, adjusted_source_start, destination_index, num_cols_to_move, copy_options, paste_options)

            # # 4. Delete original columns
            # delete_options = DeleteOptions() # Configure as needed
            # worksheet.cells.delete_columns(adjusted_source_start, num_cols_to_move, delete_options)
            # --- End of complex logic sketch ---

            raise NotImplementedError(
                "move_columns requires a complex implementation with Aspose.Cells."
            )

        except Exception as e:
            raise RuntimeError(
                f"Error moving columns in sheet '{sheet_name}': {e}"
            ) from e

    def move_rows(
        self,
        sheet_name: str,
        source_start_row: int,
        source_end_row: int,  # Exclusive in abstract class
        destination_index: int,
    ) -> None:
        """Move rows from one position to another."""
        # Similar complexity to move_columns.
        logger.warning(
            "Warning: move_rows is complex to implement reliably with Aspose.Cells and is not fully supported by this manager."
        )
        try:
            worksheet = self._get_worksheet(sheet_name)
            num_rows_to_move = source_end_row - source_start_row
            if num_rows_to_move <= 0:
                return

            # --- Complex logic sketch (similar issues as move_columns) ---
            # worksheet.cells.insert_rows(destination_index, num_rows_to_move)
            # adjusted_source_start = source_start_row
            # if destination_index <= source_start_row:
            #     adjusted_source_start += num_rows_to_move
            # copy_options = CopyOptions()
            # paste_options = PasteOptions(paste_type=PasteType.ALL)
            # worksheet.cells.copy_rows(worksheet.cells, adjusted_source_start, destination_index, num_rows_to_move, copy_options, paste_options)
            # delete_options = DeleteOptions()
            # worksheet.cells.delete_rows(adjusted_source_start, num_rows_to_move, delete_options)
            # --- End complex logic sketch ---

            raise NotImplementedError(
                "move_rows requires a complex implementation with Aspose.Cells."
            )

        except Exception as e:
            raise RuntimeError(f"Error moving rows in sheet '{sheet_name}': {e}") from e

    def auto_resize_columns(
        self,
        sheet_name: str,
        start_column: int,
        end_column: int,
    ) -> None:
        """Auto-resize columns based on content."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            if start_column >= end_column:
                return  # No columns to resize
            # Aspose auto_fit_columns uses inclusive last index
            worksheet.auto_fit_columns(start_column, end_column - 1)
        except Exception as e:
            raise RuntimeError(
                f"Error auto-resizing columns {start_column}-{end_column - 1} in sheet '{sheet_name}': {e}"
            ) from e

    def auto_resize_rows(self, sheet_name: str, start_row: int, end_row: int) -> None:
        """Auto-resize rows based on content."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            if start_row >= end_row:
                return  # No rows to resize
            # Aspose auto_fit_rows uses inclusive last index
            worksheet.auto_fit_rows(start_row, end_row - 1)
        except Exception as e:
            raise RuntimeError(
                f"Error auto-resizing rows {start_row}-{end_row - 1} in sheet '{sheet_name}': {e}"
            ) from e

    def change_sheet(self, sheet_name: str) -> None:
        """Change the active sheet in the spreadsheet."""
        try:
            worksheet = self._get_worksheet(sheet_name)
            self.workbook.worksheets.active_sheet_index = worksheet.index
        except Exception as e:
            raise RuntimeError(
                f"Error changing active sheet to '{sheet_name}': {e}"
            ) from e

    def display_only_range(self, range_spec: str, max_row: int, max_col: int) -> None:
        """Display a range in the spreadsheet by hiding others."""
        try:
            worksheet, start_row, start_col, end_row, end_col = self._parse_range_spec(
                range_spec
            )

            # Hide rows before the range
            if start_row > 0:
                worksheet.cells.hide_rows(0, start_row)

            # Hide rows after the range
            if end_row + 1 < max_row:
                # Aspose hide_rows takes start index and *count*
                count_after = max_row - (end_row + 1)
                if count_after > 0:
                    worksheet.cells.hide_rows(end_row + 1, count_after)
            elif end_row + 1 == max_row:
                # If the range ends exactly at max_row-1, nothing to hide after
                pass
            else:  # end_row + 1 > max_row - should not happen if max_row is correct
                logger.warning(
                    f"Warning: end_row {end_row} seems beyond max_row {max_row} in display_only_range"
                )

            # Hide columns before the range
            if start_col > 0:
                worksheet.cells.hide_columns(0, start_col)

            # Hide columns after the range
            if end_col + 1 < max_col:
                # Aspose hide_columns takes start index and *count*
                count_after = max_col - (end_col + 1)
                if count_after > 0:
                    worksheet.cells.hide_columns(end_col + 1, count_after)
            elif end_col + 1 == max_col:
                pass  # Range ends exactly at max_col-1
            else:
                logger.warning(
                    f"Warning: end_col {end_col} seems beyond max_col {max_col} in display_only_range"
                )

        except Exception as e:
            raise RuntimeError(
                f"Error displaying only range '{range_spec}': {e}"
            ) from e

    # --- Aspose Specific Helper ---
    def col_letter_to_index(self, col_letters: str) -> int:
        """Convert column letters (e.g., 'A', 'BC') to a 0-based column index using Aspose helper."""
        try:
            return CellsHelper.column_name_to_index(col_letters)
        except Exception as e:
            raise ValueError(f"Invalid column letters '{col_letters}': {e}") from e

    def col_index_to_letter(self, col_index: int) -> str:
        """Convert a 0-based column index to column letters using Aspose helper."""
        if col_index < 0:
            raise ValueError("Column index must be non-negative")
        try:
            return CellsHelper.column_index_to_name(col_index)
        except Exception as e:
            raise ValueError(f"Invalid column index '{col_index}': {e}") from e

    def _parse_cell(self, cell_ref: str) -> Tuple[str, int]:
        """Parse a cell reference (e.g., 'A1') into column letter and 1-based row number
        using Python string manipulation.
        """
        # Find the boundary between letters and numbers
        match = re.match(r"([A-Za-z]+)([1-9]\d*)", cell_ref)
        if not match:
            raise ValueError(f"Invalid cell reference format: {cell_ref}")

        col_letters = match.group(1).upper()
        row_num = int(match.group(2))

        # This method returns the column letters and the 1-based row number directly
        return col_letters, row_num

    # Override base class helper to handle quoted sheet names correctly
    def extract_sheet_name_from_range(self, range_spec: str) -> str:
        """Extract sheet name from a range specification, handling quoted names."""
        if "!" not in range_spec:
            # If no sheet name, assume the active sheet.
            # Get the active sheet index and then its name.
            active_index = self.workbook.worksheets.active_sheet_index
            if 0 <= active_index < len(self.workbook.worksheets):
                return self.workbook.worksheets[active_index].name
            else:
                # Fallback if active index is invalid (shouldn't normally happen)
                raise ValueError(
                    "Could not determine active sheet for range spec without sheet name."
                )

        sheet_name_part = range_spec.split("!")[0]
        # Remove single quotes if they exist
        if sheet_name_part.startswith("'") and sheet_name_part.endswith("'"):
            return sheet_name_part[1:-1]
        return sheet_name_part

    def get_sheets_metadata(self) -> Dict[str, Any]:
        """Get metadata about all sheets in the workbook.

        Returns:
            Dict: A dictionary containing workbook identifier, title, and sheet metadata.
        """
        try:
            sheets_data = []
            for sheet in self.workbook.worksheets:
                sheets_data.append(
                    {
                        "title": sheet.name,
                        "index": sheet.index,  # Aspose uses 0-based index
                        "max_row": sheet.cells.max_data_row + 1,
                        "max_col": sheet.cells.max_data_column,
                    }
                )

            # Attempt to get title from properties, fallback to filename
            workbook_title = ""
            try:
                # Accessing by string key might raise KeyError or other issues if property doesn't exist
                title_prop = self.workbook.built_in_document_properties.get("Title")
                if title_prop and title_prop.value:
                    workbook_title = str(title_prop.value)
                elif self.workbook.file_name:
                    # Use filename without extension as a fallback title
                    workbook_title = os.path.splitext(
                        os.path.basename(self.workbook.file_name)
                    )[0]
                else:
                    workbook_title = "Untitled Workbook"  # Default if no info
            except Exception:  # Catch potential issues accessing properties
                if self.workbook.file_name:
                    workbook_title = os.path.splitext(
                        os.path.basename(self.workbook.file_name)
                    )[0]
                else:
                    workbook_title = "Untitled Workbook"

            spreadsheet_state = {
                "title": workbook_title,
                "sheets": sheets_data,
            }

            return spreadsheet_state
        except Exception as e:
            raise RuntimeError(f"Error getting sheets metadata: {e}") from e

    def auto_fill(
        self,
        source_range: str,
        destination_range: str,
        fill_type: Literal[
            "COPY",
            "SERIES",
            "FORMATS",
            "VALUES",
            "DEFAULT",
        ] = "DEFAULT",
    ) -> None:
        """Auto-fill a destination range based on the data in a source range using Aspose.Cells.

        Args:
            source_range: The source range string (e.g., 'Sheet1!A1:A2').
            destination_range: The target range string (e.g., 'Sheet1!A1:A10').
            fill_type: The type of auto-fill operation.
        """
        try:
            # Parse source range
            src_sheet_name = self.extract_sheet_name_from_range(source_range)
            src_worksheet = self._get_worksheet(src_sheet_name)
            _, src_start_row, src_start_col, src_end_row, src_end_col = (
                self._parse_range_spec(source_range)
            )
            src_row_count = src_end_row - src_start_row + 1
            src_col_count = src_end_col - src_start_col + 1
            source_range_obj = src_worksheet.cells.create_range(
                src_start_row, src_start_col, src_row_count, src_col_count
            )

            # Parse destination range (using its sheet)
            dest_sheet_name = self.extract_sheet_name_from_range(destination_range)
            dest_worksheet = self._get_worksheet(dest_sheet_name)
            _, dest_start_row, dest_start_col, dest_end_row, dest_end_col = (
                self._parse_range_spec(destination_range)
            )
            dest_row_count = dest_end_row - dest_start_row + 1
            dest_col_count = dest_end_col - dest_start_col + 1
            destination_range_obj = dest_worksheet.cells.create_range(
                dest_start_row, dest_start_col, dest_row_count, dest_col_count
            )

            # Map fill_type string to Aspose.Cells.AutoFillType enum
            fill_type_map = {
                "COPY": AutoFillType.COPY,
                "SERIES": AutoFillType.SERIES,
                "FORMATS": AutoFillType.FORMATS,
                "VALUES": AutoFillType.VALUES,
                "DEFAULT": AutoFillType.DEFAULT,
            }
            aspose_fill_type = fill_type_map.get(fill_type.upper())
            if aspose_fill_type is None:
                raise ValueError(f"Unsupported fill_type: {fill_type}")

            source_range_obj.auto_fill(destination_range_obj, aspose_fill_type)

        except Exception as e:
            raise RuntimeError(
                f"Error performing auto_fill from '{source_range}' to '{destination_range}': {e}"
            ) from e

    def save(self) -> None:
        """Save the workbook to the file."""
        if not self.workbook.file_name:
            raise ValueError(
                "Workbook file_name is not set. Cannot determine where to save."
            )
        try:
            self.workbook.save(self.workbook.file_name)
            logger.info(f"Workbook saved to '{self.workbook.file_name}'.")
        except Exception as e:
            raise RuntimeError(
                f"Error saving workbook to '{self.workbook.file_name}': {e}"
            ) from e
