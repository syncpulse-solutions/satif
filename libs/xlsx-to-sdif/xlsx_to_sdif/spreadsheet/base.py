from abc import ABC, abstractmethod
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

from pydantic import BaseModel, Field

SpreadsheetNavigationAction = Literal["display_range"]
SpreadsheetReadAction = Literal["read_cells"]


Values = List[List[Any]]


class AddValues(BaseModel):
    range: str = Field(
        ...,
        description="The A1 notation of the range to add values to (e.g., 'Sheet1!A1:B5').",
    )
    values: Values = Field(..., description="The 2D array of values to add.")


class UpdateValues(BaseModel):
    action: Literal["update_values"] = "update_values"
    range: str = Field(
        ...,
        description="The A1 notation of the range to update (e.g., 'Sheet1!C1:D5').",
    )
    values: Values = Field(..., description="The 2D array of values to update.")


class DeleteValues(BaseModel):
    action: Literal["delete_values"] = "delete_values"
    range: str = Field(
        ...,
        description="The A1 notation of the range to clear values from (e.g., 'Sheet1!E1:E5').",
    )


class InsertRows(BaseModel):
    action: Literal["insert_rows"] = "insert_rows"
    sheet_name: str = Field(
        ..., description="The name of the sheet where rows will be inserted."
    )
    start_row: int = Field(
        ..., description="The 0-based index of the row where insertion begins."
    )
    count: int = Field(..., description="The number of rows to insert.")
    values: Optional[Values] = Field(
        None, description="Optional values to populate the new rows."
    )


class DeleteRows(BaseModel):
    action: Literal["delete_rows"] = "delete_rows"
    sheet_name: str = Field(
        ..., description="The name of the sheet from which rows will be deleted."
    )
    start_row: int = Field(
        ..., description="The 0-based index of the first row to delete."
    )
    count: int = Field(..., description="The number of rows to delete.")


class InsertColumns(BaseModel):
    action: Literal["insert_columns"] = "insert_columns"
    sheet_name: str = Field(
        ..., description="The name of the sheet where columns will be inserted."
    )
    start_column: int = Field(
        ..., description="The 0-based index of the column where insertion begins."
    )
    count: int = Field(..., description="The number of columns to insert.")
    values: Optional[Values] = Field(
        None, description="Optional values to populate the new columns."
    )


class DeleteColumns(BaseModel):
    action: Literal["delete_columns"] = "delete_columns"
    sheet_name: str = Field(
        ..., description="The name of the sheet from which columns will be deleted."
    )
    start_column: int = Field(
        ..., description="The 0-based index of the first column to delete."
    )
    count: int = Field(..., description="The number of columns to delete.")


class MergeCells(BaseModel):
    action: Literal["merge_cells"] = "merge_cells"
    range: str = Field(
        ..., description="The A1 notation of the range to merge (e.g., 'Sheet1!A1:B2')."
    )


class UnmergeCells(BaseModel):
    action: Literal["unmerge_cells"] = "unmerge_cells"
    range: str = Field(
        ...,
        description="The A1 notation of the range to unmerge (e.g., 'Sheet1!C1:D1').",
    )


class CopyPaste(BaseModel):
    action: Literal["copy_paste"] = "copy_paste"
    source_range: str = Field(
        ..., description="The source range to copy from (e.g., 'Sheet1!A1:A5')."
    )
    destination_range: str = Field(
        ..., description="The destination range to paste to (e.g., 'Sheet1!B1')."
    )
    paste_type: Literal["PASTE_NORMAL", "PASTE_VALUES", "PASTE_FORMAT"] = Field(
        "PASTE_NORMAL", description="Type of paste operation."
    )


class FindReplace(BaseModel):
    action: Literal["find_replace"] = "find_replace"
    find: str = Field(..., description="The text to find.")
    replace: str = Field(..., description="The text to replace matches with.")
    range: Optional[str] = Field(
        None,
        description="Optional A1 notation range to limit the search (e.g., 'Sheet1!A1:Z100').",
    )
    match_case: bool = Field(False, description="Whether the search is case-sensitive.")
    match_entire_cell: bool = Field(
        False, description="Whether to match the entire cell content."
    )


class MoveColumns(BaseModel):
    action: Literal["move_columns"] = "move_columns"
    sheet_name: str = Field(
        ..., description="The name of the sheet where columns will be moved."
    )
    source_start_column: int = Field(
        ..., description="The 0-based starting index of the column(s) to move."
    )
    source_end_column: int = Field(
        ...,
        description="The 0-based ending index (exclusive) of the column(s) to move.",
    )
    destination_index: int = Field(
        ..., description="The 0-based index where the columns should be moved to."
    )


class MoveRows(BaseModel):
    action: Literal["move_rows"] = "move_rows"
    sheet_name: str = Field(
        ..., description="The name of the sheet where rows will be moved."
    )
    source_start_row: int = Field(
        ..., description="The 0-based starting index of the row(s) to move."
    )
    source_end_row: int = Field(
        ..., description="The 0-based ending index (exclusive) of the row(s) to move."
    )
    destination_index: int = Field(
        ..., description="The 0-based index where the rows should be moved to."
    )


class AutoResizeColumns(BaseModel):
    action: Literal["auto_resize_columns"] = "auto_resize_columns"
    sheet_name: str = Field(
        ..., description="The name of the sheet containing the columns to resize."
    )
    start_column: int = Field(
        ..., description="The 0-based starting index of the column range to resize."
    )
    end_column: int = Field(
        ...,
        description="The 0-based ending index (exclusive) of the column range to resize.",
    )


class AutoResizeRows(BaseModel):
    action: Literal["auto_resize_rows"] = "auto_resize_rows"
    sheet_name: str = Field(
        ..., description="The name of the sheet containing the rows to resize."
    )
    start_row: int = Field(
        ..., description="The 0-based starting index of the row range to resize."
    )
    end_row: int = Field(
        ...,
        description="The 0-based ending index (exclusive) of the row range to resize.",
    )


class AutoFill(BaseModel):
    action: Literal["auto_fill"] = "auto_fill"
    source_range: str = Field(
        ...,
        description="The source range with the data/pattern (e.g., 'Sheet1!A1:A2').",
    )
    destination_range: str = Field(
        ..., description="The target range to fill (e.g., 'Sheet1!A1:A10')."
    )
    fill_type: Literal["COPY", "SERIES", "FORMATS", "VALUES", "DEFAULT"] = Field(
        "DEFAULT", description="The type of auto-fill operation."
    )


SpreadsheetOperation = Annotated[
    Union[
        AddValues,
        UpdateValues,
        DeleteValues,
        InsertRows,
        DeleteRows,
        InsertColumns,
        DeleteColumns,
        MergeCells,
        UnmergeCells,
        CopyPaste,
        FindReplace,
        MoveColumns,
        MoveRows,
        AutoResizeColumns,
        AutoResizeRows,
        AutoFill,
    ],
    Field(discriminator="action"),
]


class SpreadsheetNavigation(TypedDict, total=False):
    action: SpreadsheetNavigationAction
    range: str


class SpreadsheetRead(TypedDict, total=False):
    action: SpreadsheetReadAction
    range: str


class SpreadsheetManager(ABC):
    """Abstract base class for spreadsheet manipulation agents.
    Concrete implementations will be created for specific platforms like Google Sheets, Excel, etc.
    """

    @abstractmethod
    def add_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Add values to a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
            values: 2D array of values to add
        """
        pass

    @abstractmethod
    def update_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Update values in a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
            values: 2D array of values to update
        """
        pass

    @abstractmethod
    def delete_values(self, range_spec: str) -> None:
        """Delete values in a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        pass

    @abstractmethod
    def insert_rows(
        self,
        sheet_name: str,
        start_row: int,
        count: int,
        values: Optional[List[List[Any]]] = None,
    ) -> None:
        """Insert rows starting at the specified row index.

        Args:
            sheet_name: Name of the sheet to modify
            start_row: Index of the first row to insert (0-based)
            count: Number of rows to insert
            values: Optional 2D array of values to populate the new rows
        """
        pass

    @abstractmethod
    def delete_rows(self, sheet_name: str, start_row: int, count: int) -> None:
        """Delete rows starting at the specified row index.

        Args:
            sheet_name: Name of the sheet to modify
            start_row: Index of the first row to delete (0-based)
            count: Number of rows to delete
        """
        pass

    @abstractmethod
    def insert_columns(
        self,
        sheet_name: str,
        start_column: int,
        count: int,
        values: Optional[List[List[Any]]] = None,
    ) -> None:
        """Insert columns starting at the specified column index.

        Args:
            sheet_name: Name of the sheet to modify
            start_column: Index of the first column to insert (0-based)
            count: Number of columns to insert
            values: Optional 2D array of values to populate the new columns
        """
        pass

    @abstractmethod
    def delete_columns(self, sheet_name: str, start_column: int, count: int) -> None:
        """Delete columns starting at the specified column index.

        Args:
            sheet_name: Name of the sheet to modify
            start_column: Index of the first column to delete (0-based)
            count: Number of columns to delete
        """
        pass

    @abstractmethod
    def merge_cells(self, range_spec: str, sheet_name: str | None = None) -> None:
        """Merge cells in the specified range.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        pass

    @abstractmethod
    def unmerge_cells(self, range_spec: str, sheet_name: str | None = None) -> None:
        """Unmerge cells in the specified range.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        pass

    @abstractmethod
    def copy_paste(
        self,
        source_range: str,
        destination_range: str,
        paste_type: Literal[
            "PASTE_NORMAL", "PASTE_VALUES", "PASTE_FORMAT"
        ] = "PASTE_NORMAL",
    ) -> None:
        """Copy and paste content from source range to destination range.

        Args:
            source_range: Source cell range in A1 notation (e.g., 'Sheet1!A1:B5')
            destination_range: Destination cell range in A1 notation
            paste_type: Type of paste operation (normal, values only, or format only)
        """
        pass

    @abstractmethod
    def find_replace(
        self,
        find: str,
        replace: str,
        range_spec: Optional[str] = None,
        match_case: bool = False,
        match_entire_cell: bool = False,
    ) -> int:
        """Find and replace content in the spreadsheet.

        Args:
            find: Text to find
            replace: Text to replace with
            range_spec: Optional cell range to limit the search (e.g., 'Sheet1!A1:B5')
            match_case: Whether to perform case-sensitive matching
            match_entire_cell: Whether to match only when the entire cell content matches

        Returns:
            Number of replacements made
        """
        pass

    @abstractmethod
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
        """Read values from one or multiple cells/ranges in the spreadsheet.

        Args:
            ranges: Either a single range in A1 notation (e.g., 'Sheet1!A1:B5') or a list of ranges
            major_dimension: How values should be represented in the output
                            Possible values: "ROWS", "COLUMNS"
            value_render_option: How values should be rendered in the output
                                Possible values: "FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"
            date_time_render_option: How dates, times, and durations should be represented
                                    Possible values: "SERIAL_NUMBER", "FORMATTED_STRING"

        Returns:
            If a single range is specified: A 2D array of values
            If multiple ranges are specified: A dictionary mapping range specs to 2D arrays of values
        """
        pass

    @abstractmethod
    def move_columns(
        self,
        sheet_name: str,
        source_start_column: int,
        source_end_column: int,
        destination_index: int,
    ) -> None:
        """Move columns from one position to another.

        Args:
            sheet_name: Name of the sheet
            source_start_column: Starting column index to move (0-based)
            source_end_column: Ending column index to move (exclusive)
            destination_index: Target position where columns will be moved to (0-based)
        """
        pass

    @abstractmethod
    def move_rows(
        self,
        sheet_name: str,
        source_start_row: int,
        source_end_row: int,
        destination_index: int,
    ) -> None:
        """Move rows from one position to another.

        Args:
            sheet_name: Name of the sheet
            source_start_row: Starting row index to move (0-based)
            source_end_row: Ending row index to move (exclusive)
            destination_index: Target position where rows will be moved to (0-based)
        """
        pass

    @abstractmethod
    def auto_resize_columns(
        self, sheet_name: str, start_column: int, end_column: int
    ) -> None:
        """Auto-resize columns based on content.

        Args:
            sheet_name: Name of the sheet
            start_column: Starting column index (0-based)
            end_column: Ending column index (exclusive)
        """
        pass

    @abstractmethod
    def auto_resize_rows(self, sheet_name: str, start_row: int, end_row: int) -> None:
        """Auto-resize rows based on content.

        Args:
            sheet_name: Name of the sheet
            start_row: Starting row index (0-based)
            end_row: Ending row index (exclusive)
        """
        pass

    @abstractmethod
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
        """Auto-fill a destination range based on the data in a source range.

        Args:
            source_range: The source range containing the data/pattern (e.g., 'Sheet1!A1:A2')
            destination_range: The target range to fill (e.g., 'Sheet1!A1:A10')
            fill_type: The type of auto-fill operation. Defaults to "DEFAULT".
                       "COPY": Copies the source range values.
                       "SERIES": Extrapolates based on patterns (numbers, dates).
                       "FORMATS": Copies only formatting.
                       "VALUES": Copies only values.
                       "DEFAULT": Lets the library decide the best fill type.
        """
        pass

    @abstractmethod
    def change_sheet(self, sheet_name: str) -> None:
        """Change the active sheet in the spreadsheet.

        Args:
            sheet_name: Name of the sheet to change to
        """
        pass

    @abstractmethod
    def display_only_range(self, range_spec: str, max_row: int, max_col: int) -> None:
        """Display a range in the spreadsheet.

        Args:
            range_spec: Range in A1 notation (e.g., 'Sheet1!A1:B5')
            max_row: Maximum number of rows to display
            max_col: Maximum number of columns to display
        """
        pass

    def save() -> None:
        pass

    # Helper methods that could be useful across different implementations
    def extract_sheet_name_from_range(self, range_spec: str) -> str:
        """Extract sheet name from a range specification.

        Args:
            range_spec: Range in A1 notation (e.g., 'Sheet1!A1:B5')

        Returns:
            Sheet name
        """
        if "!" in range_spec:
            return range_spec.split("!")[0]
        return "Sheet1"  # Default sheet

    def parse_range(self, range_spec: str) -> Tuple[str, int, str, int]:
        """Parse a range specification in A1 notation into start and end coordinates.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'A1:B5')

        Returns:
            Tuple of (start_col, start_row, end_col, end_row)
        """
        if "!" in range_spec:
            range_spec = range_spec.split("!")[1]

        if ":" not in range_spec:
            # Single cell
            start_cell = end_cell = range_spec
        else:
            start_cell, end_cell = range_spec.split(":")

        start_col, start_row = self._parse_cell(start_cell)
        end_col, end_row = self._parse_cell(end_cell)

        return start_col, start_row, end_col, end_row

    def _parse_cell(self, cell_ref: str) -> Tuple[str, int]:
        """Parse a cell reference (e.g., 'A1') into column and row components.

        Args:
            cell_ref: Cell reference in A1 notation

        Returns:
            Tuple of (column_letter, row_number)
        """
        # Find the boundary between letters and numbers
        for i, char in enumerate(cell_ref):
            if char.isdigit():
                break
        else:
            raise ValueError(f"Invalid cell reference: {cell_ref}")

        col_letters = cell_ref[:i]
        row_num = int(cell_ref[i:])

        return col_letters, row_num

    def col_letter_to_index(self, col_letters: str) -> int:
        """Convert column letters (e.g., 'A', 'BC') to a 0-based column index.

        Args:
            col_letters: Column letters in A1 notation

        Returns:
            0-based column index
        """
        index = 0
        for letter in col_letters:
            index = index * 26 + (ord(letter.upper()) - ord("A") + 1)
        return index - 1  # Convert to 0-based

    def col_index_to_letter(self, col_index: int) -> str:
        """Convert a 0-based column index to column letters (e.g., 0 -> 'A', 27 -> 'AB').

        Args:
            col_index: 0-based column index

        Returns:
            Column letters in A1 notation
        """
        if col_index < 0:
            raise ValueError("Column index must be non-negative")

        col_index += 1  # Convert to 1-based for the calculation
        letters = ""

        while col_index > 0:
            col_index, remainder = divmod(col_index - 1, 26)
            letters = chr(ord("A") + remainder) + letters

        return letters

    def execute_batch_operations(self, operations: List[SpreadsheetOperation]) -> None:
        """Execute a batch of Pydantic operation models in sequence.

        Args:
            operations: List of Pydantic models, each being one of the types in AnySpreadsheetOperation.
        """
        for operation in operations:
            # Use isinstance checks based on the Pydantic models
            if isinstance(operation, AddValues):
                self.add_values(operation.range, operation.values)
            elif isinstance(operation, UpdateValues):
                self.update_values(operation.range, operation.values)
            elif isinstance(operation, DeleteValues):
                self.delete_values(operation.range)
            elif isinstance(operation, InsertRows):
                self.insert_rows(
                    operation.sheet_name,
                    operation.start_row,
                    operation.count,
                    operation.values,
                )
            elif isinstance(operation, DeleteRows):
                self.delete_rows(
                    operation.sheet_name, operation.start_row, operation.count
                )
            elif isinstance(operation, InsertColumns):
                self.insert_columns(
                    operation.sheet_name,
                    operation.start_column,
                    operation.count,
                    operation.values,
                )
            elif isinstance(operation, DeleteColumns):
                self.delete_columns(
                    operation.sheet_name,
                    operation.start_column,
                    operation.count,
                )
            elif isinstance(operation, MergeCells):
                # Extract sheet name implicitly if needed by implementation, or pass None
                sheet_name = (
                    self.extract_sheet_name_from_range(operation.range)
                    if "!" in operation.range
                    else None
                )
                self.merge_cells(operation.range, sheet_name=sheet_name)
            elif isinstance(operation, UnmergeCells):
                # Extract sheet name implicitly if needed by implementation, or pass None
                sheet_name = (
                    self.extract_sheet_name_from_range(operation.range)
                    if "!" in operation.range
                    else None
                )
                self.unmerge_cells(operation.range, sheet_name=sheet_name)
            elif isinstance(operation, CopyPaste):
                self.copy_paste(
                    operation.source_range,
                    operation.destination_range,
                    operation.paste_type,
                )
            elif isinstance(operation, FindReplace):
                self.find_replace(
                    operation.find,
                    operation.replace,
                    operation.range,  # Pass the optional range
                    operation.match_case,
                    operation.match_entire_cell,
                )
            elif isinstance(operation, MoveColumns):
                self.move_columns(
                    operation.sheet_name,
                    operation.source_start_column,
                    operation.source_end_column,
                    operation.destination_index,
                )
            elif isinstance(operation, MoveRows):
                self.move_rows(
                    operation.sheet_name,
                    operation.source_start_row,
                    operation.source_end_row,
                    operation.destination_index,
                )
            elif isinstance(operation, AutoResizeColumns):
                self.auto_resize_columns(
                    operation.sheet_name,
                    operation.start_column,
                    operation.end_column,
                )
            elif isinstance(operation, AutoResizeRows):
                self.auto_resize_rows(
                    operation.sheet_name,
                    operation.start_row,
                    operation.end_row,
                )
            elif isinstance(operation, AutoFill):
                self.auto_fill(
                    operation.source_range,
                    operation.destination_range,
                    operation.fill_type,
                )
            else:
                # Should not happen with discriminated union, but included as a safeguard
                raise ValueError(f"Unsupported operation type: {type(operation)}")

    def range_dimensions(self, range_spec: str) -> Tuple[int, int]:
        """Calculate the dimensions (rows, columns) of a specified range.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'A1:B5')

        Returns:
            Tuple of (row_count, column_count)
        """
        start_col, start_row, end_col, end_row = self.parse_range(range_spec)

        start_col_idx = self.col_letter_to_index(start_col)
        end_col_idx = self.col_letter_to_index(end_col)

        row_count = end_row - start_row + 1
        col_count = end_col_idx - start_col_idx + 1

        return row_count, col_count
