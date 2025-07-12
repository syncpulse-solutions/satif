from typing import Any, Dict, List, Literal, Optional

from googleapiclient.discovery import Resource

from .base import SpreadsheetManager


class GoogleSheetsManager(SpreadsheetManager):
    """Concrete implementation of SpreadsheetManager for Google Sheets.
    Uses the Google Sheets API to perform spreadsheet operations.
    """

    def __init__(self, spreadsheet_id: str, service: Resource):
        """Initialize the Google Sheets manager.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet
            service: The Google Sheets API service object
        """
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.sheet_id_cache = {}  # Cache for sheet IDs

    def get_sheet_id(self, sheet_name: str) -> int:
        """Get the sheet ID for a given sheet name.

        Args:
            sheet_name: Name of the sheet

        Returns:
            Sheet ID as an integer

        Raises:
            ValueError: If sheet with given name doesn't exist
        """
        if sheet_name in self.sheet_id_cache:
            return self.sheet_id_cache[sheet_name]

        # Fetch spreadsheet metadata
        spreadsheet = (
            self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        )

        # Find the sheet ID
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                self.sheet_id_cache[sheet_name] = sheet_id
                return sheet_id

        raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet")

    def extract_sheet_name_from_range(self, range_spec: str) -> str:
        """Extract sheet name from a range specification.

        Args:
            range_spec: Range in A1 notation (e.g., 'Sheet1!A1:B5')

        Returns:
            Sheet name
        """
        if "!" in range_spec:
            return range_spec.split("!")[0]
        raise ValueError(
            f"Could not extract sheet name from {range_spec}. Verify range spec follows 'Sheet1!A1:B5' format "
        )

    def add_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Add values to a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
            values: 2D array of values to add
        """
        body = {"values": values}

        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_spec,
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

    def update_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Update values in a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
            values: 2D array of values to update
        """
        body = {"values": values}

        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_spec,
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

    def delete_values(self, range_spec: str) -> None:
        """Delete values in a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        self.service.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id, range=range_spec, body={}
        ).execute()

    def read_cells(
        self,
        ranges: str | List[str],
        major_dimension: str = "ROWS",
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> Dict[str, List[List[Any]]] | List[List[Any]]:
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
        params = {
            "majorDimension": major_dimension,
            "valueRenderOption": value_render_option,
        }

        # Only add dateTimeRenderOption if valueRenderOption is not FORMATTED_VALUE
        if value_render_option != "FORMATTED_VALUE":
            params["dateTimeRenderOption"] = date_time_render_option

        if isinstance(ranges, str):
            # Single range
            response = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=ranges, **params)
                .execute()
            )

            return response.get("values", [])
        else:
            # Multiple ranges
            response = (
                self.service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=self.spreadsheet_id, ranges=ranges, **params)
                .execute()
            )

            result = {}
            for value_range in response.get("valueRanges", []):
                range_name = value_range.get("range")
                values = value_range.get("values", [])
                result[range_name] = values

            return result

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
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": start_row + count,
                    },
                    "inheritFromBefore": start_row > 0,
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

        # If values are provided, add them to the newly inserted rows
        if values:
            # Convert 0-based row index to 1-based for A1 notation
            a1_start_row = start_row + 1
            range_spec = f"{sheet_name}!A{a1_start_row}"
            self.add_values(range_spec, values)

    def delete_rows(self, sheet_name: str, start_row: int, count: int) -> None:
        """Delete rows starting at the specified row index.

        Args:
            sheet_name: Name of the sheet to modify
            start_row: Index of the first row to delete (0-based)
            count: Number of rows to delete
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": start_row + count,
                    }
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

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
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": start_column + count,
                    },
                    "inheritFromBefore": start_column > 0,
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

        # If values are provided, add them to the newly inserted columns
        if values:
            # Convert 0-based column index to A1 notation
            start_col_letter = self.col_index_to_letter(start_column)
            end_col_letter = self.col_index_to_letter(start_column + count - 1)
            range_spec = f"{sheet_name}!{start_col_letter}1:{end_col_letter}"

            # Transpose values for column-wise insertion
            transposed_values = list(map(list, zip(*values)))
            self.add_values(range_spec, transposed_values)

    def delete_columns(self, sheet_name: str, start_column: int, count: int) -> None:
        """Delete columns starting at the specified column index.

        Args:
            sheet_name: Name of the sheet to modify
            start_column: Index of the first column to delete (0-based)
            count: Number of columns to delete
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": start_column + count,
                    }
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def merge_cells(self, range_spec: str) -> None:
        """Merge cells in the specified range.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        sheet_name = self.extract_sheet_name_from_range(range_spec)
        sheet_id = self.get_sheet_id(sheet_name)

        # Extract the range coordinates
        start_col, start_row, end_col, end_row = self.parse_range(range_spec)
        start_col_idx = self.col_letter_to_index(start_col)
        end_col_idx = self.col_letter_to_index(end_col)

        # Adjust for 0-based indexing
        start_row_idx = start_row - 1
        end_row_idx = end_row

        requests = [
            {
                "mergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_idx,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": end_col_idx + 1,
                    },
                    "mergeType": "MERGE_ALL",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def unmerge_cells(self, range_spec: str) -> None:
        """Unmerge cells in the specified range.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')
        """
        sheet_name = self.extract_sheet_name_from_range(range_spec)
        sheet_id = self.get_sheet_id(sheet_name)

        # Extract the range coordinates
        start_col, start_row, end_col, end_row = self.parse_range(range_spec)
        start_col_idx = self.col_letter_to_index(start_col)
        end_col_idx = self.col_letter_to_index(end_col)

        # Adjust for 0-based indexing
        start_row_idx = start_row - 1
        end_row_idx = end_row

        requests = [
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_idx,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": end_col_idx + 1,
                    }
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

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
        source_sheet_name = self.extract_sheet_name_from_range(source_range)
        dest_sheet_name = self.extract_sheet_name_from_range(destination_range)

        source_sheet_id = self.get_sheet_id(source_sheet_name)
        dest_sheet_id = self.get_sheet_id(dest_sheet_name)

        # Extract the range coordinates
        source_start_col, source_start_row, source_end_col, source_end_row = (
            self.parse_range(source_range)
        )
        dest_start_col, dest_start_row, _, _ = self.parse_range(destination_range)

        # Convert to 0-based indices
        source_start_col_idx = self.col_letter_to_index(source_start_col)
        source_end_col_idx = self.col_letter_to_index(source_end_col)
        dest_start_col_idx = self.col_letter_to_index(dest_start_col)

        source_start_row_idx = source_start_row - 1
        source_end_row_idx = source_end_row
        dest_start_row_idx = dest_start_row - 1

        requests = [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": source_sheet_id,
                        "startRowIndex": source_start_row_idx,
                        "endRowIndex": source_end_row_idx,
                        "startColumnIndex": source_start_col_idx,
                        "endColumnIndex": source_end_col_idx + 1,
                    },
                    "destination": {
                        "sheetId": dest_sheet_id,
                        "startRowIndex": dest_start_row_idx,
                        "startColumnIndex": dest_start_col_idx,
                    },
                    "pasteType": paste_type,
                    "pasteOrientation": "NORMAL",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

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
        sheet_id = None
        start_row_idx = None
        end_row_idx = None
        start_col_idx = None
        end_col_idx = None

        if range_spec:
            sheet_name = self.extract_sheet_name_from_range(range_spec)
            sheet_id = self.get_sheet_id(sheet_name)

            # Extract the range coordinates
            start_col, start_row, end_col, end_row = self.parse_range(range_spec)
            start_col_idx = self.col_letter_to_index(start_col)
            end_col_idx = self.col_letter_to_index(end_col)

            # Adjust for 0-based indexing
            start_row_idx = start_row - 1
            end_row_idx = end_row

        find_replace_request = {
            "find": find,
            "replacement": replace,
            "matchCase": match_case,
            "matchEntireCell": match_entire_cell,
        }

        # If a range is specified, add it to the request
        if range_spec:
            find_replace_request["range"] = {
                "sheetId": sheet_id,
                "startRowIndex": start_row_idx,
                "endRowIndex": end_row_idx,
                "startColumnIndex": start_col_idx,
                "endColumnIndex": end_col_idx + 1,
            }

        requests = [{"findReplace": find_replace_request}]

        body = {"requests": requests}

        response = (
            self.service.spreadsheets()
            .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
            .execute()
        )

        # Extract the number of replacements from the response
        if "replies" in response and len(response["replies"]) > 0:
            find_replace_response = response["replies"][0].get("findReplace", {})
            return find_replace_response.get("occurrencesChanged", 0)

        return 0

    def get_values(self, range_spec: str) -> List[List[Any]]:
        """Read values from a specified range in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation (e.g., 'Sheet1!A1:B5')

        Returns:
            2D array of values
        """
        response = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=range_spec)
            .execute()
        )

        return response.get("values", [])

    def get_values_batch(self, ranges: List[str]) -> Dict[str, List[List[Any]]]:
        """Read values from multiple ranges in the spreadsheet.

        Args:
            ranges: List of cell ranges in A1 notation

        Returns:
            Dictionary mapping range specs to their values
        """
        response = (
            self.service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=self.spreadsheet_id, ranges=ranges)
            .execute()
        )

        result = {}
        for value_range in response.get("valueRanges", []):
            range_name = value_range.get("range")
            values = value_range.get("values", [])
            result[range_name] = values

        return result

    def append_values(self, range_spec: str, values: List[List[Any]]) -> None:
        """Append values to a table in the spreadsheet.

        Args:
            range_spec: Cell range in A1 notation that represents the table
            values: 2D array of values to append
        """
        body = {"values": values}

        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=range_spec,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

    def create_sheet(self, title: str) -> int:
        """Create a new sheet in the spreadsheet.

        Args:
            title: Name of the new sheet

        Returns:
            ID of the newly created sheet
        """
        requests = [{"addSheet": {"properties": {"title": title}}}]

        body = {"requests": requests}

        response = (
            self.service.spreadsheets()
            .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
            .execute()
        )

        # Extract the new sheet ID from the response
        sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

        # Update the cache
        self.sheet_id_cache[title] = sheet_id

        return sheet_id

    def delete_sheet(self, sheet_name: str) -> None:
        """Delete a sheet from the spreadsheet.

        Args:
            sheet_name: Name of the sheet to delete
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [{"deleteSheet": {"sheetId": sheet_id}}]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

        # Remove from cache
        if sheet_name in self.sheet_id_cache:
            del self.sheet_id_cache[sheet_name]

    def auto_resize_columns(
        self, sheet_name: str, start_column: int, end_column: int
    ) -> None:
        """Auto-resize columns based on content.

        Args:
            sheet_name: Name of the sheet
            start_column: Starting column index (0-based)
            end_column: Ending column index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": end_column,
                    }
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def auto_resize_rows(self, sheet_name: str, start_row: int, end_row: int) -> None:
        """Auto-resize rows based on content.

        Args:
            sheet_name: Name of the sheet
            start_row: Starting row index (0-based)
            end_row: Ending row index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": end_row,
                    }
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def set_column_width(
        self, sheet_name: str, start_column: int, end_column: int, width_px: int
    ) -> None:
        """Set the width of columns in pixels.

        Args:
            sheet_name: Name of the sheet
            start_column: Starting column index (0-based)
            end_column: Ending column index (exclusive)
            width_px: Width in pixels
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": end_column,
                    },
                    "properties": {"pixelSize": width_px},
                    "fields": "pixelSize",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def set_row_height(
        self, sheet_name: str, start_row: int, end_row: int, height_px: int
    ) -> None:
        """Set the height of rows in pixels.

        Args:
            sheet_name: Name of the sheet
            start_row: Starting row index (0-based)
            end_row: Ending row index (exclusive)
            height_px: Height in pixels
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": end_row,
                    },
                    "properties": {"pixelSize": height_px},
                    "fields": "pixelSize",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def hide_columns(self, sheet_name: str, start_column: int, end_column: int) -> None:
        """Hide columns in the specified range.

        Args:
            sheet_name: Name of the sheet
            start_column: Starting column index (0-based)
            end_column: Ending column index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": end_column,
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def show_columns(self, sheet_name: str, start_column: int, end_column: int) -> None:
        """Show (unhide) columns in the specified range.

        Args:
            sheet_name: Name of the sheet
            start_column: Starting column index (0-based)
            end_column: Ending column index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column,
                        "endIndex": end_column,
                    },
                    "properties": {"hiddenByUser": False},
                    "fields": "hiddenByUser",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def hide_rows(self, sheet_name: str, start_row: int, end_row: int) -> None:
        """Hide rows in the specified range.

        Args:
            sheet_name: Name of the sheet
            start_row: Starting row index (0-based)
            end_row: Ending row index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": end_row,
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def show_rows(self, sheet_name: str, start_row: int, end_row: int) -> None:
        """Show (unhide) rows in the specified range.

        Args:
            sheet_name: Name of the sheet
            start_row: Starting row index (0-based)
            end_row: Ending row index (exclusive)
        """
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row,
                        "endIndex": end_row,
                    },
                    "properties": {"hiddenByUser": False},
                    "fields": "hiddenByUser",
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

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
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "moveDimension": {
                    "source": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": source_start_column,
                        "endIndex": source_end_column,
                    },
                    "destinationIndex": destination_index,
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

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
        sheet_id = self.get_sheet_id(sheet_name)

        requests = [
            {
                "moveDimension": {
                    "source": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": source_start_row,
                        "endIndex": source_end_row,
                    },
                    "destinationIndex": destination_index,
                }
            }
        ]

        body = {"requests": requests}

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def get_sheet_max_view_dimensions(self, sheet_name: str = None) -> tuple[int, int]:
        # Get the entire spreadsheet
        sheet_metadata = (
            self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        )

        if sheet_name:
            # Find the specific sheet ID if name provided
            sheet_id = None
            for sheet in sheet_metadata.get("sheets", []):
                if sheet["properties"]["title"] == sheet_name:
                    sheet_id = sheet["properties"]["sheetId"]
                    break

            if not sheet_id:
                return None, "Sheet not found"
        else:
            # Use first sheet by default
            sheet_id = sheet_metadata.get("sheets", [])[0]["properties"]["sheetId"]

        # Get the data ranges for the sheet
        result = (
            self.service.spreadsheets()
            .get(spreadsheetId=self.spreadsheet_id, ranges=[], includeGridData=False)
            .execute()
        )

        for sheet in result.get("sheets", []):
            if sheet["properties"]["sheetId"] == sheet_id:
                grid_properties = sheet["properties"]["gridProperties"]

                # These represent the actual data range (excluding empty cells)
                row_count = grid_properties.get("rowCount", 0)
                column_count = grid_properties.get("columnCount", 0)

                return row_count, column_count

        return None, "Failed to get dimensions"

    def display_only_range(
        self, range_spec: str, max_row: int, max_col: int, sheet_name: str | None = None
    ) -> None:
        """Hide all rows and columns outside of the specified A1 range, leaving only
        the cells in 'range_spec' visible. Takes total rows (max_row) and columns (max_col)
        that you'd like to consider, so it can hide them accordingly.

        Args:
            range_spec (str): A1 notation range (e.g. 'Sheet1!A1:C5').
            max_row (int): Number of total rows in the sheet (or the maximum you want to show).
            max_col (int): Number of total columns in the sheet (or the maximum you want to show).
        """
        if not sheet_name:
            # Extract the sheet name
            sheet_name = self.extract_sheet_name_from_range(range_spec)

        # Parse the range into start/end positions
        start_col, start_row, end_col, end_row = self.parse_range(range_spec)

        # Convert letters/numbers to 0-based indices
        start_col_idx = self.col_letter_to_index(start_col)  # e.g. 'A' -> 0
        end_col_idx = self.col_letter_to_index(end_col)  # inclusive
        # The parse_range function returns row numbers as 1-based,
        # so we convert to 0-based by subtracting 1
        start_row_idx = start_row - 1
        end_row_idx = end_row - 1  # inclusive

        # 1) Hide columns *before* the range
        #    This means columns [0 .. start_col_idx-1]
        if start_col_idx > 0:
            self.hide_columns(sheet_name, 0, start_col_idx)

        # 2) Hide columns *after* the range
        #    This means columns [end_col_idx+1 .. max_col-1]
        #    because end_col_idx is inclusive, so hide starting from end_col_idx+1
        if (end_col_idx + 1) < max_col:
            self.hide_columns(sheet_name, end_col_idx + 1, max_col)

        # 3) Hide rows *before* the range
        #    This means rows [0 .. start_row_idx-1]
        if start_row_idx > 0:
            self.hide_rows(sheet_name, 0, start_row_idx)

        # 4) Hide rows *after* the range
        #    This means rows [end_row_idx+1 .. max_row-1]
        if (end_row_idx + 1) < max_row:
            self.hide_rows(sheet_name, end_row_idx + 1, max_row)

    def get_sheets_metadata(self, spreadsheet_id: str) -> Dict[str, Any]:
        """Get metadata about all sheets in a spreadsheet.

        Args:
            spreadsheet_id: ID of the spreadsheet
            google_sheets_service: The Google Sheets service object

        Returns:
            Dict: A dictionary containing basic spreadsheet information including all sheets
        """
        # Get the spreadsheet metadata
        spreadsheet_info = (
            self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        )

        # Extract sheet information
        sheets_data = []
        for sheet in spreadsheet_info.get("sheets", []):
            sheet_properties = sheet["properties"]
            sheets_data.append(
                {
                    "title": sheet_properties["title"],
                    "sheetId": sheet_properties["sheetId"],
                }
            )

        # Build the base spreadsheet state
        spreadsheet_state = {
            "spreadsheetId": spreadsheet_id,
            "title": spreadsheet_info.get("properties", {}).get("title", ""),
            "sheets": sheets_data,
        }

        return spreadsheet_state

    def change_sheet(self, sheet_name: str) -> None:
        return super().change_sheet(sheet_name)
