import json
from typing import Annotated, Any, List, Literal, Optional

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.prebuilt import InjectedState
from langgraph.types import Command

from xlsx_to_sdif.spreadsheet.aspose_cells import AsposeCellsManager, get_workbook
from xlsx_to_sdif.spreadsheet.base import SpreadsheetNavigation
from xlsx_to_sdif.state import State


@tool
def navigate(
    navigation: Annotated[
        SpreadsheetNavigation,
        "The navigation to execute on the spreadsheet. Full Range spec notation (e.g. 'Sheet1!A1:B5')",
    ],
    state: Annotated[State, InjectedState],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> str:
    """Navigate to a specific sheet in the spreadsheet."""
    if navigation["action"] == "display_range":
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        f"Successfully navigated to {navigation['range']}.",
                        tool_call_id=tool_call_id,
                    ),
                ],
                "active_sheet": {
                    "title": state["active_sheet"]["title"],
                    "range_to_display": navigation["range"],
                    "range_displayed": state["active_sheet"]["range_displayed"],
                },
            },
        )
    else:
        raise ValueError(f"Unknown action: {navigation['action']}")


@tool
def read_cells(
    ranges: Annotated[str | list[str], "The range(s) to read from the spreadsheet."],
    state: Annotated[State, InjectedState],
) -> str:
    """Read the cells in the specified range from the spreadsheet."""
    spreadsheet_manager = AsposeCellsManager(
        workbook=get_workbook(state["spreadsheet_path"]),
    )
    cells = spreadsheet_manager.read_cells(ranges)
    return cells


def _execute_spreadsheet_action(action_func, success_message, state):
    """Executes a spreadsheet action with error handling and saving."""
    try:
        spreadsheet_manager = AsposeCellsManager(
            workbook=get_workbook(state["spreadsheet_path"]),
        )
        # Action func should accept the manager instance
        action_func(spreadsheet_manager)
        spreadsheet_manager.save()
        return success_message
    except Exception as e:
        # Log the full traceback here if needed for debugging
        # import traceback
        # print(f"Error details: {traceback.format_exc()}")
        error_info = {"error": type(e).__name__, "message": str(e)}
        return f"Error: {json.dumps(error_info)}"


@tool
def add_values(
    range: Annotated[
        str, "The A1 notation of the range to add values to (e.g., 'Sheet1!A1:B5')."
    ],
    values: Annotated[List[List[str]], "The 2D array of values to add."],
    state: Annotated[State, InjectedState],
) -> str:
    """Add values to a specified range in the spreadsheet."""

    def action(manager: AsposeCellsManager):
        manager.add_values(range_spec=range, values=values)

    return _execute_spreadsheet_action(
        action, f"Successfully added values to {range}.", state
    )


@tool
def update_values(
    range: Annotated[
        str, "The A1 notation of the range to update (e.g., 'Sheet1!C1:D5')."
    ],
    values: Annotated[List[List[str]], "The 2D array of values to update."],
    state: Annotated[State, InjectedState],
) -> str:
    """Update values in a specified range in the spreadsheet."""

    def action(manager: AsposeCellsManager):
        manager.update_values(range_spec=range, values=values)

    return _execute_spreadsheet_action(
        action, f"Successfully updated values in {range}.", state
    )


@tool
def delete_values(
    range: Annotated[
        str, "The A1 notation of the range to clear values from (e.g., 'Sheet1!E1:E5')."
    ],
    state: Annotated[State, InjectedState],
) -> str:
    """Delete values (clear content) in a specified range in the spreadsheet."""

    def action(manager: AsposeCellsManager):
        manager.delete_values(range_spec=range)

    return _execute_spreadsheet_action(
        action, f"Successfully deleted values from {range}.", state
    )


@tool
def insert_rows(
    sheet_name: Annotated[str, "The name of the sheet where rows will be inserted."],
    start_row: Annotated[int, "The 0-based index of the row where insertion begins."],
    count: Annotated[int, "The number of rows to insert."],
    values: Annotated[
        Optional[List[List[Any]]], "Optional values to populate the new rows."
    ] = None,
    state: Annotated[State, InjectedState] = None,
) -> str:
    """Insert one or more rows into a sheet, optionally populating them with values."""

    def action(manager: AsposeCellsManager):
        manager.insert_rows(
            sheet_name=sheet_name, start_row=start_row, count=count, values=values
        )

    return _execute_spreadsheet_action(
        action,
        f"Successfully inserted {count} row(s) into '{sheet_name}' starting at index {start_row}.",
        state,
    )


@tool
def delete_rows(
    sheet_name: Annotated[
        str, "The name of the sheet from which rows will be deleted."
    ],
    start_row: Annotated[int, "The 0-based index of the first row to delete."],
    count: Annotated[int, "The number of rows to delete."],
    state: Annotated[State, InjectedState],
) -> str:
    """Delete one or more rows from a sheet."""

    def action(manager: AsposeCellsManager):
        manager.delete_rows(sheet_name=sheet_name, start_row=start_row, count=count)

    return _execute_spreadsheet_action(
        action,
        f"Successfully deleted {count} row(s) from '{sheet_name}' starting at index {start_row}.",
        state,
    )


@tool
def insert_columns(
    sheet_name: Annotated[str, "The name of the sheet where columns will be inserted."],
    start_column: Annotated[
        int, "The 0-based index of the column where insertion begins."
    ],
    count: Annotated[int, "The number of columns to insert."],
    values: Annotated[
        Optional[List[List[Any]]], "Optional values to populate the new columns."
    ] = None,
    state: Annotated[State, InjectedState] = None,
) -> str:
    """Insert one or more columns into a sheet, optionally populating them with values."""

    def action(manager: AsposeCellsManager):
        manager.insert_columns(
            sheet_name=sheet_name, start_column=start_column, count=count, values=values
        )

    return _execute_spreadsheet_action(
        action,
        f"Successfully inserted {count} column(s) into '{sheet_name}' starting at index {start_column}.",
        state,
    )


@tool
def delete_columns(
    sheet_name: Annotated[
        str, "The name of the sheet from which columns will be deleted."
    ],
    start_column: Annotated[int, "The 0-based index of the first column to delete."],
    count: Annotated[int, "The number of columns to delete."],
    state: Annotated[State, InjectedState],
) -> str:
    """Delete one or more columns from a sheet."""

    def action(manager: AsposeCellsManager):
        manager.delete_columns(
            sheet_name=sheet_name, start_column=start_column, count=count
        )

    return _execute_spreadsheet_action(
        action,
        f"Successfully deleted {count} column(s) from '{sheet_name}' starting at index {start_column}.",
        state,
    )


@tool
def merge_cells(
    range: Annotated[
        str, "The A1 notation of the range to merge (e.g., 'Sheet1!A1:B2')."
    ],
    state: Annotated[State, InjectedState],
) -> str:
    """Merge cells in the specified range."""

    def action(manager: AsposeCellsManager):
        # Extract sheet name implicitly if needed by implementation
        sheet_name = (
            manager.extract_sheet_name_from_range(range) if "!" in range else None
        )
        # Assuming base method needs range_spec and optionally sheet_name
        manager.merge_cells(range_spec=range, sheet_name=sheet_name)

    return _execute_spreadsheet_action(
        action, f"Successfully merged cells in range {range}.", state
    )


@tool
def unmerge_cells(
    range: Annotated[
        str, "The A1 notation of the range to unmerge (e.g., 'Sheet1!C1:D1')."
    ],
    state: Annotated[State, InjectedState],
) -> str:
    """Unmerge previously merged cells in the specified range."""

    def action(manager: AsposeCellsManager):
        # Extract sheet name implicitly if needed by implementation
        sheet_name = (
            manager.extract_sheet_name_from_range(range) if "!" in range else None
        )
        # Assuming base method needs range_spec and optionally sheet_name
        manager.unmerge_cells(range_spec=range, sheet_name=sheet_name)

    return _execute_spreadsheet_action(
        action, f"Successfully unmerged cells in range {range}.", state
    )


@tool
def copy_paste(
    source_range: Annotated[
        str, "The source range to copy from (e.g., 'Sheet1!A1:A5')."
    ],
    destination_range: Annotated[
        str, "The destination range to paste to (e.g., 'Sheet1!B1')."
    ],
    paste_type: Annotated[
        Literal["PASTE_NORMAL", "PASTE_VALUES", "PASTE_FORMAT"],
        "Type of paste operation.",
    ] = "PASTE_NORMAL",
    state: Annotated[State, InjectedState] = None,
) -> str:
    """Copy data from a source range and paste it to a destination range."""

    def action(manager: AsposeCellsManager):
        manager.copy_paste(
            source_range=source_range,
            destination_range=destination_range,
            paste_type=paste_type,
        )

    return _execute_spreadsheet_action(
        action,
        f"Successfully copied from {source_range} to {destination_range} with paste type '{paste_type}'.",
        state,
    )


@tool
def auto_fill(
    source_range: Annotated[
        str, "The source range with the data/pattern (e.g., 'Sheet1!A1:A2')."
    ],
    destination_range: Annotated[
        str, "The target range to fill (e.g., 'Sheet1!A1:A10')."
    ],
    fill_type: Annotated[
        Literal["COPY", "SERIES", "FORMATS", "VALUES", "DEFAULT"],
        "The type of auto-fill operation.",
    ] = "DEFAULT",
    state: Annotated[State, InjectedState] = None,
) -> str:
    """Auto-fill a destination range based on the data or pattern in a source range."""

    def action(manager: AsposeCellsManager):
        manager.auto_fill(
            source_range=source_range,
            destination_range=destination_range,
            fill_type=fill_type,
        )

    return _execute_spreadsheet_action(
        action,
        f"Successfully auto-filled range {destination_range} from {source_range} using fill type '{fill_type}'.",
        state,
    )
