from xlsx_to_sdif.spreadsheet.aspose_cells import workbook_manager
from xlsx_to_sdif.state import State


def cleanup_workbook_node(state: State) -> State:
    """Node to close the specific workbook used by this graph run from the manager."""
    spreadsheet_path = state.get("spreadsheet_path")
    if spreadsheet_path:
        workbook_manager.close_workbook(spreadsheet_path)
    return state
